#![cfg(feature = "driver-sqlx")]

use chrono::Utc;
use graphile_worker::sql::claim_queue_jobs::{
    claim_queue_jobs, claim_queue_jobs_sql, lock_worker_control, QueueClaim,
};
use graphile_worker::sql::complete_job::complete_job;
use graphile_worker::sql::task_identifiers::get_tasks_details;
use graphile_worker::{JobSpec, JobSpecBuilder, Schema};
use serde_json::json;

use helpers::{sql::safe_query_scalar, with_test_db};

mod helpers;

const TASK: &str = "claim_control_job";
const OTHER_TASK: &str = "claim_control_other_job";

async fn claim_committed(
    test_db: &helpers::TestDatabase,
    task_id: i32,
    worker_id: &str,
    quota: i32,
    cursor: Option<i32>,
    now: Option<chrono::DateTime<Utc>>,
) -> QueueClaim {
    let tx = test_db.database.begin().await.expect("begin claim wave");
    let control = lock_worker_control(&tx, Schema::default())
        .await
        .expect("read claim control");
    if control.paused {
        tx.commit().await.expect("commit paused claim wave");
        return QueueClaim {
            jobs: Vec::new(),
            next_ordering_cursor: cursor,
        };
    }
    let claimed = claim_queue_jobs(
        &tx,
        Schema::default(),
        worker_id,
        task_id,
        TASK,
        &[],
        quota,
        cursor,
        now,
    )
    .await
    .expect("claim task jobs");
    tx.commit().await.expect("commit claim wave");
    claimed
}

#[tokio::test]
async fn queue_claim_commit_releases_job_for_completion() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");
        let task_details = get_tasks_details(
            &test_db.test_pool,
            &Schema::default(),
            vec![TASK.to_string()],
        )
        .await
        .expect("Failed to register task");
        let task_id = task_details.get_id(TASK).expect("registered task id");
        let job = utils
            .add_raw_job(
                TASK,
                json!({}),
                JobSpecBuilder::new()
                    .queue_name("claim-then-complete")
                    .build(),
            )
            .await
            .expect("Failed to add job");

        let tx = test_db.database.begin().await.expect("begin claim");
        assert!(
            !lock_worker_control(&tx, Schema::default())
                .await
                .expect("read pause control")
                .paused
        );
        let claimed = claim_queue_jobs(
            &tx,
            Schema::default(),
            "claim-complete-worker",
            task_id,
            TASK,
            &[],
            1,
            None,
            None,
        )
        .await
        .expect("claim job");
        tx.commit().await.expect("commit claim");

        assert_eq!(claimed.jobs.len(), 1);
        assert_eq!(claimed.jobs[0].id(), job.id());
        complete_job(
            &test_db.database,
            &claimed.jobs[0],
            "claim-complete-worker",
            Schema::default(),
        )
        .await
        .expect("complete claimed job");
        assert!(test_db.get_jobs().await.is_empty());
    })
    .await;
}

#[tokio::test]
async fn queue_scoped_claim_locks_ordering_rows_before_jobs() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");
        let task_details = get_tasks_details(
            &test_db.test_pool,
            &Schema::default(),
            vec![TASK.to_string()],
        )
        .await
        .expect("Failed to register task");
        let task_id = task_details.get_id(TASK).expect("registered task id");
        let now = Utc::now();

        let blocked_spec = JobSpecBuilder::new()
            .queue_name("ordering-lock-first")
            .run_at(now)
            .build();
        let blocked_first = utils
            .add_raw_job(TASK, json!({ "n": 1 }), blocked_spec.clone())
            .await
            .expect("Failed to add first blocked-key job");
        let blocked_successor = utils
            .add_raw_job(TASK, json!({ "n": 2 }), blocked_spec)
            .await
            .expect("Failed to add blocked-key successor");
        let reachable = utils
            .add_raw_job(
                TASK,
                json!({ "n": 3 }),
                JobSpecBuilder::new()
                    .queue_name("ordering-lock-second")
                    .run_at(now)
                    .build(),
            )
            .await
            .expect("Failed to add reachable-key job");

        let mut blocker = test_db
            .test_pool
            .begin()
            .await
            .expect("Failed to begin ordering-row blocker");
        sqlx::query(
            "SELECT id FROM graphile_worker._private_job_queues \
             WHERE queue_name = 'ordering-lock-first' FOR UPDATE",
        )
        .fetch_one(&mut *blocker)
        .await
        .expect("Failed to lock ordering row");

        let claim_tx = test_db
            .database
            .begin()
            .await
            .expect("Failed to begin claim wave");
        assert!(
            !lock_worker_control(&claim_tx, Schema::default())
                .await
                .expect("Failed to read pause control")
                .paused
        );
        let claimed = claim_queue_jobs(
            &claim_tx,
            Schema::default(),
            "ordering-lock-worker",
            task_id,
            TASK,
            &[],
            1,
            None,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await
        .expect("Failed to claim past locked ordering row");
        claim_tx
            .commit()
            .await
            .expect("Failed to commit claim wave");

        assert_eq!(claimed.jobs.len(), 1);
        assert_eq!(claimed.jobs[0].id(), reachable.id());
        assert_ne!(claimed.jobs[0].id(), blocked_first.id());
        assert_ne!(claimed.jobs[0].id(), blocked_successor.id());

        blocker
            .rollback()
            .await
            .expect("Failed to release ordering-row blocker");
    })
    .await;
}

#[tokio::test]
async fn same_ordering_key_excludes_claims_across_task_queues() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");
        let task_details = get_tasks_details(
            &test_db.test_pool,
            &Schema::default(),
            vec![TASK.to_string(), OTHER_TASK.to_string()],
        )
        .await
        .expect("Failed to register tasks");
        let first_task_id = task_details.get_id(TASK).expect("first task id");
        let second_task_id = task_details.get_id(OTHER_TASK).expect("second task id");
        let now = Utc::now();
        let spec = JobSpecBuilder::new()
            .queue_name("cross-task-ordering-key")
            .run_at(now)
            .build();
        utils
            .add_raw_job(TASK, json!({ "task": 1 }), spec.clone())
            .await
            .expect("Failed to add first task job");
        utils
            .add_raw_job(OTHER_TASK, json!({ "task": 2 }), spec)
            .await
            .expect("Failed to add second task job");

        let first_tx = test_db.database.begin().await.expect("begin first wave");
        assert!(
            !lock_worker_control(&first_tx, Schema::default())
                .await
                .expect("read first control")
                .paused
        );
        let first = claim_queue_jobs(
            &first_tx,
            Schema::default(),
            "cross-task-worker-one",
            first_task_id,
            TASK,
            &[],
            1,
            None,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await
        .expect("claim first task");
        assert_eq!(first.jobs.len(), 1);

        let second_tx = test_db.database.begin().await.expect("begin second wave");
        assert!(
            !lock_worker_control(&second_tx, Schema::default())
                .await
                .expect("read second control")
                .paused
        );
        let second = claim_queue_jobs(
            &second_tx,
            Schema::default(),
            "cross-task-worker-two",
            second_task_id,
            OTHER_TASK,
            &[],
            1,
            None,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await
        .expect("claim second task");
        second_tx.commit().await.expect("commit second wave");
        assert!(
            second.jobs.is_empty(),
            "one ordering key must serialize jobs even when their task queues differ"
        );

        drop(first_tx);
    })
    .await;
}

#[tokio::test]
async fn deep_backlog_on_one_ordering_key_does_not_hide_another_key() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");
        let task_details = get_tasks_details(
            &test_db.test_pool,
            &Schema::default(),
            vec![TASK.to_string()],
        )
        .await
        .expect("Failed to register task");
        let task_id = task_details.get_id(TASK).expect("registered task id");
        let first_queue_id: i32 = safe_query_scalar(
            "INSERT INTO graphile_worker._private_job_queues (queue_name) \
             VALUES ('deep-ordering-key') RETURNING id",
        )
        .fetch_one(&test_db.test_pool)
        .await
        .expect("create deep ordering key");
        let second_queue_id: i32 = safe_query_scalar(
            "INSERT INTO graphile_worker._private_job_queues (queue_name) \
             VALUES ('reachable-ordering-key') RETURNING id",
        )
        .fetch_one(&test_db.test_pool)
        .await
        .expect("create reachable ordering key");
        sqlx::query(
            "INSERT INTO graphile_worker._private_jobs (job_queue_id, task_id, payload) \
             SELECT $1, $2, '{}'::json FROM generate_series(1, 10000)",
        )
        .bind(first_queue_id)
        .bind(task_id)
        .execute(&test_db.test_pool)
        .await
        .expect("seed deep backlog");
        sqlx::query(
            "INSERT INTO graphile_worker._private_jobs (job_queue_id, task_id, payload) \
             VALUES ($1, $2, '{}'::json)",
        )
        .bind(second_queue_id)
        .bind(task_id)
        .execute(&test_db.test_pool)
        .await
        .expect("seed second ordering key");

        let claimed = claim_committed(&test_db, task_id, "deep-key-worker", 2, None, None).await;
        let queue_ids = claimed
            .jobs
            .iter()
            .map(|job| (*job.job_queue_id()).expect("queued job"))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(queue_ids, [first_queue_id, second_queue_id].into());
    })
    .await;
}

#[tokio::test]
async fn queue_claim_work_is_bounded_by_quota_not_queue_cardinality() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        let task_details = get_tasks_details(
            &test_db.test_pool,
            &Schema::default(),
            vec![TASK.to_string()],
        )
        .await
        .expect("Failed to register task");
        let task_id = task_details.get_id(TASK).expect("registered task id");

        sqlx::query(
            "INSERT INTO graphile_worker._private_job_queues (queue_name) \
             SELECT 'bounded-plan-' || n FROM generate_series(1, 10000) AS n",
        )
        .execute(&test_db.test_pool)
        .await
        .expect("Failed to seed ordering queues");
        sqlx::query(
            "INSERT INTO graphile_worker._private_jobs (job_queue_id, task_id) \
             SELECT id, $1 FROM graphile_worker._private_job_queues \
             WHERE queue_name LIKE 'bounded-plan-%'",
        )
        .bind(task_id)
        .execute(&test_db.test_pool)
        .await
        .expect("Failed to seed queued jobs");
        sqlx::query("ANALYZE graphile_worker._private_job_queues")
            .execute(&test_db.test_pool)
            .await
            .expect("Failed to analyze ordering queues");
        sqlx::query("ANALYZE graphile_worker._private_jobs")
            .execute(&test_db.test_pool)
            .await
            .expect("Failed to analyze jobs");

        let statement = claim_queue_jobs_sql(&Schema::default());
        let explain = format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {statement}");
        let mut tx = test_db
            .test_pool
            .begin()
            .await
            .expect("Failed to begin plan transaction");
        let plan: serde_json::Value = safe_query_scalar(explain)
            .bind("bounded-plan-worker")
            .bind(task_id)
            .bind(1_i32)
            .bind(Option::<i32>::None)
            .bind(Vec::<String>::new())
            .bind(Utc::now())
            .fetch_one(&mut *tx)
            .await
            .expect("Failed to explain production claim");
        tx.rollback().await.expect("Failed to roll back claim");

        let root = &plan[0]["Plan"];
        let shared_hits = root["Shared Hit Blocks"]
            .as_u64()
            .expect("root shared-hit count");
        assert!(
            shared_hits < 500,
            "quota-one claim touched {shared_hits} shared buffers for 10,000 ordering queues"
        );
    })
    .await;
}

#[tokio::test]
async fn batch_claims_at_most_one_job_per_named_queue() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        let task_details = get_tasks_details(
            &test_db.test_pool,
            &Schema::default(),
            vec![TASK.to_string()],
        )
        .await
        .expect("Failed to register task");
        let task_id = task_details.get_id(TASK).expect("registered task id");

        let now = Utc::now();
        let queue_one = JobSpecBuilder::new()
            .queue_name("claim-control-one")
            .run_at(now)
            .build();
        let queue_two = JobSpecBuilder::new()
            .queue_name("claim-control-two")
            .run_at(now)
            .build();

        let first_queue_one = utils
            .add_raw_job(TASK, json!({ "n": 1 }), queue_one.clone())
            .await
            .expect("Failed to add first queue-one job");
        let second_queue_one = utils
            .add_raw_job(TASK, json!({ "n": 2 }), queue_one)
            .await
            .expect("Failed to add second queue-one job");
        let queue_two_job = utils
            .add_raw_job(TASK, json!({ "n": 3 }), queue_two)
            .await
            .expect("Failed to add queue-two job");
        let unqueued_one = utils
            .add_raw_job(TASK, json!({ "n": 4 }), JobSpec::default())
            .await
            .expect("Failed to add first unqueued job");
        let unqueued_two = utils
            .add_raw_job(TASK, json!({ "n": 5 }), JobSpec::default())
            .await
            .expect("Failed to add second unqueued job");

        let claimed = claim_committed(
            &test_db,
            task_id,
            "claim-control-worker",
            5,
            None,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await;

        let ids = claimed.jobs.iter().map(|job| *job.id()).collect::<Vec<_>>();
        let queue_one_count = ids
            .iter()
            .filter(|id| **id == *first_queue_one.id() || **id == *second_queue_one.id())
            .count();

        assert_eq!(queue_one_count, 1);
        assert!(ids.contains(queue_two_job.id()));
        assert!(ids.contains(unqueued_one.id()));
        assert!(ids.contains(unqueued_two.id()));
        assert_eq!(claimed.jobs.len(), 4);

        for job in &claimed.jobs {
            complete_job(
                &test_db.test_pool,
                job,
                "claim-control-worker",
                &Schema::default(),
            )
            .await
            .expect("Failed to complete first claim batch");
        }
        let successor = claim_committed(
            &test_db,
            task_id,
            "claim-control-successor-worker",
            5,
            claimed.next_ordering_cursor,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await;
        assert_eq!(successor.jobs.len(), 1);
        assert_eq!(successor.jobs[0].id(), second_queue_one.id());
    })
    .await;
}

#[tokio::test]
async fn batch_claim_locks_only_named_queues_it_can_return() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        let task_details = get_tasks_details(
            &test_db.test_pool,
            &Schema::default(),
            vec![TASK.to_string()],
        )
        .await
        .expect("Failed to register task");
        let task_id = task_details.get_id(TASK).expect("registered task id");

        let now = Utc::now();
        for queue_name in [
            "bounded-queue-one",
            "bounded-queue-two",
            "bounded-queue-three",
        ] {
            let spec = JobSpecBuilder::new()
                .queue_name(queue_name)
                .run_at(now)
                .build();
            utils
                .add_raw_job(TASK, json!({ "queue": queue_name }), spec)
                .await
                .expect("Failed to add named-queue job");
        }

        let mut first_tx = test_db
            .test_pool
            .begin()
            .await
            .expect("Failed to begin first claim transaction");
        assert!(
            !lock_worker_control(&mut first_tx, Schema::default())
                .await
                .expect("Failed to read pause control")
                .paused
        );
        let first = claim_queue_jobs(
            &mut first_tx,
            Schema::default(),
            "bounded-queue-worker-one",
            task_id,
            TASK,
            &[],
            1,
            None,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await
        .expect("Failed to claim first named queue");
        assert_eq!(first.jobs.len(), 1);

        let second = claim_committed(
            &test_db,
            task_id,
            "bounded-queue-worker-two",
            1,
            None,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await;
        assert_eq!(second.jobs.len(), 1);
        assert_ne!(first.jobs[0].id(), second.jobs[0].id());

        first_tx
            .rollback()
            .await
            .expect("Failed to roll back first claim");
    })
    .await;
}

#[tokio::test]
async fn unkeyed_claim_stays_bounded_under_row_contention() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        let task_details = get_tasks_details(
            &test_db.test_pool,
            &Schema::default(),
            vec![TASK.to_string()],
        )
        .await
        .expect("Failed to register task");
        let task_id = task_details.get_id(TASK).expect("registered task id");

        let now = Utc::now();
        let spec = JobSpecBuilder::new().run_at(now).build();
        let first = utils
            .add_raw_job(TASK, json!({ "n": 1 }), spec.clone())
            .await
            .expect("Failed to add first unqueued job");
        utils
            .add_raw_job(TASK, json!({ "n": 2 }), spec.clone())
            .await
            .expect("Failed to add second unqueued job");
        utils
            .add_raw_job(TASK, json!({ "n": 3 }), spec)
            .await
            .expect("Failed to add third unqueued job");

        let mut blocker = test_db
            .test_pool
            .begin()
            .await
            .expect("Failed to begin blocker transaction");
        sqlx::query("SELECT id FROM graphile_worker._private_jobs WHERE id = $1 FOR UPDATE")
            .bind(first.id())
            .fetch_one(&mut *blocker)
            .await
            .expect("Failed to lock first unqueued job");

        let claimed = claim_committed(
            &test_db,
            task_id,
            "unqueued-backfill-worker",
            2,
            None,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await;
        assert_eq!(
            claimed.jobs.len(),
            2,
            "a locked first row must not consume the bounded unkeyed claim window"
        );
        assert!(claimed.jobs.iter().all(|job| job.id() != first.id()));

        blocker
            .rollback()
            .await
            .expect("Failed to roll back blocker transaction");
    })
    .await;
}

#[tokio::test]
async fn pause_and_claim_share_one_atomic_row_lock_handshake() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        let task_details = get_tasks_details(
            &test_db.test_pool,
            &Schema::default(),
            vec![TASK.to_string()],
        )
        .await
        .expect("Failed to register task");
        utils
            .add_raw_job(TASK, json!({ "n": 1 }), JobSpec::default())
            .await
            .expect("Failed to add job");

        let claim_tx = test_db
            .database
            .begin()
            .await
            .expect("Failed to begin claim transaction");
        assert!(!lock_worker_control(&claim_tx, Schema::default())
            .await
            .expect("Failed to read pause control")
            .paused);
        let task_id = task_details.get_id(TASK).expect("registered task id");
        let claimed = claim_queue_jobs(
            &claim_tx,
            Schema::default(),
            "claim-control-lock-worker",
            task_id,
            TASK,
            &[],
            1,
            None,
            None,
        )
        .await
        .expect("Failed to claim job");
        assert_eq!(claimed.jobs.len(), 1);

        let mut pause_tx = test_db
            .test_pool
            .begin()
            .await
            .expect("Failed to begin pause transaction");
        sqlx::query("SET LOCAL lock_timeout = '100ms'")
            .execute(&mut *pause_tx)
            .await
            .expect("Failed to set lock timeout");
        let error = sqlx::query(
            "UPDATE graphile_worker._private_worker_control SET paused = true, pause_reason = 'test_transition', updated_at = now() WHERE id = true",
        )
        .execute(&mut *pause_tx)
        .await
        .expect_err("Pause must wait for a claim transaction that passed the gate");
        assert_eq!(
            error.as_database_error().and_then(|error| error.code()),
            Some(std::borrow::Cow::Borrowed("55P03"))
        );
        pause_tx
            .rollback()
            .await
            .expect("Failed to roll back blocked pause");

        drop(claim_tx);

        sqlx::query(
            "UPDATE graphile_worker._private_worker_control SET paused = true, pause_reason = 'test_transition', updated_at = now() WHERE id = true",
        )
        .execute(&test_db.test_pool)
        .await
        .expect("Failed to pause after claim completed");

        let paused_tx = test_db.database.begin().await.expect("begin paused wave");
        let control = lock_worker_control(&paused_tx, Schema::default())
            .await
            .expect("Paused control row remains readable");
        assert!(control.paused);
        paused_tx.commit().await.expect("commit paused wave");
    })
    .await;
}

#[tokio::test]
async fn missing_pause_control_row_fails_closed() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        utils
            .add_raw_job(TASK, json!({ "n": 1 }), JobSpec::default())
            .await
            .expect("Failed to add job");

        sqlx::query("DELETE FROM graphile_worker._private_worker_control")
            .execute(&test_db.test_pool)
            .await
            .expect("Failed to remove control row");

        let tx = test_db.database.begin().await.expect("begin claim wave");
        lock_worker_control(&tx, Schema::default())
            .await
            .expect_err("Missing control row must fail the claim wave closed");
        drop(tx);
    })
    .await;
}

#[tokio::test]
async fn worker_utils_pause_operations_are_idempotent_and_transactional() {
    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        let initial = utils
            .worker_control_state()
            .await
            .expect("Failed to read initial worker control state");
        assert!(!initial.paused);

        let paused = utils
            .set_worker_pause(Some("test_transition"))
            .await
            .expect("Failed to pause worker claims");
        assert!(!paused.previous.paused);
        assert!(paused.current.paused);
        assert_eq!(
            paused.current.pause_reason.as_deref(),
            Some("test_transition")
        );
        utils
            .add_raw_job(
                TASK,
                json!({ "queued_while_paused": true }),
                JobSpec::default(),
            )
            .await
            .expect("Pause must not gate enqueue");

        let repeated = utils
            .set_worker_pause(Some("test_transition"))
            .await
            .expect("Failed to repeat pause");
        assert!(repeated.previous.paused);
        assert_eq!(repeated.previous.updated_at, repeated.current.updated_at);

        let mut tx = test_db
            .test_pool
            .begin()
            .await
            .expect("Failed to begin control transaction");
        let mut transactional = utils.clone().with_executor(&mut tx);
        let resumed = transactional
            .set_worker_pause(None)
            .await
            .expect("Failed to resume inside transaction");
        assert!(resumed.previous.paused);
        assert!(!resumed.current.paused);
        tx.rollback()
            .await
            .expect("Failed to roll back control transaction");

        let after_rollback = utils
            .worker_control_state()
            .await
            .expect("Failed to read worker control state after rollback");
        assert!(after_rollback.paused);
    })
    .await;
}
