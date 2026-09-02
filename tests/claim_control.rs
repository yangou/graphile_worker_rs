#![cfg(feature = "driver-sqlx")]

use chrono::Utc;
use graphile_worker::sql::batch_get_jobs::batch_get_jobs;
use graphile_worker::sql::complete_job::complete_job;
use graphile_worker::sql::get_job::get_job;
use graphile_worker::sql::task_identifiers::get_tasks_details;
use graphile_worker::{JobSpec, JobSpecBuilder, Schema};
use serde_json::json;

use helpers::with_test_db;

mod helpers;

const TASK: &str = "claim_control_job";

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

        let claimed = batch_get_jobs(
            &test_db.test_pool,
            &task_details,
            &Schema::default(),
            "claim-control-worker",
            &[],
            5,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await
        .expect("Failed to claim batch");

        let ids = claimed.iter().map(|job| *job.id()).collect::<Vec<_>>();
        let queue_one_count = ids
            .iter()
            .filter(|id| **id == *first_queue_one.id() || **id == *second_queue_one.id())
            .count();

        assert_eq!(queue_one_count, 1);
        assert!(ids.contains(queue_two_job.id()));
        assert!(ids.contains(unqueued_one.id()));
        assert!(ids.contains(unqueued_two.id()));
        assert_eq!(claimed.len(), 4);

        for job in &claimed {
            complete_job(
                &test_db.test_pool,
                job,
                "claim-control-worker",
                &Schema::default(),
            )
            .await
            .expect("Failed to complete first claim batch");
        }
        let successor = batch_get_jobs(
            &test_db.test_pool,
            &task_details,
            &Schema::default(),
            "claim-control-successor-worker",
            &[],
            5,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await
        .expect("Failed to claim named-queue successor");
        assert_eq!(successor.len(), 1);
        assert_eq!(successor[0].id(), second_queue_one.id());
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

        let mut claim_tx = test_db
            .test_pool
            .begin()
            .await
            .expect("Failed to begin claim transaction");
        let claimed = batch_get_jobs(
            &mut claim_tx,
            &task_details,
            &Schema::default(),
            "claim-control-lock-worker",
            &[],
            1,
            None,
        )
        .await
        .expect("Failed to claim job");
        assert_eq!(claimed.len(), 1);

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
            "UPDATE graphile_worker._private_worker_control SET paused = true, updated_at = now() WHERE id = true",
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

        claim_tx
            .rollback()
            .await
            .expect("Failed to roll back claim");

        sqlx::query(
            "UPDATE graphile_worker._private_worker_control SET paused = true, updated_at = now() WHERE id = true",
        )
        .execute(&test_db.test_pool)
        .await
        .expect("Failed to pause after claim completed");

        let claimed_while_paused = batch_get_jobs(
            &test_db.test_pool,
            &task_details,
            &Schema::default(),
            "claim-control-paused-worker",
            &[],
            1,
            None,
        )
        .await
        .expect("Paused claim must fail closed without an error");
        assert!(claimed_while_paused.is_empty());
    })
    .await;
}

#[tokio::test]
async fn missing_pause_control_row_fails_closed() {
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

        sqlx::query("DELETE FROM graphile_worker._private_worker_control")
            .execute(&test_db.test_pool)
            .await
            .expect("Failed to remove control row");

        let claimed = batch_get_jobs(
            &test_db.test_pool,
            &task_details,
            &Schema::default(),
            "claim-control-missing-worker",
            &[],
            1,
            None,
        )
        .await
        .expect("Missing control row must fail closed without an error");
        assert!(claimed.is_empty());
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
            .set_worker_paused(true)
            .await
            .expect("Failed to pause worker claims");
        assert!(!paused.previous.paused);
        assert!(paused.current.paused);
        utils
            .add_raw_job(
                TASK,
                json!({ "queued_while_paused": true }),
                JobSpec::default(),
            )
            .await
            .expect("Pause must not gate enqueue");

        let repeated = utils
            .set_worker_paused(true)
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
            .set_worker_paused(false)
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

#[tokio::test]
async fn pause_gates_single_job_claims_too() {
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
        utils
            .set_worker_paused(true)
            .await
            .expect("Failed to pause worker claims");

        let claimed = get_job(
            &test_db.test_pool,
            &task_details,
            &Schema::default(),
            "claim-control-single-worker",
            &[],
            None,
        )
        .await
        .expect("Paused single claim must fail closed without an error");
        assert!(claimed.is_none());
    })
    .await;
}
