use std::sync::Arc;

use graphile_worker::{
    IntoTaskHandlerResult, JobSpec, TaskHandler, Worker, WorkerContext, WorkerShutdownConfig,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::Notify,
    task::spawn_local,
    time::{Duration, Instant},
};

use crate::helpers::{with_test_db, StaticCounter};

mod helpers;

#[tokio::test]
async fn request_shutdown_executes_scheduled_jobs() {
    static JOB_CALL_COUNT: StaticCounter = StaticCounter::new();

    #[derive(Serialize, Deserialize)]
    struct ShutdownJob;

    impl TaskHandler for ShutdownJob {
        const IDENTIFIER: &'static str = "shutdown_job";

        async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
            JOB_CALL_COUNT.increment().await;
        }
    }

    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        let worker = Arc::new(
            Worker::options()
                .database(test_db.database.clone())
                .concurrency(3)
                .listen_os_shutdown_signals(false)
                .define_job::<ShutdownJob>()
                .init()
                .await
                .expect("Failed to create worker"),
        );

        let worker_handle = spawn_local({
            let worker = worker.clone();
            async move {
                worker.run().await.expect("Worker run failed");
            }
        });

        let job_count = 5;
        for _ in 0..job_count {
            utils
                .add_job(ShutdownJob, JobSpec::default())
                .await
                .expect("Failed to add job");
        }

        let start = Instant::now();
        while JOB_CALL_COUNT.get().await < job_count {
            if start.elapsed().as_secs() > 5 {
                panic!("Jobs should have been processed before shutdown");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        worker.request_shutdown();

        tokio::time::timeout(Duration::from_secs(2), worker_handle)
            .await
            .expect("Worker did not shut down after request")
            .expect("Worker task panicked");

        let remaining_jobs = test_db.get_jobs().await;
        assert!(
            remaining_jobs.is_empty(),
            "Expected no remaining jobs, found {}",
            remaining_jobs.len()
        );

        assert_eq!(
            JOB_CALL_COUNT.get().await,
            job_count,
            "All scheduled jobs should have run before shutdown"
        );
    })
    .await;
}

#[tokio::test]
async fn custom_shutdown_signal_stops_worker_run() {
    with_test_db(|test_db| async move {
        let shutdown_notify = Arc::new(Notify::new());
        let shutdown = WorkerShutdownConfig::default()
            .listen_os_shutdown_signals(false)
            .shutdown_signal({
                let shutdown_notify = shutdown_notify.clone();
                async move {
                    shutdown_notify.notified().await;
                }
            });

        let worker = Arc::new(
            Worker::options()
                .database(test_db.database.clone())
                .worker_shutdown(shutdown)
                .init()
                .await
                .expect("Failed to create worker"),
        );

        let worker_handle = spawn_local({
            let worker = worker.clone();
            async move {
                worker.run().await.expect("Worker run failed");
            }
        });

        shutdown_notify.notify_one();

        tokio::time::timeout(Duration::from_secs(2), worker_handle)
            .await
            .expect("Worker did not shut down after custom shutdown signal")
            .expect("Worker task panicked");
    })
    .await;
}

#[tokio::test]
async fn request_shutdown_still_works_with_pending_custom_signal() {
    with_test_db(|test_db| async move {
        let worker = Arc::new(
            Worker::options()
                .database(test_db.database.clone())
                .listen_os_shutdown_signals(false)
                .shutdown_signal(std::future::pending::<()>())
                .init()
                .await
                .expect("Failed to create worker"),
        );

        let worker_handle = spawn_local({
            let worker = worker.clone();
            async move {
                worker.run().await.expect("Worker run failed");
            }
        });

        worker.request_shutdown();

        tokio::time::timeout(Duration::from_secs(2), worker_handle)
            .await
            .expect("Worker did not shut down after request_shutdown")
            .expect("Worker task panicked");
    })
    .await;
}

#[tokio::test]
async fn request_shutdown_cancels_a_direct_claim_waiting_on_worker_control() {
    #[derive(Serialize, Deserialize)]
    struct BlockedClaimJob;

    impl TaskHandler for BlockedClaimJob {
        const IDENTIFIER: &'static str = "blocked_direct_claim_job";

        async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {}
    }

    with_test_db(|test_db| async move {
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");
        utils
            .add_job(BlockedClaimJob, JobSpec::default())
            .await
            .expect("Failed to add job");

        let worker = Arc::new(
            Worker::options()
                .database(test_db.database.clone())
                .concurrency(1)
                .listen_os_shutdown_signals(false)
                .define_job::<BlockedClaimJob>()
                .init()
                .await
                .expect("Failed to create worker"),
        );

        let mut blocker = test_db
            .test_pool
            .begin()
            .await
            .expect("Failed to begin worker-control blocker");
        sqlx::query(
            "UPDATE graphile_worker._private_worker_control SET paused = paused WHERE id = true",
        )
        .execute(&mut *blocker)
        .await
        .expect("Failed to lock worker-control row");

        let mut worker_handle = spawn_local({
            let worker = worker.clone();
            async move { worker.run().await }
        });

        let wait_start = Instant::now();
        loop {
            let blocked_claims: i64 = sqlx::query_scalar(
                r#"
                    SELECT count(*)
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND wait_event_type = 'Lock'
                      AND query LIKE '%_private_worker_control%for share%'
                "#,
            )
            .fetch_one(&test_db.test_pool)
            .await
            .expect("Failed to inspect blocked direct claim");
            if blocked_claims > 0 {
                break;
            }
            if wait_start.elapsed() > Duration::from_secs(5) {
                panic!("Direct claim did not block on worker control");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        worker.request_shutdown();
        let shutdown_finished = tokio::time::timeout(Duration::from_secs(2), &mut worker_handle)
            .await
            .is_ok();

        blocker
            .rollback()
            .await
            .expect("Failed to release worker-control blocker");
        if !worker_handle.is_finished() {
            tokio::time::timeout(Duration::from_secs(2), worker_handle)
                .await
                .expect("Worker did not finish after releasing blocker")
                .expect("Worker task panicked")
                .expect("Worker run failed");
        }

        assert!(
            shutdown_finished,
            "Direct-mode shutdown must cancel an in-flight claim blocked on worker control"
        );

        let jobs = test_db.get_jobs().await;
        assert_eq!(jobs.len(), 1);
        assert!(jobs[0].locked_by.is_none());
    })
    .await;
}
