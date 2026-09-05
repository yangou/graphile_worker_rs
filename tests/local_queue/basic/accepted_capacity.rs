use super::*;

#[tokio::test]
async fn process_cap_includes_running_and_completion_pending_jobs() {
    with_test_db(|test_db| async move {
        CAPACITY_BOUND_STARTED.reset().await;
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        for _ in 0..6 {
            utils
                .add_job(CapacityBoundJob, JobSpec::default())
                .await
                .expect("Failed to add capacity-bound job");
        }

        let worker_fut = spawn_local({
            let database = test_db.database.clone();
            async move {
                Worker::options()
                    .database(database)
                    .concurrency(3)
                    .local_queue(LocalQueueConfig::builder().size(3).build())
                    .complete_job_batch_delay(Duration::from_secs(1))
                    .define_job::<CapacityBoundJob>()
                    .init()
                    .await
                    .expect("Failed to create worker")
                    .run()
                    .await
                    .expect("Failed to run worker");
            }
        });

        wait_for_counter(
            &CAPACITY_BOUND_STARTED,
            3,
            Duration::from_secs(5),
            Duration::from_millis(25),
            "The first accepted wave should start",
        )
        .await;

        sleep(Duration::from_millis(100)).await;
        let locked: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM graphile_worker._private_jobs WHERE locked_at IS NOT NULL",
        )
        .fetch_one(&test_db.test_pool)
        .await
        .expect("Failed to count accepted jobs");
        assert_eq!(
            locked, 3,
            "the process-wide accepted cap must not be exceeded"
        );
        assert_eq!(CAPACITY_BOUND_STARTED.get().await, 3);

        worker_fut.abort();
    })
    .await;
}
