use super::*;

#[tokio::test]
async fn small_process_capacity_rotates_across_runtime_task_queues() {
    with_test_db(|test_db| async move {
        RUNTIME_QUEUE_ORDER
            .lock()
            .expect("runtime queue order lock")
            .clear();
        let utils = test_db.worker_utils();
        utils.migrate().await.expect("Failed to migrate");

        let queue_ids = ["fair_queue_a", "fair_queue_b", "fair_queue_c"];
        for queue_id in queue_ids {
            for _ in 0..10 {
                utils
                    .add_raw_job(queue_id, serde_json::Value::Null, JobSpec::default())
                    .await
                    .expect("Failed to add runtime queue job");
            }
        }

        let worker_fut =
            spawn_local({
                let database = test_db.database.clone();
                async move {
                    Worker::options()
                        .database(database)
                        .concurrency(1)
                        .local_queue(LocalQueueConfig::builder().size(2).build())
                        .define_jobs(queue_ids.map(|queue_id| {
                            RuntimeQueueJob::definition().with_identifier(queue_id)
                        }))
                        .init()
                        .await
                        .expect("Failed to create worker")
                        .run()
                        .await
                        .expect("Failed to run worker");
                }
            });

        let start = Instant::now();
        loop {
            let observed = RUNTIME_QUEUE_ORDER
                .lock()
                .expect("runtime queue order lock")
                .clone();
            if observed.len() >= 3 {
                assert_eq!(
                    observed[..3]
                        .iter()
                        .cloned()
                        .collect::<std::collections::HashSet<_>>(),
                    queue_ids.into_iter().map(str::to_string).collect(),
                    "the first three dispatches must visit every queue despite deep backlogs"
                );
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "timed out: {observed:?}"
            );
            sleep(Duration::from_millis(25)).await;
        }

        worker_fut.abort();
    })
    .await;
}
