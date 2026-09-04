use super::*;

#[tokio::test]
async fn sqlx_pool_exercises_get_and_fail_helpers() {
    with_test_db(|test_db| async move {
        test_db
            .worker_utils()
            .migrate()
            .await
            .expect("Failed to migrate");

        let task_details = get_tasks_details(
            &test_db.test_pool,
            &graphile_worker::Schema::default(),
            vec![
                "sqlx_fetch_no_queue".to_string(),
                "sqlx_fetch_queue".to_string(),
            ],
        )
        .await
        .expect("Failed to get task details");

        let now = Utc::now();
        let no_queue_spec = JobSpecBuilder::new().priority(1).run_at(now).build();
        add_job(
            &test_db.test_pool,
            &graphile_worker::Schema::default(),
            "sqlx_fetch_no_queue",
            json!({ "kind": "no_queue" }),
            no_queue_spec,
            false,
        )
        .await
        .expect("Failed to add no-queue job");

        let queue_spec = JobSpecBuilder::new()
            .priority(5)
            .run_at(now)
            .queue_name("sqlx_fetch_queue")
            .flags(vec!["allowed".to_string()])
            .build();
        add_job(
            &test_db.test_pool,
            &graphile_worker::Schema::default(),
            "sqlx_fetch_queue",
            json!({ "kind": "queue" }),
            queue_spec,
            false,
        )
        .await
        .expect("Failed to add queued job");

        let skip_flags = vec!["blocked".to_string()];
        let first_tx = test_db.database.begin().await.expect("begin first claim");
        assert!(
            !read_worker_control(&first_tx, graphile_worker::Schema::default())
                .await
                .expect("read claim control")
                .paused
        );
        let first_claim = claim_queue_jobs(
            &first_tx,
            graphile_worker::Schema::default(),
            "sqlx-worker-one",
            task_details
                .get_id("sqlx_fetch_no_queue")
                .expect("registered no-queue task"),
            "sqlx_fetch_no_queue",
            &skip_flags,
            1,
            None,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await
        .expect("SQLx task-scoped claim should succeed");
        first_tx.commit().await.expect("commit first claim");
        let first_job = first_claim.jobs.into_iter().next().expect("job fetched");
        assert_eq!(first_job.task_identifier(), "sqlx_fetch_no_queue");

        fail_jobs(
            &test_db.test_pool,
            &[FailedJob {
                job: &first_job,
                error: "retry no queue",
            }],
            &graphile_worker::Schema::default(),
            "sqlx-worker-one",
        )
        .await
        .expect("SQLx fail_jobs should handle jobs without queues");

        let second_tx = test_db.database.begin().await.expect("begin second claim");
        assert!(
            !read_worker_control(&second_tx, graphile_worker::Schema::default())
                .await
                .expect("read claim control")
                .paused
        );
        let batch = claim_queue_jobs(
            &second_tx,
            graphile_worker::Schema::default(),
            "sqlx-worker-two",
            task_details
                .get_id("sqlx_fetch_queue")
                .expect("registered queued task"),
            "sqlx_fetch_queue",
            &skip_flags,
            10,
            None,
            Some(now + chrono::Duration::seconds(1)),
        )
        .await
        .expect("SQLx task-scoped claim should succeed");
        second_tx.commit().await.expect("commit second claim");
        assert_eq!(batch.jobs.len(), 1);
        assert_eq!(batch.jobs[0].task_identifier(), "sqlx_fetch_queue");

        fail_jobs(
            &test_db.test_pool,
            &[FailedJob {
                job: &batch.jobs[0],
                error: "retry queue",
            }],
            &graphile_worker::Schema::default(),
            "sqlx-worker-two",
        )
        .await
        .expect("SQLx fail_jobs should handle queued jobs");
    })
    .await;
}
