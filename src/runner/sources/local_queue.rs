use std::sync::Arc;

use graphile_worker_lifecycle_hooks::JobFetchContext;
use graphile_worker_runtime as runtime;

use super::super::errors::ProcessJobError;
use super::super::job_execution::run_and_release_job;
use super::super::{Worker, WorkerRunner};
use crate::local_queue::{LocalQueue, LocalQueueParams, LocalQueueSignalReceiver};
use crate::streams::job_signal::JobSignalSource;

pub(in crate::runner) fn create_local_queue(
    worker: &Worker,
) -> Option<(LocalQueue, LocalQueueSignalReceiver)> {
    let config = worker.local_queue_config.as_ref()?;
    let (tx, rx) = runtime::channel(worker.concurrency * 2);
    let queue = LocalQueue::new(LocalQueueParams {
        config: config.clone(),
        database: worker.database.clone(),
        schema: worker.schema.clone(),
        worker_id: worker.worker_id.clone(),
        claim_coordinator: worker.claim_coordinator.clone(),
        poll_interval: worker.poll_interval,
        continuous: true,
        shutdown_signal: Some(worker.shutdown_signal.clone()),
        shutdown_grace_period: worker.shutdown_config.grace_period,
        hooks: worker.hooks.clone(),
        job_signal_sender: tx,
    });

    Some((queue, rx))
}

pub(in crate::runner) async fn process_local_queue_source(
    worker: &WorkerRunner,
    local_queue: &LocalQueue,
    source: JobSignalSource,
) -> Result<(), ProcessJobError> {
    if matches!(source, JobSignalSource::Notification) {
        local_queue.pulse(1).await;
    }

    let mut source = source;
    loop {
        let job = local_queue.get_job().await;

        let Some(job) = job else {
            break;
        };

        let job = Arc::new(job);

        if !worker.hooks.is_empty() {
            worker
                .hooks
                .emit(JobFetchContext {
                    job: job.clone(),
                    worker_id: worker.worker_id.clone(),
                })
                .await;
        }

        run_and_release_job(job.clone(), worker, &source).await?;
        source = JobSignalSource::LocalQueue;
    }

    Ok(())
}
