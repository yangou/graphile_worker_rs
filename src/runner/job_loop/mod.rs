mod cron;
mod direct;
mod local_queue;

use super::{sources, Worker, WorkerRunner, WorkerRuntimeError};
use crate::local_queue::{LocalQueue, LocalQueueSignalReceiver};

impl Worker {
    pub(super) fn create_local_queue(&self) -> Option<(LocalQueue, LocalQueueSignalReceiver)> {
        sources::create_local_queue(self)
    }

    pub(super) fn runner(&self) -> WorkerRunner {
        WorkerRunner {
            worker_id: self.worker_id.clone(),
            jobs: self.jobs.clone(),
            database: self.database.clone(),
            schema: self.schema.clone(),
            task_details: self.task_details.clone(),
            claim_coordinator: self.claim_coordinator.clone(),
            use_local_time: self.use_local_time,
            shutdown_signal: self.shutdown_signal.clone(),
            extensions: self.extensions.clone(),
            hooks: self.hooks.clone(),
            completion_batcher: self.completion_batcher.clone(),
            failure_batcher: self.failure_batcher.clone(),
            accepted_tracker: None,
            shutdown_config: self.shutdown_config.clone(),
        }
    }

    pub(super) async fn job_runner_internal(
        &self,
        local_queue: Option<(LocalQueue, LocalQueueSignalReceiver)>,
    ) -> Result<(), WorkerRuntimeError> {
        match local_queue {
            Some((local_queue, rx)) => local_queue::run(self, local_queue, rx).await,
            None => direct::run(self).await,
        }
    }

    pub(super) async fn crontab_scheduler(&self) -> Result<(), WorkerRuntimeError> {
        cron::run(self).await
    }
}
