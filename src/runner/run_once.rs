use std::sync::Arc;

use futures::StreamExt;
use tracing::{error, info};

use super::job_execution::run_and_release_job;
use super::{Worker, WorkerRuntimeError};
use crate::streams::job_fetch::job_stream_from_coordinator;
use crate::streams::job_signal::JobSignalSource;

impl Worker {
    /// Runs the worker once and processes all available jobs, then returns.
    pub async fn run_once(&self) -> Result<(), WorkerRuntimeError> {
        let job_stream = job_stream_from_coordinator(
            self.claim_coordinator.clone(),
            self.shutdown_signal.clone(),
        );

        let runner = self.runner();

        job_stream
            .for_each_concurrent(self.concurrency, {
                let runner = runner.clone();
                move |mut job| {
                    let runner = runner.clone();
                    async move {
                        loop {
                            let job_id = *job.id();
                            let task_id = *job.task_id();
                            let has_queue = job.job_queue_id().is_some();
                            let result = run_and_release_job(
                                Arc::new(job),
                                &runner,
                                &JobSignalSource::RunOnce,
                            )
                            .await;

                            match result {
                                Ok(_) => {
                                    info!(job_id, "Job processed");
                                }
                                Err(e) => {
                                    error!("Error while processing job : {:?}", e);
                                }
                            };

                            if !has_queue {
                                break;
                            }
                            info!(job_id, "Job has queue, fetching another job");
                            runner.claim_coordinator.restart_task_scan(task_id).await;
                            let new_job = runner
                                .claim_coordinator
                                .claim_one_until_shutdown(runner.shutdown_signal.clone())
                                .await
                                .unwrap_or(None);
                            let Some(new_job) = new_job else {
                                break;
                            };
                            job = new_job;
                        }
                    }
                }
            })
            .await;

        Ok(())
    }
}
