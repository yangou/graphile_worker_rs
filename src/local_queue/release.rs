use graphile_worker_database::{Database, Schema};
use graphile_worker_job::Job;
use graphile_worker_lifecycle_hooks::{LocalQueueMode, LocalQueueReturnJobsContext};
use graphile_worker_runtime as runtime;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{debug, error, warn};

use graphile_worker_queries::return_jobs::batch::return_jobs;

use super::config::{calculate_retry_delay, RETURN_JOBS_RETRY_OPTIONS};
use super::{LocalQueue, LocalQueueError};

impl LocalQueue {
    async fn return_jobs_with_retry(
        database: &Database,
        jobs: &[Job],
        schema: &Schema,
        worker_id: &str,
    ) -> Result<(), LocalQueueError> {
        let mut attempt = 0u32;
        loop {
            match return_jobs(database, jobs, schema, worker_id).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    attempt += 1;
                    if attempt >= RETURN_JOBS_RETRY_OPTIONS.max_attempts {
                        return Err(LocalQueueError::ReturnJobsError(format!(
                            "Failed after {} attempts: {}",
                            attempt, e
                        )));
                    }
                    let delay = calculate_retry_delay(attempt - 1, &RETURN_JOBS_RETRY_OPTIONS);
                    warn!(
                        attempt,
                        max_attempts = RETURN_JOBS_RETRY_OPTIONS.max_attempts,
                        ?delay,
                        error = %e,
                        "Failed to return jobs, retrying"
                    );
                    runtime::sleep(delay).await;
                }
            }
        }
    }

    pub(super) async fn set_mode_ttl_expired(&self) {
        // Serialize expiry with release so shutdown cannot abort this task
        // after it drains local jobs but before it returns them.
        let _release = self.0.release_lock.lock().await;
        let mut mode = self.0.mode.write().await;
        if *mode != LocalQueueMode::Waiting {
            return;
        }
        *mode = LocalQueueMode::TtlExpired;
        let jobs: Vec<Job> = self.0.job_queues.lock().await.drain();
        drop(mode);

        debug!("LocalQueue TTL expired, returning jobs to database");

        if !jobs.is_empty() {
            let jobs_count = jobs.len();
            let result = Self::return_jobs_with_retry(
                &self.0.database,
                &jobs,
                &self.0.schema,
                &self.0.worker_id,
            )
            .await;
            self.0.accepted_tracker.settled(jobs_count);
            if let Err(e) = result {
                error!(error = %e, "Failed to return jobs after TTL expiry (exhausted retries)");
            } else {
                self.0
                    .hooks
                    .emit(LocalQueueReturnJobsContext {
                        worker_id: self.0.worker_id.clone(),
                        jobs_count,
                    })
                    .await;
            }
        }
    }

    pub async fn release(&self) -> Result<(), LocalQueueError> {
        // Shutdown reaches LocalQueue through both its signal task and the
        // runner lifecycle. Join those callers so none can return before the
        // one release operation has returned every accepted job.
        let _release = self.0.release_lock.lock().await;

        if self.0.release_finished.load(Ordering::SeqCst) {
            return match self.0.release_error.lock().await.clone() {
                Some(error) => Err(LocalQueueError::ReturnJobsError(error)),
                None => Ok(()),
            };
        }

        let mut mode = self.0.mode.write().await;
        if *mode != LocalQueueMode::Released {
            *mode = LocalQueueMode::Released;
        }
        drop(mode);

        self.set_refetch_delay_active(false);
        self.0.refetch_delay.abort_notify.notify_waiters();
        self.0.state_notify.notify_one();

        self.0.ttl_timer_task.abort();
        self.0.refetch_delay_task.abort();

        // Give a short in-flight claim the normal shutdown grace period to
        // finish and enter the cache. Cancel a database wait that exceeds the
        // deadline; dropping that transaction rolls its claim back.
        let claim_deadline = Instant::now() + self.0.shutdown_grace_period;
        if runtime::timeout_at(claim_deadline, self.0.run_complete_notify.notified())
            .await
            .is_err()
        {
            warn!(
                grace_period_ms = self.0.shutdown_grace_period.as_millis(),
                "Aborting LocalQueue claim after shutdown grace period"
            );
            self.0.run_task.abort_and_stop().await;
        }

        debug!("LocalQueue releasing, returning jobs to database");

        let jobs: Vec<Job> = self.0.job_queues.lock().await.drain();
        if !jobs.is_empty() {
            let jobs_count = jobs.len();
            if let Err(error) = Self::return_jobs_with_retry(
                &self.0.database,
                &jobs,
                &self.0.schema,
                &self.0.worker_id,
            )
            .await
            {
                let message = error.to_string();
                *self.0.release_error.lock().await = Some(message);
                self.0.release_finished.store(true, Ordering::SeqCst);
                self.0.accepted_tracker.settled(jobs_count);
                return Err(error);
            }
            self.0.accepted_tracker.settled(jobs_count);

            self.0
                .hooks
                .emit(LocalQueueReturnJobsContext {
                    worker_id: self.0.worker_id.clone(),
                    jobs_count,
                })
                .await;
        }

        self.0.release_finished.store(true, Ordering::SeqCst);
        Ok(())
    }
}
