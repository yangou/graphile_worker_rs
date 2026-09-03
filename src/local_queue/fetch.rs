use futures::FutureExt;
use graphile_worker_lifecycle_hooks::{LocalQueueGetJobsCompleteContext, LocalQueueMode};
use graphile_worker_runtime as runtime;
use tracing::{debug, error};

use crate::claim_coordinator::ClaimWave;

use super::LocalQueue;

impl LocalQueue {
    pub(super) async fn schedule_fetch(&self) {
        loop {
            let mode = *self.0.mode.read().await;

            if mode == LocalQueueMode::Released {
                break;
            }

            let can_fetch = mode == LocalQueueMode::Polling
                && !self.is_fetch_in_progress()
                && !self.is_refetch_delay_active();

            if !can_fetch {
                self.0.state_notify.notified().await;
                continue;
            }

            if self.0.accepted_tracker.free_capacity(self.0.config.size) == 0 {
                let capacity_available = self.0.accepted_tracker.capacity_notify.notified().fuse();
                let state_changed = self.0.state_notify.notified().fuse();
                futures::pin_mut!(capacity_available, state_changed);
                futures::select_biased! {
                    _ = state_changed => {}
                    _ = capacity_available => {}
                }
                continue;
            }

            self.fetch().await;

            let mode = *self.0.mode.read().await;
            if mode == LocalQueueMode::Polling && self.0.continuous {
                let should_fetch_again = self.take_fetch_again();
                let refetch_delay_wants_fetch = self.take_refetch_delay_fetch_on_complete();

                if !should_fetch_again && !refetch_delay_wants_fetch {
                    let sleep = runtime::sleep(self.0.poll_interval).fuse();
                    let notified = self.0.state_notify.notified().fuse();
                    futures::pin_mut!(sleep, notified);
                    futures::select_biased! {
                        _ = sleep => {}
                        _ = notified => {}
                    };
                }
            } else if mode == LocalQueueMode::Polling && !self.0.continuous {
                self.set_mode(LocalQueueMode::Released).await;
                break;
            }
        }
    }

    async fn fetch(&self) {
        if !self.try_start_fetch() {
            return;
        }

        if self.is_refetch_delay_active() {
            self.set_refetch_delay_fetch_on_complete(true);
            self.end_fetch();
            return;
        }

        self.set_fetch_again(false);
        self.reset_refetch_delay_counter();

        let free_capacity = self.0.accepted_tracker.free_capacity(self.0.config.size);
        let result = self.0.claim_coordinator.claim_wave(free_capacity).await;

        self.end_fetch();

        match result {
            Ok(ClaimWave::Claimed(queues)) => {
                let job_count = queues.iter().map(|queue| queue.jobs.len()).sum::<usize>();
                debug!(job_count, "LocalQueue fetched jobs from database");

                self.0
                    .hooks
                    .emit(LocalQueueGetJobsCompleteContext {
                        worker_id: self.0.worker_id.clone(),
                        jobs_count: job_count,
                    })
                    .await;

                let fetched_max = job_count >= free_capacity && free_capacity > 0;

                if let Some(ref refetch_delay_config) = self.0.config.refetch_delay {
                    let threshold_surpassed =
                        fetched_max || job_count > refetch_delay_config.threshold;

                    if !threshold_surpassed {
                        self.start_refetch_delay(refetch_delay_config).await;
                    }
                }

                if job_count > 0 {
                    self.received_jobs(queues, fetched_max).await;
                } else if !self.0.continuous {
                    self.set_mode(LocalQueueMode::Released).await;
                }
            }
            Ok(ClaimWave::Paused) => {
                debug!("LocalQueue claim paused");
            }
            Err(e) => {
                error!(error = %e, "LocalQueue failed to fetch jobs");
            }
        }
    }
}
