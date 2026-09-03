use graphile_worker_job::Job;
use graphile_worker_lifecycle_hooks::LocalQueueMode;

use super::LocalQueue;

impl LocalQueue {
    pub async fn get_job(&self) -> Option<Job> {
        let mode = *self.0.mode.read().await;
        if mode == LocalQueueMode::Released {
            return None;
        }

        self.get_job_from_cache().await
    }

    async fn get_job_from_cache(&self) -> Option<Job> {
        {
            let mode = *self.0.mode.read().await;
            if mode == LocalQueueMode::TtlExpired {
                self.set_mode(LocalQueueMode::Polling).await;
            }
            if mode == LocalQueueMode::Released {
                return None;
            }
        }

        let mut buffers = self.0.job_queues.lock().await;
        if let Some(job) = buffers.pop_round_robin() {
            let remaining = buffers.len();
            drop(buffers);

            if remaining == 0 {
                self.set_mode(LocalQueueMode::Polling).await;
            }

            return Some(job);
        }

        None
    }
}
