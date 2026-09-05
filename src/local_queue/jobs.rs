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
        let mut mode = self.0.mode.write().await;
        if *mode == LocalQueueMode::Released {
            return None;
        }
        let old_mode = *mode;
        let mut buffers = self.0.job_queues.lock().await;
        let job = buffers.pop_round_robin();
        if old_mode == LocalQueueMode::TtlExpired || (job.is_some() && buffers.len() == 0) {
            *mode = LocalQueueMode::Polling;
        }
        let new_mode = *mode;
        drop(buffers);
        drop(mode);

        if new_mode != old_mode {
            self.emit_mode_transition(old_mode, new_mode).await;
        }

        job
    }
}
