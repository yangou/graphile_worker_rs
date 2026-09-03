use graphile_worker_lifecycle_hooks::{LocalQueueMode, LocalQueueSetModeContext};
use tracing::trace;

use super::super::LocalQueue;

impl LocalQueue {
    pub(in crate::local_queue) async fn set_mode(&self, new_mode: LocalQueueMode) {
        let mut mode = self.0.mode.write().await;
        let old_mode = *mode;
        // Release is terminal. In particular, an in-flight database claim may
        // finish after shutdown has started and must not revive this queue.
        if old_mode == LocalQueueMode::Released || old_mode == new_mode {
            return;
        }
        *mode = new_mode;
        drop(mode);

        self.emit_mode_transition(old_mode, new_mode).await;
    }

    pub(in crate::local_queue) async fn emit_mode_transition(
        &self,
        old_mode: LocalQueueMode,
        new_mode: LocalQueueMode,
    ) {
        trace!(?old_mode, ?new_mode, "LocalQueue mode transition");

        self.0
            .hooks
            .emit(LocalQueueSetModeContext {
                worker_id: self.0.worker_id.clone(),
                old_mode,
                new_mode,
            })
            .await;

        // schedule_fetch is the sole waiter; retain a permit if it has not
        // registered yet.
        self.0.state_notify.notify_one();
    }
}
