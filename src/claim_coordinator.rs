use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use futures::FutureExt;
use graphile_worker_database::{Database, Schema};
use graphile_worker_job::Job;
use graphile_worker_queries::claim_queue_jobs::{claim_queue_jobs, lock_worker_control};
use graphile_worker_queries::errors::Result;
use graphile_worker_queries::task_identifiers::SharedTaskDetails;
use graphile_worker_runtime as runtime;
use graphile_worker_shutdown_signal::ShutdownSignal;

#[derive(Debug)]
pub(crate) struct QueueJobs {
    pub(crate) identifier: String,
    pub(crate) jobs: Vec<Job>,
}

#[derive(Debug)]
pub(crate) enum ClaimWave {
    Paused,
    Claimed(Vec<QueueJobs>),
}

#[derive(Default)]
struct CoordinatorState {
    queue_rotation: usize,
    ordering_cursors: HashMap<i32, Option<i32>>,
}

/// Serializes one process's claim waves and allocates its fixed capacity fairly
/// across the registered task identifiers.
pub(crate) struct ClaimCoordinator {
    database: Database,
    schema: Schema,
    worker_id: String,
    task_details: SharedTaskDetails,
    forbidden_flags: Vec<String>,
    use_local_time: bool,
    state: runtime::Mutex<CoordinatorState>,
}

impl ClaimCoordinator {
    pub(crate) fn new(
        database: Database,
        schema: Schema,
        worker_id: String,
        task_details: SharedTaskDetails,
        forbidden_flags: Vec<String>,
        use_local_time: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            database,
            schema,
            worker_id,
            task_details,
            forbidden_flags,
            use_local_time,
            state: runtime::Mutex::new(CoordinatorState::default()),
        })
    }

    pub(crate) async fn claim_one_until_shutdown(
        &self,
        shutdown: ShutdownSignal,
    ) -> Result<Option<Job>> {
        self.claim_one_with_shutdown(Some(shutdown)).await
    }

    async fn claim_one_with_shutdown(
        &self,
        shutdown: Option<ShutdownSignal>,
    ) -> Result<Option<Job>> {
        let queue_count = self.task_details.read().await.entries().len();
        for _ in 0..queue_count {
            let Some(wave) = self.claim_wave_with_shutdown(1, shutdown.clone()).await? else {
                return Ok(None);
            };
            match wave {
                ClaimWave::Paused => return Ok(None),
                ClaimWave::Claimed(queues) => {
                    if let Some(job) = queues.into_iter().find_map(|queue| {
                        debug_assert!(queue
                            .jobs
                            .iter()
                            .all(|job| job.task_identifier() == &queue.identifier));
                        queue.jobs.into_iter().next()
                    }) {
                        return Ok(Some(job));
                    }
                }
            }
        }
        Ok(None)
    }

    pub(crate) async fn restart_task_scan(&self, task_id: i32) {
        self.state
            .lock()
            .await
            .ordering_cursors
            .insert(task_id, None);
    }

    pub(crate) async fn claim_wave(&self, capacity: usize) -> Result<ClaimWave> {
        Ok(self
            .claim_wave_with_shutdown(capacity, None)
            .await?
            .expect("a claim wave without a shutdown signal cannot be cancelled"))
    }

    async fn claim_wave_with_shutdown(
        &self,
        capacity: usize,
        shutdown: Option<ShutdownSignal>,
    ) -> Result<Option<ClaimWave>> {
        if capacity == 0 {
            return Ok(Some(ClaimWave::Claimed(Vec::new())));
        }

        let mut tasks = self.task_details.read().await.entries();
        tasks.sort_by(|left, right| left.1.cmp(&right.1));
        if tasks.is_empty() {
            return Ok(Some(ClaimWave::Claimed(Vec::new())));
        }

        let mut state = self.state.lock().await;
        let allocation = allocate_quotas(tasks.len(), capacity, state.queue_rotation);
        let tx = self.database.begin().await?;
        let control = if let Some(shutdown) = shutdown {
            let lock_control = lock_worker_control(&tx, self.schema.clone()).fuse();
            let shutdown = shutdown.fuse();
            futures::pin_mut!(lock_control, shutdown);
            futures::select_biased! {
                result = lock_control => result?,
                _ = shutdown => return Ok(None),
            }
        } else {
            lock_worker_control(&tx, self.schema.clone()).await?
        };
        if control.paused {
            tx.commit().await?;
            return Ok(Some(ClaimWave::Paused));
        }

        let now = self.use_local_time.then(Utc::now);
        let mut claimed = Vec::with_capacity(allocation.len());
        let mut next_cursors = Vec::with_capacity(allocation.len());
        for (task_index, quota) in &allocation {
            let (task_id, identifier) = &tasks[*task_index];
            let cursor = state.ordering_cursors.get(task_id).copied().flatten();
            let queue_claim = claim_queue_jobs(
                &tx,
                self.schema.clone(),
                &self.worker_id,
                *task_id,
                identifier,
                &self.forbidden_flags,
                (*quota).try_into().unwrap_or(i32::MAX),
                cursor,
                now,
            )
            .await?;
            next_cursors.push((*task_id, queue_claim.next_ordering_cursor));
            claimed.push(QueueJobs {
                identifier: identifier.clone(),
                jobs: queue_claim.jobs,
            });
        }
        tx.commit().await?;

        for (task_id, cursor) in next_cursors {
            state.ordering_cursors.insert(task_id, cursor);
        }
        state.queue_rotation = next_rotation(tasks.len(), capacity, state.queue_rotation);
        Ok(Some(ClaimWave::Claimed(claimed)))
    }
}

fn allocate_quotas(queue_count: usize, capacity: usize, start: usize) -> Vec<(usize, usize)> {
    let visited = queue_count.min(capacity);
    let base = capacity / queue_count;
    let remainder = capacity % queue_count;

    (0..visited)
        .map(|offset| {
            let task_index = (start + offset) % queue_count;
            let quota = if capacity < queue_count {
                1
            } else {
                base + usize::from(offset < remainder)
            };
            (task_index, quota)
        })
        .collect()
}

fn next_rotation(queue_count: usize, capacity: usize, start: usize) -> usize {
    let advance = if capacity < queue_count { capacity } else { 1 };
    (start + advance) % queue_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_capacity_rotates_across_queues() {
        assert_eq!(allocate_quotas(3, 2, 0), vec![(0, 1), (1, 1)]);
        assert_eq!(next_rotation(3, 2, 0), 2);
        assert_eq!(allocate_quotas(3, 2, 2), vec![(2, 1), (0, 1)]);
    }

    #[test]
    fn remainder_slot_rotates_without_redistribution() {
        assert_eq!(allocate_quotas(3, 5, 0), vec![(0, 2), (1, 2), (2, 1)]);
        assert_eq!(allocate_quotas(3, 5, 1), vec![(1, 2), (2, 2), (0, 1)]);
    }
}
