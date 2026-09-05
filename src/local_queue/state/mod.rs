mod access;
mod mode;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;

use graphile_worker_database::{Database, Schema};
use graphile_worker_job::Job;
use graphile_worker_lifecycle_hooks::{HookRegistry, LocalQueueMode};
use graphile_worker_runtime as runtime;

use crate::background_tasks::TaskSlot;
use crate::claim_coordinator::ClaimCoordinator;

use super::{AcceptedWorkTracker, LocalQueueConfig, LocalQueueParams, LocalQueueSignalSender};

#[derive(Default)]
pub(super) struct QueueBuffers {
    queues: Vec<(String, VecDeque<Job>)>,
    next_queue: usize,
}

impl QueueBuffers {
    pub(super) fn push(&mut self, identifier: String, jobs: Vec<Job>) {
        if jobs.is_empty() {
            return;
        }
        if let Some((_, queue)) = self
            .queues
            .iter_mut()
            .find(|(known, _)| known == &identifier)
        {
            queue.extend(jobs);
            return;
        }
        self.queues.push((identifier, jobs.into()));
    }

    pub(super) fn pop_round_robin(&mut self) -> Option<Job> {
        if self.queues.is_empty() {
            return None;
        }
        for _ in 0..self.queues.len() {
            let index = self.next_queue % self.queues.len();
            self.next_queue = (index + 1) % self.queues.len();
            if let Some(job) = self.queues[index].1.pop_front() {
                return Some(job);
            }
        }
        None
    }

    pub(super) fn len(&self) -> usize {
        self.queues.iter().map(|(_, queue)| queue.len()).sum()
    }

    pub(super) fn drain(&mut self) -> Vec<Job> {
        let mut jobs = Vec::with_capacity(self.len());
        for (_, queue) in &mut self.queues {
            jobs.extend(queue.drain(..));
        }
        jobs
    }
}

pub(super) struct RefetchDelayState {
    pub(super) active: AtomicBool,
    pub(super) fetch_on_complete: AtomicBool,
    pub(super) counter: AtomicUsize,
    pub(super) abort_threshold: runtime::RwLock<usize>,
    pub(super) abort_notify: runtime::Notify,
}

impl Default for RefetchDelayState {
    fn default() -> Self {
        Self {
            active: AtomicBool::new(false),
            fetch_on_complete: AtomicBool::new(false),
            counter: AtomicUsize::new(0),
            abort_threshold: runtime::RwLock::new(usize::MAX),
            abort_notify: runtime::Notify::new(),
        }
    }
}

pub(super) struct LocalQueueState {
    pub(super) mode: runtime::RwLock<LocalQueueMode>,
    pub(super) job_queues: runtime::Mutex<QueueBuffers>,
    pub(super) job_signal_sender: LocalQueueSignalSender,
    pub(super) fetch_in_progress: AtomicBool,
    pub(super) fetch_again: AtomicBool,
    pub(super) refetch_delay: RefetchDelayState,
    pub(super) state_notify: runtime::Notify,
    pub(super) run_task: TaskSlot,
    pub(super) shutdown_task: TaskSlot,
    pub(super) refetch_delay_task: TaskSlot,
    pub(super) ttl_timer_task: TaskSlot,
    pub(super) run_complete_notify: runtime::Notify,
    pub(super) release_lock: runtime::Mutex<()>,
    pub(super) release_finished: AtomicBool,
    pub(super) release_error: runtime::Mutex<Option<String>>,
    pub(super) config: LocalQueueConfig,
    pub(super) database: Database,
    pub(super) schema: Schema,
    pub(super) worker_id: String,
    pub(super) claim_coordinator: Arc<ClaimCoordinator>,
    pub(super) accepted_tracker: Arc<AcceptedWorkTracker>,
    pub(super) poll_interval: Duration,
    pub(super) shutdown_grace_period: Duration,
    pub(super) continuous: bool,
    pub(super) hooks: Arc<HookRegistry>,
}

impl LocalQueueState {
    pub(super) fn new(params: LocalQueueParams) -> Self {
        Self {
            mode: runtime::RwLock::new(LocalQueueMode::Starting),
            job_queues: runtime::Mutex::new(QueueBuffers::default()),
            job_signal_sender: params.job_signal_sender,
            fetch_in_progress: AtomicBool::new(false),
            fetch_again: AtomicBool::new(false),
            refetch_delay: RefetchDelayState::default(),
            state_notify: runtime::Notify::new(),
            run_task: TaskSlot::empty("local_queue_run"),
            shutdown_task: TaskSlot::empty("local_queue_shutdown"),
            refetch_delay_task: TaskSlot::empty("local_queue_refetch_delay"),
            ttl_timer_task: TaskSlot::empty("local_queue_ttl"),
            run_complete_notify: runtime::Notify::new(),
            release_lock: runtime::Mutex::new(()),
            release_finished: AtomicBool::new(false),
            release_error: runtime::Mutex::new(None),
            config: params.config,
            database: params.database,
            schema: params.schema,
            worker_id: params.worker_id,
            claim_coordinator: params.claim_coordinator,
            accepted_tracker: Arc::new(AcceptedWorkTracker::default()),
            poll_interval: params.poll_interval,
            shutdown_grace_period: params.shutdown_grace_period,
            continuous: params.continuous,
            hooks: params.hooks,
        }
    }
}
