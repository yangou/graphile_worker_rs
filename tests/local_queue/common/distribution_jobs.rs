use super::*;
use std::sync::{LazyLock, Mutex};

#[derive(Serialize, Deserialize)]
pub(super) struct ConcurrentDistributionJob {
    pub(super) id: u32,
}

pub(super) static CONCURRENT_CALL_COUNT: StaticCounter = StaticCounter::new();

impl TaskHandler for ConcurrentDistributionJob {
    const IDENTIFIER: &'static str = "concurrent_distribution_job";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        runtime_sleep(Duration::from_millis(100)).await;
        CONCURRENT_CALL_COUNT.increment().await;
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct ModeTransitionJob {
    pub(super) id: u32,
}

pub(super) static MODE_TRANSITION_CALL_COUNT: StaticCounter = StaticCounter::new();

impl TaskHandler for ModeTransitionJob {
    const IDENTIFIER: &'static str = "mode_transition_job";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        runtime_sleep(Duration::from_millis(50)).await;
        MODE_TRANSITION_CALL_COUNT.increment().await;
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct EmptyQueueJob {
    pub(super) id: u32,
}

pub(super) static EMPTY_QUEUE_CALL_COUNT: StaticCounter = StaticCounter::new();

impl TaskHandler for EmptyQueueJob {
    const IDENTIFIER: &'static str = "empty_queue_job";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        EMPTY_QUEUE_CALL_COUNT.increment().await;
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct LargeBatchJob {
    pub(super) id: u32,
}

pub(super) static LARGE_BATCH_CALL_COUNT: StaticCounter = StaticCounter::new();

impl TaskHandler for LargeBatchJob {
    const IDENTIFIER: &'static str = "large_batch_job";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        LARGE_BATCH_CALL_COUNT.increment().await;
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct RuntimeQueueJob;

pub(super) static RUNTIME_QUEUE_ORDER: LazyLock<Mutex<Vec<String>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

impl TaskHandler for RuntimeQueueJob {
    const IDENTIFIER: &'static str = "runtime_queue_template";

    async fn run(self, ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        RUNTIME_QUEUE_ORDER
            .lock()
            .expect("runtime queue order lock")
            .push(ctx.job().task_identifier().clone());
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct CapacityBoundJob;

pub(super) static CAPACITY_BOUND_STARTED: StaticCounter = StaticCounter::new();

impl TaskHandler for CapacityBoundJob {
    const IDENTIFIER: &'static str = "capacity_bound_job";

    async fn run(self, _ctx: WorkerContext) -> impl IntoTaskHandlerResult {
        CAPACITY_BOUND_STARTED.increment().await;
        runtime_sleep(Duration::from_millis(500)).await;
    }
}
