use graphile_worker_ctx::WorkerContext;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::batch::BatchTaskHandler;
use super::outcome::{TaskHandlerFn, TaskHandlerOutcome};
use super::runner::{run_batch_task_from_worker_ctx, run_task_from_worker_ctx_outcome};
use super::task::TaskHandler;

/// Reusable registration value for a [`TaskHandler`].
///
/// `JobDefinition` lets crates and modules expose the jobs they provide as
/// values, so applications can register a collection of jobs in one call.
#[derive(Clone)]
pub struct JobDefinition {
    identifier: String,
    handler: TaskHandlerFn,
}

impl JobDefinition {
    /// Creates a job definition for a task handler type.
    pub fn of<T: TaskHandler>() -> Self {
        let handler = move |ctx: WorkerContext| {
            let ctx = ctx.clone();
            Box::pin(run_task_from_worker_ctx_outcome::<T>(ctx))
                as Pin<Box<dyn Future<Output = TaskHandlerOutcome> + Send>>
        };

        Self {
            identifier: T::IDENTIFIER.to_string(),
            handler: Arc::new(handler),
        }
    }

    /// Creates a job definition for a batch task handler type.
    pub fn of_batch<T: BatchTaskHandler>() -> Self {
        let handler = move |ctx: WorkerContext| {
            let ctx = ctx.clone();
            Box::pin(run_batch_task_from_worker_ctx::<T>(ctx))
                as Pin<Box<dyn Future<Output = TaskHandlerOutcome> + Send>>
        };

        Self {
            identifier: T::IDENTIFIER.to_string(),
            handler: Arc::new(handler),
        }
    }

    /// Replaces the static handler identifier with a runtime-composed one.
    ///
    /// The handler and payload type stay unchanged. This lets applications
    /// register a typed execution boundary under an identifier contributed by
    /// their runtime registry.
    pub fn with_identifier(mut self, identifier: impl Into<String>) -> Self {
        self.identifier = identifier.into();
        self
    }

    /// The identifier handled by this definition.
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// The type-erased task handler function.
    pub fn handler(&self) -> TaskHandlerFn {
        self.handler.clone()
    }

    /// Splits this definition into the identifier and handler function.
    pub fn into_parts(self) -> (String, TaskHandlerFn) {
        (self.identifier, self.handler)
    }
}
