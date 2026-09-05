use crate::local_queue::LocalQueueConfig;

use super::WorkerOptions;

impl WorkerOptions {
    /// Enables the LocalQueue with the specified configuration.
    ///
    /// LocalQueue batch-fetches jobs from the database to reduce DB load,
    /// trading latency for throughput. Jobs are cached locally and distributed
    /// to workers without additional database queries until the cache is empty.
    ///
    /// # Arguments
    /// * `config` - The LocalQueue configuration (size and TTL)
    ///
    /// # Note
    /// When LocalQueue is enabled, jobs may experience slightly higher latency
    /// as they wait in the local cache. The cache has a TTL after which
    /// unclaimed jobs are returned to the database.
    ///
    /// One coordinator divides the process-wide `size` cap fairly across all
    /// registered task identifiers. Workers with `forbidden_flags` bypass the
    /// LocalQueue and fetch directly from the database; those direct fetches
    /// still use the same bounded claim path.
    ///
    /// # Example
    /// ```
    /// # use graphile_worker::{WorkerOptions, LocalQueueConfig, RefetchDelayConfig};
    /// # use std::time::Duration;
    ///
    /// let options = WorkerOptions::default()
    ///     .local_queue(
    ///         LocalQueueConfig::default()
    ///             .with_size(100)
    ///             .with_ttl(Duration::from_secs(300))
    ///             .with_refetch_delay(
    ///                 RefetchDelayConfig::default()
    ///                     .with_duration(Duration::from_millis(100))
    ///                     .with_threshold(10)
    ///                     .with_max_abort_threshold(500),
    ///             ),
    ///     );
    /// ```
    pub fn local_queue(mut self, config: LocalQueueConfig) -> Self {
        self.local_queue_config = Some(config);
        self
    }
}
