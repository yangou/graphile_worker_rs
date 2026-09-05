use std::time::Duration;

use derive_builder::Builder;

use super::error::LocalQueueConfigError;
use super::refetch::RefetchDelayConfig;

const DEFAULT_LOCAL_QUEUE_SIZE: usize = 100;
const DEFAULT_LOCAL_QUEUE_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Builder)]
#[builder(build_fn(private, name = "build_internal"), pattern = "owned")]
pub struct LocalQueueConfig {
    /// Process-wide maximum number of accepted jobs. Buffered, running, and
    /// completion/failure-pending jobs all count against this limit.
    #[builder(default = "DEFAULT_LOCAL_QUEUE_SIZE")]
    pub size: usize,
    /// How long locally fetched jobs may stay unclaimed before being returned to the database.
    #[builder(default = "DEFAULT_LOCAL_QUEUE_TTL")]
    pub ttl: Duration,
    /// Optional delay strategy used when a fetch returns fewer jobs than requested.
    #[builder(default, setter(strip_option))]
    pub refetch_delay: Option<RefetchDelayConfig>,
}

impl Default for LocalQueueConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl LocalQueueConfig {
    pub fn builder() -> LocalQueueConfigBuilder {
        LocalQueueConfigBuilder::default()
    }

    pub fn validate(&self, poll_interval: Duration) -> Result<(), LocalQueueConfigError> {
        if let Some(refetch_delay) = self.refetch_delay.as_ref() {
            if refetch_delay.duration > poll_interval {
                return Err(LocalQueueConfigError::RefetchDelayExceedsPollInterval {
                    duration: refetch_delay.duration,
                    poll_interval,
                });
            }
        }

        if self.size == 0 {
            return Err(LocalQueueConfigError::EmptySize);
        }

        if self.size > i32::MAX as usize {
            return Err(LocalQueueConfigError::SizeTooLarge {
                size: self.size,
                max: i32::MAX,
            });
        }

        Ok(())
    }

    pub fn with_size(self, size: usize) -> Self {
        Self { size, ..self }
    }

    pub fn with_ttl(self, ttl: Duration) -> Self {
        Self { ttl, ..self }
    }

    pub fn with_refetch_delay(self, refetch_delay: RefetchDelayConfig) -> Self {
        Self {
            refetch_delay: Some(refetch_delay),
            ..self
        }
    }
}

impl LocalQueueConfigBuilder {
    pub fn build(self) -> LocalQueueConfig {
        self.build_internal()
            .expect("All fields have defaults, build should never fail")
    }
}
