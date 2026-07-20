// LoggingQueuePublisher — A no-op QueuePublisher that logs published events.
//
// Extracted from main.rs into a shared module for use across the codebase.

use async_trait::async_trait;
use tracing::info;

use stg_application::QueuePublisher;
use stg_domain::{DomainError, DomainEvent};

/// A QueuePublisher implementation that simply logs events and acknowledges them.
/// Used as a placeholder until a real message queue is integrated.
#[derive(Clone)]
pub struct LoggingQueuePublisher;

#[async_trait]
impl QueuePublisher for LoggingQueuePublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), DomainError> {
        info!(
            aggregate_type = %event.aggregate_type,
            aggregate_id = %event.aggregate_id,
            event_id = %event.id,
            "Published event (logging publisher)"
        );
        Ok(())
    }
}
