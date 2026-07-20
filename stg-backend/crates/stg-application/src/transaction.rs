use async_trait::async_trait;
use stg_domain::{DomainError, DomainEvent};
use uuid::Uuid;

/// TransactionHandle abstracts database transaction operations.
/// Repositories use this instead of raw sqlx::Transaction.
#[async_trait]
pub trait TransactionHandle: Send + Sync {
    /// Commit the transaction
    async fn commit(self: Box<Self>) -> Result<(), DomainError>;
    /// Rollback the transaction
    async fn rollback(self: Box<Self>) -> Result<(), DomainError>;
    /// Get the inner transaction object for raw SQL operations
    fn as_mut_tx(&mut self) -> &mut dyn std::any::Any;
}

/// TransactionManager creates and manages database transactions.
/// Application services depend on this instead of raw PgPool.
#[async_trait]
pub trait TransactionManager: Send + Sync {
    async fn begin_transaction(&self) -> Result<Box<dyn TransactionHandle>, DomainError>;
}

/// Outbox is the transactional outbox interface.
/// Every state mutation MUST enqueue an OutboxRecord inside the same transaction.
#[derive(Debug, Clone)]
pub struct OutboxRecord {
    pub id: Uuid,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub payload_json: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[async_trait]
pub trait OutboxStorage: Send + Sync {
    /// Enqueue an outbox record within the current transaction.
    async fn enqueue_in_tx(
        &self,
        tx: &mut dyn TransactionHandle,
        record: &OutboxRecord,
    ) -> Result<(), DomainError>;

    /// Fetch pending records (for outbox worker).
    async fn fetch_pending(&self, batch_size: i32)
        -> Result<Vec<(OutboxRecord, i32)>, DomainError>;

    /// Mark a record as published.
    async fn mark_published(&self, id: Uuid) -> Result<(), DomainError>;

    /// Mark a record as failed with retry info.
    async fn mark_failed(
        &self,
        id: Uuid,
        attempt_count: i32,
        error: &str,
        available_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), DomainError>;
}

/// DomainEventBus collects domain events and routes them to the outbox.
/// Domain layer emits events, application layer collects them,
/// infrastructure publishes them.
pub struct DomainEventCollector {
    events: Vec<DomainEvent>,
    correlation_id: Uuid,
}

impl DomainEventCollector {
    pub fn new(correlation_id: Uuid) -> Self {
        Self {
            events: Vec::new(),
            correlation_id,
        }
    }

    /// Record a domain event for later persistence.
    pub fn record(&mut self, event: DomainEvent) {
        self.events.push(event);
    }

    /// Get all collected events.
    pub fn events(&self) -> &[DomainEvent] {
        &self.events
    }

    /// Drain all events and convert to OutboxRecords.
    pub fn drain_to_outbox(&mut self) -> Vec<OutboxRecord> {
        self.events
            .drain(..)
            .map(|event| {
                let payload_json = serde_json::to_value(&event)
                    .unwrap_or_else(|_| serde_json::json!({"error": "serialization_failed"}));
                let event_type = match &event.payload {
                    stg_domain::DomainEventPayload::PlayerRegistered { .. } => {
                        "PlayerRegistered".to_string()
                    }
                    stg_domain::DomainEventPayload::MoneyTransferred { .. } => {
                        "MoneyTransferred".to_string()
                    }
                    stg_domain::DomainEventPayload::EnergyModeChanged { .. } => {
                        "EnergyModeChanged".to_string()
                    }
                    stg_domain::DomainEventPayload::SessionCreated { .. } => {
                        "SessionCreated".to_string()
                    }
                    stg_domain::DomainEventPayload::SessionTerminated { .. } => {
                        "SessionTerminated".to_string()
                    }
                    stg_domain::DomainEventPayload::SessionReconnected { .. } => {
                        "SessionReconnected".to_string()
                    }
                };
                OutboxRecord {
                    id: Uuid::new_v4(),
                    aggregate_type: event.aggregate_type.clone(),
                    aggregate_id: event.aggregate_id,
                    event_type,
                    payload_json,
                    created_at: event.occurred_at,
                }
            })
            .collect()
    }

    pub fn correlation_id(&self) -> Uuid {
        self.correlation_id
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }
}
