use async_trait::async_trait;
use sqlx::{PgPool, Row};
use stg_application::transaction::{
    OutboxRecord, OutboxStorage, TransactionHandle, TransactionManager,
};
use stg_domain::DomainError;
use uuid::Uuid;

/// PostgresTransactionHandle wraps a sqlx Postgres transaction.
pub struct PostgresTransactionHandle {
    tx: Option<sqlx::Transaction<'static, sqlx::Postgres>>,
}

impl PostgresTransactionHandle {
    pub fn new(tx: sqlx::Transaction<'static, sqlx::Postgres>) -> Self {
        Self { tx: Some(tx) }
    }

    pub fn inner(&mut self) -> &mut sqlx::Transaction<'static, sqlx::Postgres> {
        self.tx.as_mut().expect("Transaction already consumed")
    }
}

#[async_trait]
impl TransactionHandle for PostgresTransactionHandle {
    async fn commit(mut self: Box<Self>) -> Result<(), DomainError> {
        let tx = self.tx.take().ok_or_else(|| {
            DomainError::InternalStateError("Transaction already consumed".into())
        })?;
        tx.commit()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), DomainError> {
        let tx = self.tx.take().ok_or_else(|| {
            DomainError::InternalStateError("Transaction already consumed".into())
        })?;
        tx.rollback()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))
    }

    fn as_mut_tx(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Manages PostgreSQL transactions via sqlx PgPool.
pub struct PostgresTransactionManager {
    pool: PgPool,
}

impl PostgresTransactionManager {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TransactionManager for PostgresTransactionManager {
    async fn begin_transaction(&self) -> Result<Box<dyn TransactionHandle>, DomainError> {
        let tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        Ok(Box::new(PostgresTransactionHandle::new(tx)))
    }
}

/// OutboxStorage implementation backed by PostgreSQL.
pub struct PostgresOutboxStorage {
    pool: PgPool,
}

impl PostgresOutboxStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OutboxStorage for PostgresOutboxStorage {
    async fn enqueue_in_tx(
        &self,
        tx: &mut dyn TransactionHandle,
        record: &OutboxRecord,
    ) -> Result<(), DomainError> {
        let pg_tx = tx
            .as_mut_tx()
            .downcast_mut::<PostgresTransactionHandle>()
            .ok_or_else(|| {
                DomainError::InternalStateError("Invalid transaction handle type".into())
            })?;
        let inner = pg_tx.inner();

        // sqlx execute requires &mut *Transaction to deref to Connection for Executor
        sqlx::query(
            r#"
            INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload_json, status, attempt_count, available_at, created_at)
            VALUES ($1, $2, $3, $4, $5, 'PENDING', 0, $6, $7)
            "#,
        )
        .bind(record.id)
        .bind(&record.aggregate_type)
        .bind(record.aggregate_id)
        .bind(&record.event_type)
        .bind(&record.payload_json)
        .bind(record.created_at)
        .bind(record.created_at)
        .execute(&mut **inner)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn fetch_pending(
        &self,
        batch_size: i32,
    ) -> Result<Vec<(OutboxRecord, i32)>, DomainError> {
        let rows = sqlx::query(
            r#"
            SELECT id, aggregate_type, aggregate_id, event_type, payload_json, attempt_count, created_at
            FROM outbox_events
            WHERE (status = 'PENDING' OR status = 'FAILED') AND available_at <= NOW()
            ORDER BY created_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let mut results = Vec::new();
        for row in rows {
            let id: Uuid = row.get("id");
            let aggregate_type: String = row.get("aggregate_type");
            let aggregate_id: Uuid = row.get("aggregate_id");
            let event_type: String = row.get("event_type");
            let payload_json: serde_json::Value = row.get("payload_json");
            let attempt_count: i32 = row.get("attempt_count");
            let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");

            results.push((
                OutboxRecord {
                    id,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    payload_json,
                    created_at,
                },
                attempt_count,
            ));
        }

        Ok(results)
    }

    async fn mark_published(&self, id: Uuid) -> Result<(), DomainError> {
        sqlx::query("UPDATE outbox_events SET status = 'PUBLISHED' WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        Ok(())
    }

    async fn mark_failed(
        &self,
        id: Uuid,
        attempt_count: i32,
        error: &str,
        available_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), DomainError> {
        sqlx::query(
            r#"
            UPDATE outbox_events
            SET status = 'FAILED', attempt_count = $1, last_error = $2, available_at = $3
            WHERE id = $4
            "#,
        )
        .bind(attempt_count)
        .bind(error)
        .bind(available_at)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        Ok(())
    }
}
