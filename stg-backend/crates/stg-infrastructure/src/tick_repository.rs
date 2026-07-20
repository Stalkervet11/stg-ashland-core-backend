use async_trait::async_trait;
use sqlx::PgPool;
use stg_application::TickRepository;
use stg_domain::{DomainError, TickMetrics, TickStatus};

/// PostgreSQL implementation of TickRepository.
pub struct PostgresTickRepository {
    pool: PgPool,
}

impl PostgresTickRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TickRepository for PostgresTickRepository {
    async fn get_latest_tick_number(&self) -> Result<Option<u64>, DomainError> {
        let row = sqlx::query_scalar::<_, i64>("SELECT MAX(tick_number) FROM simulation_ticks")
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(row.map(|v| v as u64))
    }

    async fn save_tick_metrics(&self, metrics: &TickMetrics) -> Result<(), DomainError> {
        let status_str = match metrics.status {
            TickStatus::InProgress => "IN_PROGRESS",
            TickStatus::Completed => "COMPLETED",
            TickStatus::PartialFailure => "PARTIAL_FAILURE",
            TickStatus::Failed => "FAILED",
        };

        sqlx::query(
            r#"
            INSERT INTO simulation_ticks (
                tick_id, tick_number, started_at, finished_at,
                duration_ms, status, total_events,
                total_entities_processed, subsystem_details
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (tick_number) DO UPDATE SET
                finished_at = EXCLUDED.finished_at,
                duration_ms = EXCLUDED.duration_ms,
                status = EXCLUDED.status,
                total_events = EXCLUDED.total_events,
                total_entities_processed = EXCLUDED.total_entities_processed,
                subsystem_details = EXCLUDED.subsystem_details
            "#,
        )
        .bind(metrics.tick_id.0)
        .bind(metrics.tick_number as i64)
        .bind(metrics.started_at)
        .bind(metrics.finished_at)
        .bind(metrics.duration_ms)
        .bind(status_str)
        .bind(metrics.total_events as i64)
        .bind(metrics.total_entities_processed as i64)
        .bind(serde_json::to_value(&metrics.subsystems).ok())
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn try_acquire_tick_lock(&self, tick_number: u64) -> Result<bool, DomainError> {
        // Use pg_try_advisory_xact_lock: returns false if lock not acquired,
        // true if acquired. Using xact-level so it auto-releases on commit/rollback.
        let result: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1) AS acquired")
            .bind(tick_number as i64)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use uuid::Uuid;

    async fn setup() -> PgPool {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@localhost:5432/stg_ashland_test".into()
        });
        let pool = PgPool::connect(&db_url).await.unwrap();
        let _ = sqlx::query("DELETE FROM simulation_ticks")
            .execute(&pool)
            .await;
        pool
    }

    #[tokio::test]
    #[ignore]
    async fn test_save_and_retrieve_latest() {
        let pool = setup().await;
        let repo = PostgresTickRepository::new(pool);

        let mut metrics = TickMetrics::new(stg_domain::TickId(Uuid::new_v4()), 1);
        metrics.finalize(TickStatus::Completed);
        repo.save_tick_metrics(&metrics).await.unwrap();

        let latest = repo.get_latest_tick_number().await.unwrap();
        assert_eq!(latest, Some(1));
    }

    #[tokio::test]
    #[ignore]
    async fn test_advisory_lock_acquired() {
        let pool = setup().await;
        let repo = PostgresTickRepository::new(pool);

        let locked = repo.try_acquire_tick_lock(42).await.unwrap();
        assert!(locked);
    }
}
