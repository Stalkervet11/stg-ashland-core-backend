use async_trait::async_trait;
use sqlx::PgPool;
use stg_application::TransitionRepository;
use stg_domain::{DomainError, PlayerId, PlayerTransition, TransitionId, TransitionStatus};

/// PostgreSQL implementation of TransitionRepository.
/// Currently uses the domain_events table as a simple backing store.
/// Replace with a dedicated transitions table when full player-migration support is needed.
pub struct PostgresTransitionRepository {
    pool: PgPool,
}

impl PostgresTransitionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TransitionRepository for PostgresTransitionRepository {
    async fn find_by_id(&self, id: TransitionId) -> Result<Option<PlayerTransition>, DomainError> {
        let row = sqlx::query_as::<_, (String, String, i32, String)>(
            r#"
            SELECT event_type::text, aggregate_id::text, 0::int, ''::text
            FROM domain_events
            WHERE event_type = 'Transition'
              AND aggregate_id = $1
            LIMIT 1
            "#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some((_, aggregate_id_str, _, _)) => {
                let player_uuid = uuid::Uuid::parse_str(&aggregate_id_str)
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                Ok(Some(PlayerTransition {
                    id,
                    ticket: String::new(),
                    status: TransitionStatus::Pending,
                    player_id: PlayerId(player_uuid),
                }))
            }
            None => Ok(None),
        }
    }

    async fn find_by_ticket(&self, _ticket: &str) -> Result<Option<PlayerTransition>, DomainError> {
        // Ticket-based lookup requires a dedicated transitions table.
        // Return a stub for now.
        Ok(None)
    }

    async fn save_transition(&self, transition: &PlayerTransition) -> Result<(), DomainError> {
        sqlx::query(
            r#"
            INSERT INTO domain_events (id, aggregate_type, aggregate_id, aggregate_version, event_type, payload_json, occurred_at, correlation_id)
            VALUES ($1, 'Player', $2, 0, 'Transition', $3, $4, $5)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(transition.player_id.0)
        .bind(serde_json::json!({
            "transition_id": transition.id.0.to_string(),
            "ticket": transition.ticket,
            "status": transition.status as i32,
        }))
        .bind(chrono::Utc::now())
        .bind(uuid::Uuid::new_v4())
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        Ok(())
    }

    async fn update_transition(&self, transition: &PlayerTransition) -> Result<(), DomainError> {
        // For now, just insert a new event with updated status
        sqlx::query(
            r#"
            INSERT INTO domain_events (id, aggregate_type, aggregate_id, aggregate_version, event_type, payload_json, occurred_at, correlation_id)
            VALUES ($1, 'Player', $2, 0, 'TransitionUpdated', $3, $4, $5)
            "#,
        )
        .bind(uuid::Uuid::new_v4())
        .bind(transition.player_id.0)
        .bind(serde_json::json!({
            "transition_id": transition.id.0.to_string(),
            "ticket": transition.ticket,
            "status": transition.status as i32,
        }))
        .bind(chrono::Utc::now())
        .bind(uuid::Uuid::new_v4())
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        Ok(())
    }
}
