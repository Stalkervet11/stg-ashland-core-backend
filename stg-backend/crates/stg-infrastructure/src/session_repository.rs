use async_trait::async_trait;
use sqlx::{PgPool, Row};
use stg_application::{SessionRepository, TransactionHandle};
use stg_domain::{DomainError, PlayerId, PlayerSession, SessionId, SessionState};
use uuid::Uuid;

use crate::transaction_manager::PostgresTransactionHandle;

/// PostgreSQL implementation of SessionRepository.
/// Uses advisory locks + optimistic locking for concurrency safety.
pub struct PostgresSessionRepository {
    pool: PgPool,
}

impl PostgresSessionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn row_to_session(row: &sqlx::postgres::PgRow) -> Result<PlayerSession, DomainError> {
    let session_id: Uuid = row
        .try_get("session_id")
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
    let player_uuid: Uuid = row
        .try_get("player_uuid")
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
    let server_id: String = row
        .try_get("server_id")
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
    let state_str: String = row
        .try_get("state")
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
    let created_at: chrono::DateTime<chrono::Utc> = row
        .try_get("created_at")
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
    let updated_at: chrono::DateTime<chrono::Utc> = row
        .try_get("updated_at")
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
    let last_heartbeat: chrono::DateTime<chrono::Utc> = row
        .try_get("last_heartbeat")
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
    let expires_at: chrono::DateTime<chrono::Utc> = row
        .try_get("expires_at")
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
    let revision: i64 = row
        .try_get("revision")
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

    let state = match state_str.as_str() {
        "ACTIVE" => SessionState::Active,
        "EXPIRED" => SessionState::Expired,
        "TERMINATED" => SessionState::Terminated,
        "RECONNECTED" => SessionState::Reconnected,
        "TRANSITIONING" => SessionState::Transitioning,
        _ => {
            return Err(DomainError::InternalStateError(format!(
                "Invalid session state: {}",
                state_str
            )))
        }
    };

    Ok(PlayerSession {
        session_id: SessionId(session_id),
        player_id: PlayerId(player_uuid),
        server_id,
        state,
        created_at,
        updated_at,
        last_heartbeat,
        expires_at,
        revision: revision as u64,
    })
}

fn session_state_to_str(state: SessionState) -> &'static str {
    match state {
        SessionState::Active => "ACTIVE",
        SessionState::Expired => "EXPIRED",
        SessionState::Terminated => "TERMINATED",
        SessionState::Reconnected => "RECONNECTED",
        SessionState::Transitioning => "TRANSITIONING",
    }
}

#[async_trait]
impl SessionRepository for PostgresSessionRepository {
    async fn find_active_by_player(
        &self,
        player_id: PlayerId,
    ) -> Result<Option<PlayerSession>, DomainError> {
        let row = sqlx::query(
            r#"
            SELECT session_id, player_uuid, server_id, state, created_at, updated_at,
                   last_heartbeat, expires_at, revision
            FROM sessions
            WHERE player_uuid = $1
              AND state IN ('ACTIVE', 'RECONNECTED', 'TRANSITIONING')
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(player_id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(ref r) => Ok(Some(row_to_session(r)?)),
            None => Ok(None),
        }
    }

    async fn find_by_id(
        &self,
        session_id: SessionId,
    ) -> Result<Option<PlayerSession>, DomainError> {
        let row = sqlx::query(
            r#"
            SELECT session_id, player_uuid, server_id, state, created_at, updated_at,
                   last_heartbeat, expires_at, revision
            FROM sessions
            WHERE session_id = $1
            "#,
        )
        .bind(session_id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(ref r) => Ok(Some(row_to_session(r)?)),
            None => Ok(None),
        }
    }

    async fn save(&self, session: &PlayerSession) -> Result<(), DomainError> {
        let state_str = session_state_to_str(session.state);
        sqlx::query(
            r#"
            INSERT INTO sessions (session_id, player_uuid, server_id, state, created_at,
                                   updated_at, last_heartbeat, expires_at, revision)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(session.session_id.0)
        .bind(session.player_id.0)
        .bind(&session.server_id)
        .bind(state_str)
        .bind(session.created_at)
        .bind(session.updated_at)
        .bind(session.last_heartbeat)
        .bind(session.expires_at)
        .bind(session.revision as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn update(&self, session: &PlayerSession) -> Result<(), DomainError> {
        let state_str = session_state_to_str(session.state);
        let old_revision = (session.revision.saturating_sub(1)) as i64;
        let new_revision = session.revision as i64;

        let result = sqlx::query(
            r#"
            UPDATE sessions
            SET server_id = $1, state = $2, updated_at = $3,
                last_heartbeat = $4, expires_at = $5, revision = $6
            WHERE session_id = $7 AND revision = $8
            "#,
        )
        .bind(&session.server_id)
        .bind(state_str)
        .bind(session.updated_at)
        .bind(session.last_heartbeat)
        .bind(session.expires_at)
        .bind(new_revision)
        .bind(session.session_id.0)
        .bind(old_revision)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if result.rows_affected() == 0 {
            // Read actual revision for diagnostics
            let actual_row = sqlx::query("SELECT revision FROM sessions WHERE session_id = $1")
                .bind(session.session_id.0)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

            let actual_revision = actual_row
                .and_then(|r| r.try_get::<i64, _>("revision").ok())
                .unwrap_or(-1) as u64;

            return Err(DomainError::RevisionConflict(
                "Session".to_string(),
                session.revision,
                actual_revision,
            ));
        }

        Ok(())
    }

    async fn find_active_by_player_for_update(
        &self,
        tx: &mut dyn TransactionHandle,
        player_id: PlayerId,
    ) -> Result<Option<PlayerSession>, DomainError> {
        let pg_tx = tx
            .as_mut_tx()
            .downcast_mut::<PostgresTransactionHandle>()
            .ok_or_else(|| {
                DomainError::InternalStateError("Invalid transaction handle type".into())
            })?;
        let inner = pg_tx.inner();

        let row = sqlx::query(
            r#"
            SELECT session_id, player_uuid, server_id, state, created_at, updated_at,
                   last_heartbeat, expires_at, revision
            FROM sessions
            WHERE player_uuid = $1
              AND state IN ('ACTIVE', 'RECONNECTED', 'TRANSITIONING')
            ORDER BY created_at DESC
            LIMIT 1
            FOR UPDATE
            "#,
        )
        .bind(player_id.0)
        .fetch_optional(&mut **inner)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(ref r) => Ok(Some(row_to_session(r)?)),
            None => Ok(None),
        }
    }

    async fn update_in_tx(
        &self,
        tx: &mut dyn TransactionHandle,
        session: &PlayerSession,
    ) -> Result<(), DomainError> {
        let pg_tx = tx
            .as_mut_tx()
            .downcast_mut::<PostgresTransactionHandle>()
            .ok_or_else(|| {
                DomainError::InternalStateError("Invalid transaction handle type".into())
            })?;
        let inner = pg_tx.inner();

        let state_str = session_state_to_str(session.state);
        let old_revision = (session.revision.saturating_sub(1)) as i64;
        let new_revision = session.revision as i64;

        let result = sqlx::query(
            r#"
            UPDATE sessions
            SET server_id = $1, state = $2, updated_at = $3,
                last_heartbeat = $4, expires_at = $5, revision = $6
            WHERE session_id = $7 AND revision = $8
            "#,
        )
        .bind(&session.server_id)
        .bind(state_str)
        .bind(session.updated_at)
        .bind(session.last_heartbeat)
        .bind(session.expires_at)
        .bind(new_revision)
        .bind(session.session_id.0)
        .bind(old_revision)
        .execute(&mut **inner)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if result.rows_affected() == 0 {
            // Read actual revision for diagnostics (within transaction)
            let actual_row = sqlx::query("SELECT revision FROM sessions WHERE session_id = $1")
                .bind(session.session_id.0)
                .fetch_optional(&mut **inner)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

            let actual_revision = actual_row
                .and_then(|r| r.try_get::<i64, _>("revision").ok())
                .unwrap_or(-1) as u64;

            return Err(DomainError::RevisionConflict(
                "Session".to_string(),
                session.revision,
                actual_revision,
            ));
        }

        Ok(())
    }

    async fn save_in_tx(
        &self,
        tx: &mut dyn TransactionHandle,
        session: &PlayerSession,
    ) -> Result<(), DomainError> {
        let pg_tx = tx
            .as_mut_tx()
            .downcast_mut::<PostgresTransactionHandle>()
            .ok_or_else(|| {
                DomainError::InternalStateError("Invalid transaction handle type".into())
            })?;
        let inner = pg_tx.inner();

        let state_str = session_state_to_str(session.state);
        sqlx::query(
            r#"
            INSERT INTO sessions (session_id, player_uuid, server_id, state, created_at,
                                   updated_at, last_heartbeat, expires_at, revision)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(session.session_id.0)
        .bind(session.player_id.0)
        .bind(&session.server_id)
        .bind(state_str)
        .bind(session.created_at)
        .bind(session.updated_at)
        .bind(session.last_heartbeat)
        .bind(session.expires_at)
        .bind(session.revision as i64)
        .execute(&mut **inner)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn expire_stale_sessions(&self) -> Result<u64, DomainError> {
        let result = sqlx::query(
            r#"
            UPDATE sessions
            SET state = 'EXPIRED', updated_at = NOW(), revision = revision + 1
            WHERE state IN ('ACTIVE', 'RECONNECTED')
              AND expires_at < NOW()
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(result.rows_affected())
    }

    async fn begin_transition_atomic(
        &self,
        player_id: PlayerId,
        from_server_id: &str,
        to_server_id: &str,
        ticket: &str,
    ) -> Result<PlayerSession, DomainError> {
        // Use pg_advisory_xact_lock to serialize access per player
        // The lock key is derived from the player UUID (first 8 bytes as i64)
        let lock_key = (player_id.0.as_u128() & 0x7FFFFFFFFFFFFFFF) as i64;

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Acquire advisory lock for this player
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Find active session for this player
        let row = sqlx::query(
            r#"
            SELECT session_id, player_uuid, server_id, state, created_at, updated_at,
                   last_heartbeat, expires_at, revision
            FROM sessions
            WHERE player_uuid = $1
              AND state IN ('ACTIVE', 'RECONNECTED')
            ORDER BY created_at DESC
            LIMIT 1
            FOR UPDATE
            "#,
        )
        .bind(player_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let session = match row {
            Some(ref r) => row_to_session(r)?,
            None => {
                // No active session – create one for the transition
                let new_session = PlayerSession::new(
                    player_id,
                    to_server_id.to_string(),
                    300, // default timeout
                );
                let mut s = new_session;
                s.mark_transitioning(to_server_id.to_string());

                let state_str = session_state_to_str(s.state);
                sqlx::query(
                    r#"
                    INSERT INTO sessions (session_id, player_uuid, server_id, state, created_at,
                                           updated_at, last_heartbeat, expires_at, revision)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    "#,
                )
                .bind(s.session_id.0)
                .bind(s.player_id.0)
                .bind(&s.server_id)
                .bind(state_str)
                .bind(s.created_at)
                .bind(s.updated_at)
                .bind(s.last_heartbeat)
                .bind(s.expires_at)
                .bind(s.revision as i64)
                .execute(&mut *tx)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

                tx.commit()
                    .await
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                return Ok(s);
            }
        };

        // Verify server ownership
        if session.server_id != from_server_id {
            tx.rollback()
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            return Err(DomainError::ServerIdentityMismatch {
                expected: from_server_id.to_string(),
                actual: session.server_id,
            });
        }

        // Mark session as transitioning
        let mut session = session;
        session.mark_transitioning(to_server_id.to_string());

        let state_str = session_state_to_str(session.state);
        let old_revision = (session.revision.saturating_sub(1)) as i64;
        let new_revision = session.revision as i64;

        sqlx::query(
            r#"
            UPDATE sessions
            SET server_id = $1, state = $2, updated_at = $3,
                last_heartbeat = $4, expires_at = $5, revision = $6
            WHERE session_id = $7 AND revision = $8
            "#,
        )
        .bind(&session.server_id)
        .bind(state_str)
        .bind(session.updated_at)
        .bind(session.last_heartbeat)
        .bind(session.expires_at)
        .bind(new_revision)
        .bind(session.session_id.0)
        .bind(old_revision)
        .execute(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Also insert a transition record
        sqlx::query(
            r#"
            INSERT INTO player_transitions (transition_id, player_uuid, ticket,
                                             from_server_id, to_server_id, status, created_at)
            VALUES ($1, $2, $3, $4, $5, 'PENDING', NOW())
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(player_id.0)
        .bind(ticket)
        .bind(from_server_id)
        .bind(to_server_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(session)
    }
}
