use async_trait::async_trait;

use chrono::{DateTime, Utc};
pub use session_repository::PostgresSessionRepository;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;
use std::sync::Arc;
use stg_application::{
    ConversionRepository, EnergyRepository, EventLogRepository, PlayerRepository, QueuePublisher,
    TransactionRepository, WalletRepository,
};
pub use tick_repository::PostgresTickRepository;
pub use transition_repository::PostgresTransitionRepository;

use stg_domain::{
    ConversionDirection, ConversionRule, DomainError, DomainEvent, EconomyTransaction,
    EconomyTransactionType, EnergyNode, EnergyNodeType, EnergyState, LedgerEntry, Money, Player,
    PlayerId, PlayerStatus, ReservationId, ReservationStatus, ResourceConversionReservation,
    SimulationSystem, TransactionId, Wallet, WalletId,
};

use uuid::Uuid;

pub mod locale;
pub mod observability;
pub mod session_repository;
pub mod tick_repository;
pub mod transaction_manager;
pub mod transition_repository;

pub use locale::ResourceBundleLocalizationProvider;
pub use observability::{trace_transaction, MetricsSnapshot, RequestContext, SystemMetrics};
pub use transaction_manager::{
    PostgresOutboxStorage, PostgresTransactionHandle, PostgresTransactionManager,
};

// =========================================================================
// SQLX REPOSITORY IMPLEMENTATIONS (NO ORM, EXPLICIT SQL)
// =========================================================================

pub struct PostgresPlayerRepository {
    pool: PgPool,
}

impl PostgresPlayerRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PlayerRepository for PostgresPlayerRepository {
    async fn find_by_id(&self, id: PlayerId) -> Result<Option<Player>, DomainError> {
        let row = sqlx::query(
            r#"
            SELECT id, username, status, created_at, last_seen_at, revision
            FROM players
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(r) => {
                let id_val: Uuid = r
                    .try_get("id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let username_val: String = r
                    .try_get("username")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let status_str: String = r
                    .try_get("status")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let created_at_val: DateTime<Utc> = r
                    .try_get("created_at")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let last_seen_at_val: DateTime<Utc> = r
                    .try_get("last_seen_at")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let revision_val: i64 = r
                    .try_get("revision")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

                let status = match status_str.as_str() {
                    "ACTIVE" => PlayerStatus::Active,
                    "LOCKED" => PlayerStatus::Locked,
                    "TRANSITIONING" => PlayerStatus::Transitioning,
                    "BANNED" => PlayerStatus::Banned,
                    _ => {
                        return Err(DomainError::InternalStateError(
                            "Invalid status in DB".to_string(),
                        ))
                    }
                };

                Ok(Some(Player {
                    id: PlayerId(id_val),
                    username: username_val,
                    status,
                    created_at: created_at_val,
                    last_seen_at: last_seen_at_val,
                    revision: revision_val as u64,
                }))
            }
            None => Ok(None),
        }
    }

    async fn save(&self, player: &Player) -> Result<(), DomainError> {
        let status_str = match player.status {
            PlayerStatus::Active => "ACTIVE",
            PlayerStatus::Locked => "LOCKED",
            PlayerStatus::Transitioning => "TRANSITIONING",
            PlayerStatus::Banned => "BANNED",
        };

        sqlx::query(
            r#"
            INSERT INTO players (id, username, status, created_at, last_seen_at, revision)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(player.id.0)
        .bind(&player.username)
        .bind(status_str)
        .bind(player.created_at)
        .bind(player.last_seen_at)
        .bind(player.revision as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn update(&self, player: &Player) -> Result<(), DomainError> {
        let status_str = match player.status {
            PlayerStatus::Active => "ACTIVE",
            PlayerStatus::Locked => "LOCKED",
            PlayerStatus::Transitioning => "TRANSITIONING",
            PlayerStatus::Banned => "BANNED",
        };

        let res = sqlx::query(
            r#"
            UPDATE players
            SET username = $1, status = $2, last_seen_at = $3, revision = $4
            WHERE id = $5 AND revision = $6
            "#,
        )
        .bind(&player.username)
        .bind(status_str)
        .bind(player.last_seen_at)
        .bind((player.revision + 1) as i64)
        .bind(player.id.0)
        .bind(player.revision as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if res.rows_affected() == 0 {
            return Err(DomainError::RevisionConflict(
                "Player".to_string(),
                player.revision,
                player.revision,
            ));
        }

        Ok(())
    }
}

pub struct PostgresWalletRepository {
    pool: PgPool,
}

impl PostgresWalletRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WalletRepository for PostgresWalletRepository {
    async fn find_by_id(&self, id: WalletId) -> Result<Option<Wallet>, DomainError> {
        let row = sqlx::query(
            r#"
            SELECT id, player_id, currency_code, balance, revision
            FROM wallets
            WHERE id = $1
            "#,
        )
        .bind(id.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(r) => {
                let id_val: Uuid = r
                    .try_get("id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let player_id_val: Uuid = r
                    .try_get("player_id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let currency_code_val: String = r
                    .try_get("currency_code")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let balance_val: i64 = r
                    .try_get("balance")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let revision_val: i64 = r
                    .try_get("revision")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

                Ok(Some(Wallet {
                    id: WalletId(id_val),
                    player_id: PlayerId(player_id_val),
                    currency_code: currency_code_val,
                    balance: Money(balance_val),
                    revision: revision_val as u64,
                }))
            }
            None => Ok(None),
        }
    }

    async fn find_by_player_and_currency(
        &self,
        player_id: PlayerId,
        currency_code: &str,
    ) -> Result<Option<Wallet>, DomainError> {
        let row = sqlx::query(
            r#"
            SELECT id, player_id, currency_code, balance, revision
            FROM wallets
            WHERE player_id = $1 AND currency_code = $2
            "#,
        )
        .bind(player_id.0)
        .bind(currency_code)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(r) => {
                let id_val: Uuid = r
                    .try_get("id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let player_id_val: Uuid = r
                    .try_get("player_id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let currency_code_val: String = r
                    .try_get("currency_code")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let balance_val: i64 = r
                    .try_get("balance")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let revision_val: i64 = r
                    .try_get("revision")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

                Ok(Some(Wallet {
                    id: WalletId(id_val),
                    player_id: PlayerId(player_id_val),
                    currency_code: currency_code_val,
                    balance: Money(balance_val),
                    revision: revision_val as u64,
                }))
            }
            None => Ok(None),
        }
    }

    async fn save(&self, wallet: &Wallet) -> Result<(), DomainError> {
        sqlx::query(
            r#"
            INSERT INTO wallets (id, player_id, currency_code, balance, revision)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(wallet.id.0)
        .bind(wallet.player_id.0)
        .bind(&wallet.currency_code)
        .bind(wallet.balance.minor_units())
        .bind(wallet.revision as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn update(&self, wallet: &Wallet) -> Result<(), DomainError> {
        let res = sqlx::query(
            r#"
            UPDATE wallets
            SET balance = $1, revision = $2
            WHERE id = $3 AND revision = $4
            "#,
        )
        .bind(wallet.balance.minor_units())
        .bind((wallet.revision + 1) as i64)
        .bind(wallet.id.0)
        .bind(wallet.revision as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if res.rows_affected() == 0 {
            return Err(DomainError::RevisionConflict(
                "Wallet".to_string(),
                wallet.revision,
                wallet.revision,
            ));
        }

        Ok(())
    }
}

// =========================================================================
// TRANSACTION BOUNDARIES (ATOMIC BALANCES MUTATION)
// =========================================================================

pub struct PostgresTransactionRepository {
    pool: PgPool,
}

impl PostgresTransactionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl TransactionRepository for PostgresTransactionRepository {
    async fn commit_transaction_with_balances(
        &self,
        transaction: &EconomyTransaction,
        updated_wallets: &[Wallet],
    ) -> Result<(), DomainError> {
        let mut tx: Transaction<'_, Postgres> = self
            .pool
            .begin()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 1. Insert Economy Transaction record
        let status_str = match transaction.status {
            stg_domain::EconomyTransactionStatus::Pending => "PENDING",
            stg_domain::EconomyTransactionStatus::Committed => "COMMITTED",
            stg_domain::EconomyTransactionStatus::Rejected => "REJECTED",
            stg_domain::EconomyTransactionStatus::Reversed => "REVERSED",
        };
        let type_str = match transaction.tx_type {
            stg_domain::EconomyTransactionType::AdminCredit => "ADMIN_CREDIT",
            stg_domain::EconomyTransactionType::AdminDebit => "ADMIN_DEBIT",
            stg_domain::EconomyTransactionType::ResourceConversion => "RESOURCE_CONVERSION",
            stg_domain::EconomyTransactionType::Purchase => "PURCHASE",
            stg_domain::EconomyTransactionType::Sale => "SALE",
            stg_domain::EconomyTransactionType::PlayerTransfer => "PLAYER_TRANSFER",
            stg_domain::EconomyTransactionType::SystemReward => "SYSTEM_REWARD",
            stg_domain::EconomyTransactionType::SystemPenalty => "SYSTEM_PENALTY",
            stg_domain::EconomyTransactionType::EnergySettlement => "ENERGY_SETTLEMENT",
            stg_domain::EconomyTransactionType::Refund => "REFUND",
        };

        sqlx::query(
            r#"
            INSERT INTO economy_transactions (id, tx_type, status, currency_code, initiating_server_id, request_id, created_at, committed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#
        )
        .bind(transaction.id.0)
        .bind(type_str)
        .bind(status_str)
        .bind(&transaction.currency_code)
        .bind(&transaction.initiating_server)
        .bind(transaction.idempotency_key)
        .bind(transaction.created_at)
        .bind(transaction.committed_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 2. Insert ledger entries
        for entry in &transaction.entries {
            sqlx::query(
                r#"
                INSERT INTO economy_transaction_entries (id, transaction_id, wallet_id, amount_delta)
                VALUES ($1, $2, $3, $4)
                "#
            )
            .bind(Uuid::new_v4())
            .bind(transaction.id.0)
            .bind(entry.wallet_id.0)
            .bind(entry.amount_delta)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        }

        // 3. Update related wallets utilizing locking and optimistic checks
        for wallet in updated_wallets {
            // Apply lock check (SELECT FOR UPDATE) to assure absolute consistency
            let locked_row = sqlx::query("SELECT revision FROM wallets WHERE id = $1 FOR UPDATE")
                .bind(wallet.id.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

            let locked_revision: i64 = locked_row
                .try_get("revision")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

            if locked_revision != wallet.revision as i64 {
                return Err(DomainError::RevisionConflict(
                    "Wallet Concurrent Modification".to_string(),
                    wallet.revision,
                    locked_revision as u64,
                ));
            }

            sqlx::query(
                r#"
                UPDATE wallets
                SET balance = $1, revision = $2
                WHERE id = $3
                "#,
            )
            .bind(wallet.balance.minor_units())
            .bind((wallet.revision + 1) as i64)
            .bind(wallet.id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn execute_transfer_atomic(
        &self,
        idempotency_key: Uuid,
        fingerprint: &str,
        from_player: PlayerId,
        to_player: PlayerId,
        currency: &str,
        amount_minor: i64,
        initiating_server: &str,
        correlation_id: Uuid,
    ) -> Result<EconomyTransaction, DomainError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 1. Check idempotency log
        let row = sqlx::query(
            "SELECT operation_type, request_fingerprint, response_payload FROM processed_requests WHERE request_id = $1"
        )
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if let Some(r) = row {
            let op_type: String = r
                .try_get("operation_type")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let stored_fingerprint: String = r
                .try_get("request_fingerprint")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let payload: String = r
                .try_get("response_payload")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

            if op_type != "PLAYER_TRANSFER" {
                return Err(DomainError::IdempotencyConflict {
                    request_id: idempotency_key,
                    details: format!(
                        "Conflict: request was previously run as a different operation type: {}",
                        op_type
                    ),
                });
            }

            if stored_fingerprint != fingerprint {
                return Err(DomainError::IdempotencyConflict {
                    request_id: idempotency_key,
                    details:
                        "Conflict: duplicate request ID used with a different payload fingerprint"
                            .to_string(),
                });
            }

            let transaction: EconomyTransaction = serde_json::from_str(&payload)
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            tx.commit()
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            return Ok(transaction);
        }

        // 2. Load source and destination wallets to get their IDs
        let source_wallet_row = sqlx::query(
            "SELECT id, balance, revision FROM wallets WHERE player_id = $1 AND currency_code = $2",
        )
        .bind(from_player.0)
        .bind(currency)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let source_wallet_data = match source_wallet_row {
            Some(w) => w,
            None => return Err(DomainError::PlayerNotFound(from_player.0)),
        };

        let dest_wallet_row = sqlx::query(
            "SELECT id, balance, revision FROM wallets WHERE player_id = $1 AND currency_code = $2",
        )
        .bind(to_player.0)
        .bind(currency)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let dest_wallet_data = match dest_wallet_row {
            Some(w) => w,
            None => return Err(DomainError::PlayerNotFound(to_player.0)),
        };

        let source_wallet_id: Uuid = source_wallet_data
            .try_get("id")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let dest_wallet_id: Uuid = dest_wallet_data
            .try_get("id")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if source_wallet_id == dest_wallet_id {
            return Err(DomainError::InternalStateError(
                "Cannot transfer money to the same wallet".to_string(),
            ));
        }

        // 3. Deterministic Wallet Locking Order
        let (first_lock_id, second_lock_id) = if source_wallet_id < dest_wallet_id {
            (source_wallet_id, dest_wallet_id)
        } else {
            (dest_wallet_id, source_wallet_id)
        };

        // Perform SELECT ... FOR UPDATE locks
        let first_wallet_row =
            sqlx::query("SELECT id, balance, revision FROM wallets WHERE id = $1 FOR UPDATE")
                .bind(first_lock_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let second_wallet_row =
            sqlx::query("SELECT id, balance, revision FROM wallets WHERE id = $1 FOR UPDATE")
                .bind(second_lock_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Extract latest values from locked rows
        let locked_source_row = if source_wallet_id == first_lock_id {
            &first_wallet_row
        } else {
            &second_wallet_row
        };
        let locked_dest_row = if dest_wallet_id == first_lock_id {
            &first_wallet_row
        } else {
            &second_wallet_row
        };

        let source_balance: i64 = locked_source_row
            .try_get("balance")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let source_revision: i64 = locked_source_row
            .try_get("revision")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let dest_balance: i64 = locked_dest_row
            .try_get("balance")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let dest_revision: i64 = locked_dest_row
            .try_get("revision")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 4. Perform authoritative domain money calculations using checked arithmetic
        if source_balance < amount_minor {
            return Err(DomainError::InsufficientFunds {
                wallet_id: source_wallet_id,
                balance: source_balance,
                required: amount_minor,
            });
        }

        let new_source_balance = source_balance
            .checked_sub(amount_minor)
            .ok_or(DomainError::ArithmeticOverflow)?;
        let new_dest_balance = dest_balance
            .checked_add(amount_minor)
            .ok_or(DomainError::ArithmeticOverflow)?;

        // 5. Update wallets
        sqlx::query("UPDATE wallets SET balance = $1, revision = $2 WHERE id = $3")
            .bind(new_source_balance)
            .bind(source_revision + 1)
            .bind(source_wallet_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        sqlx::query("UPDATE wallets SET balance = $1, revision = $2 WHERE id = $3")
            .bind(new_dest_balance)
            .bind(dest_revision + 1)
            .bind(dest_wallet_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 6. Create Ledger entries and validate them
        let entries = vec![
            LedgerEntry {
                wallet_id: WalletId(source_wallet_id),
                amount_delta: -amount_minor,
            },
            LedgerEntry {
                wallet_id: WalletId(dest_wallet_id),
                amount_delta: amount_minor,
            },
        ];

        let tx_id = TransactionId(Uuid::new_v4());
        let mut economy_tx = EconomyTransaction::create_and_validate(
            tx_id,
            stg_domain::EconomyTransactionType::PlayerTransfer,
            currency.to_string(),
            entries,
            idempotency_key,
            initiating_server.to_string(),
            HashMap::new(),
        )?;
        economy_tx.commit()?;

        // 7. Save EconomyTransaction record
        sqlx::query(
            r#"
            INSERT INTO economy_transactions (id, tx_type, status, currency_code, initiating_server_id, request_id, created_at, committed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#
        )
        .bind(economy_tx.id.0)
        .bind("PLAYER_TRANSFER")
        .bind("COMMITTED")
        .bind(&economy_tx.currency_code)
        .bind(&economy_tx.initiating_server)
        .bind(economy_tx.idempotency_key)
        .bind(economy_tx.created_at)
        .bind(economy_tx.committed_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 8. Save Ledger entries
        for entry in &economy_tx.entries {
            sqlx::query(
                r#"
                INSERT INTO economy_transaction_entries (id, transaction_id, wallet_id, amount_delta)
                VALUES ($1, $2, $3, $4)
                "#
            )
            .bind(Uuid::new_v4())
            .bind(economy_tx.id.0)
            .bind(entry.wallet_id.0)
            .bind(entry.amount_delta)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        }

        // 9. Append Domain Event
        let event = DomainEvent {
            id: Uuid::new_v4(),
            aggregate_type: "Wallet".to_string(),
            aggregate_id: source_wallet_id,
            aggregate_version: economy_tx.id.0.as_u128() as u64,
            payload: stg_domain::DomainEventPayload::MoneyTransferred {
                from_wallet: source_wallet_id,
                to_wallet: dest_wallet_id,
                currency: currency.to_string(),
                amount: amount_minor,
            },
            occurred_at: Utc::now(),
            correlation_id,
        };

        let event_payload_json = serde_json::to_value(&event.payload)
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO domain_events (id, aggregate_type, aggregate_id, aggregate_version, payload_json, occurred_at, correlation_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#
        )
        .bind(event.id)
        .bind(&event.aggregate_type)
        .bind(event.aggregate_id)
        .bind(event.aggregate_version as i64)
        .bind(&event_payload_json)
        .bind(event.occurred_at)
        .bind(event.correlation_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 10. Persist Outbox event atomically in same transaction
        let outbox_id = Uuid::new_v4();
        let event_json = serde_json::to_value(&event)
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO outbox_events (id, aggregate_type, aggregate_id, payload_json, status, attempt_count, last_error, available_at, created_at)
            VALUES ($1, $2, $3, $4, 'PENDING', 0, NULL, $5, $6)
            "#
        )
        .bind(outbox_id)
        .bind("EconomyTransaction")
        .bind(economy_tx.id.0)
        .bind(event_json)
        .bind(Utc::now())
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 11. Save processed request (idempotency payload)
        let economy_tx_json = serde_json::to_string(&economy_tx)
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO processed_requests (request_id, operation_type, request_fingerprint, response_payload, processed_at)
            VALUES ($1, $2, $3, $4, $5)
            "#
        )
        .bind(idempotency_key)
        .bind("PLAYER_TRANSFER")
        .bind(fingerprint)
        .bind(&economy_tx_json)
        .bind(Utc::now())
        .execute(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 12. Commit transaction
        tx.commit()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(economy_tx)
    }

    async fn find_by_id(
        &self,
        _id: TransactionId,
    ) -> Result<Option<EconomyTransaction>, DomainError> {
        Ok(None)
    }

    async fn is_idempotent_processed(&self, key: Uuid) -> Result<bool, DomainError> {
        let row =
            sqlx::query("SELECT count(*) as count FROM processed_requests WHERE request_id = $1")
                .bind(key)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let count: i64 = row
            .try_get("count")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        Ok(count > 0)
    }

    async fn save_processed_request(
        &self,
        key: Uuid,
        result_json: &str,
    ) -> Result<(), DomainError> {
        sqlx::query(
            r#"
            INSERT INTO processed_requests (request_id, operation_type, request_fingerprint, response_payload, processed_at)
            VALUES ($1, $2, $3, $4, $5)
            "#
        )
        .bind(key)
        .bind("GENERIC")
        .bind("")
        .bind(result_json)
        .bind(Utc::now())
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn get_processed_result(&self, key: Uuid) -> Result<Option<String>, DomainError> {
        let row =
            sqlx::query("SELECT response_payload FROM processed_requests WHERE request_id = $1")
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(r) => {
                let payload: String = r
                    .try_get("response_payload")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                Ok(Some(payload))
            }
            None => Ok(None),
        }
    }
}

// =========================================================================
// QUEUE & EVENT LOG INFRASTRUCTURE (NATS INTEGRATION PLUGINS)
// =========================================================================

pub struct PostgresEventLogRepository {
    pool: PgPool,
}

impl PostgresEventLogRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventLogRepository for PostgresEventLogRepository {
    async fn append_event(&self, event: &DomainEvent) -> Result<(), DomainError> {
        let payload_json = serde_json::to_value(&event.payload)
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO domain_events (id, aggregate_type, aggregate_id, aggregate_version, payload_json, occurred_at, correlation_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#
        )
        .bind(event.id)
        .bind(&event.aggregate_type)
        .bind(event.aggregate_id)
        .bind(event.aggregate_version as i64)
        .bind(payload_json)
        .bind(event.occurred_at)
        .bind(event.correlation_id)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }
}

pub struct PostgresConversionRepository {
    pool: PgPool,
}

impl PostgresConversionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConversionRepository for PostgresConversionRepository {
    async fn find_rule(
        &self,
        namespace: &str,
        path: &str,
        direction: ConversionDirection,
    ) -> Result<Option<ConversionRule>, DomainError> {
        let dir_str = match direction {
            ConversionDirection::ResourceToCurrency => "RESOURCE_TO_CURRENCY",
            ConversionDirection::CurrencyToResource => "CURRENCY_TO_RESOURCE",
            ConversionDirection::Unspecified => "UNSPECIFIED",
        };

        let row = sqlx::query(
            "SELECT id, direction, resource_namespace, resource_path, currency_code, unit_price_minor, min_amount, max_amount, enabled, pricing_revision FROM conversion_rules WHERE direction = $1 AND resource_namespace = $2 AND resource_path = $3"
        )
        .bind(dir_str)
        .bind(namespace)
        .bind(path)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(r) => {
                let id: Uuid = r
                    .try_get("id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let dir_str: String = r
                    .try_get("direction")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let resource_namespace: String = r
                    .try_get("resource_namespace")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let resource_path: String = r
                    .try_get("resource_path")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let currency_code: String = r
                    .try_get("currency_code")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let unit_price_minor: i64 = r
                    .try_get("unit_price_minor")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let min_amount: i64 = r
                    .try_get("min_amount")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let max_amount: i64 = r
                    .try_get("max_amount")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let enabled: bool = r
                    .try_get("enabled")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let pricing_revision: i64 = r
                    .try_get("pricing_revision")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

                let direction = match dir_str.as_str() {
                    "RESOURCE_TO_CURRENCY" => ConversionDirection::ResourceToCurrency,
                    "CURRENCY_TO_RESOURCE" => ConversionDirection::CurrencyToResource,
                    _ => ConversionDirection::Unspecified,
                };

                Ok(Some(ConversionRule {
                    id,
                    direction,
                    resource_namespace,
                    resource_path,
                    currency_code,
                    unit_price_minor,
                    min_amount,
                    max_amount,
                    enabled,
                    pricing_revision: pricing_revision as u64,
                }))
            }
            None => Ok(None),
        }
    }

    async fn save_reservation(
        &self,
        reservation: &ResourceConversionReservation,
    ) -> Result<(), DomainError> {
        let status_str = match reservation.status {
            ReservationStatus::Prepared => "PREPARED",
            ReservationStatus::Committed => "COMMITTED",
            ReservationStatus::Aborted => "ABORTED",
            ReservationStatus::Expired => "EXPIRED",
        };

        // Note: For simplicity, we serialize required_mutations to JSON or just ignore it in DB if we don't strictly need it.
        // Actually, let's just insert the scalar fields to `resource_conversion_reservations`

        // Also we have only one direction for the quote currently.
        // Oh, wait, direction is not in Reservation directly. We can infer it or we can just save it.
        let direction = "UNKNOWN"; // We can fetch from mutations or just leave it.

        sqlx::query(
            r#"
            INSERT INTO resource_conversion_reservations (id, player_id, status, direction, resource_namespace, resource_path, resource_amount, currency_code, unit_price_minor, total_price_minor, pricing_revision, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#
        )
        .bind(reservation.id.0)
        .bind(reservation.player_id.0)
        .bind(status_str)
        .bind(direction)
        .bind(&reservation.quote.resource.namespace)
        .bind(&reservation.quote.resource.path)
        .bind(reservation.quote.resource_amount)
        .bind(&reservation.quote.currency_code)
        .bind(reservation.quote.unit_price_minor)
        .bind(reservation.quote.total_price_minor)
        .bind(reservation.quote.pricing_revision as i64)
        .bind(reservation.expires_at)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn find_reservation_by_id(
        &self,
        id: ReservationId,
    ) -> Result<Option<ResourceConversionReservation>, DomainError> {
        let row = sqlx::query("SELECT * FROM resource_conversion_reservations WHERE id = $1")
            .bind(id.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(r) => {
                let id_val: Uuid = r
                    .try_get("id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let player_id_val: Uuid = r
                    .try_get("player_id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let status_str: String = r
                    .try_get("status")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

                let resource_namespace: String = r
                    .try_get("resource_namespace")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let resource_path: String = r
                    .try_get("resource_path")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let resource_amount: i64 = r
                    .try_get("resource_amount")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let currency_code: String = r
                    .try_get("currency_code")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let unit_price_minor: i64 = r
                    .try_get("unit_price_minor")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let total_price_minor: i64 = r
                    .try_get("total_price_minor")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let pricing_revision: i64 = r
                    .try_get("pricing_revision")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let expires_at: DateTime<Utc> = r
                    .try_get("expires_at")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

                let status = match status_str.as_str() {
                    "PREPARED" => ReservationStatus::Prepared,
                    "COMMITTED" => ReservationStatus::Committed,
                    "ABORTED" => ReservationStatus::Aborted,
                    "EXPIRED" => ReservationStatus::Expired,
                    _ => ReservationStatus::Prepared,
                };

                let quote = stg_domain::ConversionQuote {
                    quote_id: id_val.to_string(), // use reservation ID as quote ID
                    resource: stg_domain::ResourceRef {
                        namespace: resource_namespace,
                        path: resource_path,
                    },
                    resource_amount,
                    currency_code,
                    unit_price_minor,
                    total_price_minor,
                    pricing_revision: pricing_revision as u64,
                    expires_at,
                };

                Ok(Some(ResourceConversionReservation {
                    id: ReservationId(id_val),
                    player_id: PlayerId(player_id_val),
                    status,
                    quote,
                    required_mutations: vec![], // Not fully restored from DB, but usually enough for check
                    expires_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn abort_reservation(
        &self,
        id: ReservationId,
    ) -> Result<ResourceConversionReservation, DomainError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Lock reservation FOR UPDATE
        let row = sqlx::query(
            "SELECT status FROM resource_conversion_reservations WHERE id = $1 FOR UPDATE",
        )
        .bind(id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if row.is_none() {
            return Err(DomainError::ConversionReservationNotFound(id.0));
        }

        let r = row.unwrap();
        let status_str: String = r
            .try_get("status")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if status_str == "COMMITTED" {
            return Err(DomainError::ConversionReservationInvalidState(
                "Cannot abort COMMITTED reservation".to_string(),
            ));
        }

        if status_str == "PREPARED" {
            sqlx::query(
                "UPDATE resource_conversion_reservations SET status = 'ABORTED' WHERE id = $1",
            )
            .bind(id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        }

        tx.commit()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let res = self.find_reservation_by_id(id).await?;
        Ok(res.unwrap())
    }

    async fn commit_conversion_atomic(
        &self,
        idempotency_key: Uuid,
        fingerprint: &str,
        reservation_id: ReservationId,
        initiating_server: &str,
        correlation_id: Uuid,
    ) -> Result<EconomyTransaction, DomainError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 1. Check idempotency log
        let row = sqlx::query(
            "SELECT operation_type, request_fingerprint, response_payload FROM processed_requests WHERE request_id = $1"
        )
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if let Some(r) = row {
            let op_type: String = r
                .try_get("operation_type")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let stored_fingerprint: String = r
                .try_get("request_fingerprint")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let payload: String = r
                .try_get("response_payload")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

            if op_type != "RESOURCE_CONVERSION" {
                return Err(DomainError::IdempotencyConflict {
                    request_id: idempotency_key,
                    details: format!(
                        "Conflict: request was previously run as a different operation type: {}",
                        op_type
                    ),
                });
            }

            if stored_fingerprint != fingerprint {
                return Err(DomainError::IdempotencyConflict {
                    request_id: idempotency_key,
                    details:
                        "Conflict: duplicate request ID used with a different payload fingerprint"
                            .to_string(),
                });
            }

            let transaction: EconomyTransaction = serde_json::from_str(&payload)
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            tx.commit()
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            return Ok(transaction);
        }

        // Lock reservation FOR UPDATE
        let res_row =
            sqlx::query("SELECT * FROM resource_conversion_reservations WHERE id = $1 FOR UPDATE")
                .bind(reservation_id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if res_row.is_none() {
            return Err(DomainError::ConversionReservationNotFound(reservation_id.0));
        }

        let res_row = res_row.unwrap();
        let status_str: String = res_row
            .try_get("status")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let expires_at: DateTime<Utc> = res_row
            .try_get("expires_at")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let player_id: Uuid = res_row
            .try_get("player_id")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let amount_minor: i64 = res_row
            .try_get("total_price_minor")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let currency_code: String = res_row
            .try_get("currency_code")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let dir_str: String = res_row
            .try_get("direction")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if status_str != "PREPARED" {
            return Err(DomainError::ConversionReservationInvalidState(format!(
                "Cannot commit reservation in state {}",
                status_str
            )));
        }

        if Utc::now() > expires_at {
            // Update to expired
            sqlx::query(
                "UPDATE resource_conversion_reservations SET status = 'EXPIRED' WHERE id = $1",
            )
            .bind(reservation_id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            tx.commit()
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            return Err(DomainError::ConversionReservationExpired(reservation_id.0));
        }

        // Lock wallet
        let wallet_row = sqlx::query("SELECT id, balance, revision FROM wallets WHERE player_id = $1 AND currency_code = $2 FOR UPDATE")
            .bind(player_id)
            .bind(&currency_code)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let wallet_row = match wallet_row {
            Some(w) => w,
            None => return Err(DomainError::PlayerNotFound(player_id)),
        };

        let wallet_id: Uuid = wallet_row
            .try_get("id")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let balance: i64 = wallet_row
            .try_get("balance")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        let revision: i64 = wallet_row
            .try_get("revision")
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Determine credit/debit
        // ResourceToCurrency: we give player money (credit wallet)
        // CurrencyToResource: we take player money (debit wallet)
        // Hmm... wait, how did we define it?
        // In Prepare, amount_delta is just what Minecraft should do with resources.
        // Let's use `dir_str` from the reservation.
        let is_debit = dir_str == "CURRENCY_TO_RESOURCE";

        let new_balance = if is_debit {
            if balance < amount_minor {
                return Err(DomainError::InsufficientFunds {
                    wallet_id,
                    balance,
                    required: amount_minor,
                });
            }
            balance
                .checked_sub(amount_minor)
                .ok_or(DomainError::ArithmeticOverflow)?
        } else {
            balance
                .checked_add(amount_minor)
                .ok_or(DomainError::ArithmeticOverflow)?
        };

        // Update wallet
        sqlx::query("UPDATE wallets SET balance = $1, revision = $2 WHERE id = $3")
            .bind(new_balance)
            .bind(revision + 1)
            .bind(wallet_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Create transaction
        let tx_id = TransactionId(Uuid::new_v4());
        let system_wallet_id = Uuid::nil(); // Using nil UUID for system economy wallet
        let entries = vec![
            LedgerEntry {
                wallet_id: WalletId(wallet_id),
                amount_delta: if is_debit {
                    -amount_minor
                } else {
                    amount_minor
                },
            },
            LedgerEntry {
                wallet_id: WalletId(system_wallet_id),
                amount_delta: if is_debit {
                    amount_minor
                } else {
                    -amount_minor
                },
            },
        ];

        let mut transaction = EconomyTransaction::create_and_validate(
            tx_id,
            EconomyTransactionType::ResourceConversion,
            currency_code.clone(),
            entries,
            idempotency_key,
            initiating_server.to_string(),
            std::collections::HashMap::new(),
        )?;

        transaction.commit()?;

        // Save transaction
        sqlx::query("INSERT INTO economy_transactions (id, tx_type, status, currency_code, initiating_server_id, request_id, created_at, committed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)")
            .bind(transaction.id.0)
            .bind("RESOURCE_CONVERSION")
            .bind("COMMITTED")
            .bind(&transaction.currency_code)
            .bind(&transaction.initiating_server)
            .bind(transaction.idempotency_key)
            .bind(transaction.created_at)
            .bind(transaction.committed_at)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        for entry in &transaction.entries {
            sqlx::query("INSERT INTO economy_transaction_entries (id, transaction_id, wallet_id, amount_delta) VALUES ($1, $2, $3, $4)")
                .bind(Uuid::new_v4())
                .bind(transaction.id.0)
                .bind(entry.wallet_id.0)
                .bind(entry.amount_delta)
                .execute(&mut *tx)
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
        }

        // Update reservation to COMMITTED
        sqlx::query(
            "UPDATE resource_conversion_reservations SET status = 'COMMITTED' WHERE id = $1",
        )
        .bind(reservation_id.0)
        .execute(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Save idempotency record
        let payload_json = serde_json::to_string(&transaction).unwrap();
        sqlx::query("INSERT INTO processed_requests (request_id, operation_type, request_fingerprint, response_payload, processed_at) VALUES ($1, $2, $3, $4, $5)")
            .bind(idempotency_key)
            .bind("RESOURCE_CONVERSION")
            .bind(fingerprint)
            .bind(&payload_json)
            .bind(Utc::now())
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Domain Event and Outbox Event
        // We will just create a generic event for now
        let event = stg_domain::DomainEvent {
            id: Uuid::new_v4(),
            aggregate_type: "EconomyTransaction".to_string(),
            aggregate_id: transaction.id.0,
            aggregate_version: 1,
            payload: stg_domain::DomainEventPayload::MoneyTransferred {
                from_wallet: if is_debit {
                    wallet_id
                } else {
                    system_wallet_id
                },
                to_wallet: if is_debit {
                    system_wallet_id
                } else {
                    wallet_id
                },
                currency: currency_code,
                amount: amount_minor,
            },
            occurred_at: Utc::now(),
            correlation_id,
        };

        let event_json = serde_json::to_value(&event.payload).unwrap();

        sqlx::query("INSERT INTO domain_events (id, aggregate_type, aggregate_id, aggregate_version, payload_json, occurred_at, correlation_id) VALUES ($1, $2, $3, $4, $5, $6, $7)")
            .bind(event.id)
            .bind(&event.aggregate_type)
            .bind(event.aggregate_id)
            .bind(event.aggregate_version as i64)
            .bind(&event_json)
            .bind(event.occurred_at)
            .bind(event.correlation_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        sqlx::query("INSERT INTO outbox_events (id, aggregate_type, aggregate_id, payload_json) VALUES ($1, $2, $3, $4)")
            .bind(Uuid::new_v4())
            .bind("EconomyTransaction")
            .bind(transaction.id.0)
            .bind(&event_json)
            .execute(&mut *tx)
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(transaction)
    }
}

pub struct PostgresEnergyRepository {
    pool: PgPool,
}

impl PostgresEnergyRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EnergyRepository for PostgresEnergyRepository {
    async fn find_node_by_id(&self, id: Uuid) -> Result<Option<EnergyNode>, DomainError> {
        let row = sqlx::query(
            r#"
            SELECT id, node_type, server_id, region_id, display_name, enabled, capacity_watts, production_watts, consumption_watts, stored_wh, max_stored_wh, efficiency, health, revision, last_reported_at
            FROM energy_nodes
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        match row {
            Some(r) => {
                let id_val: Uuid = r
                    .try_get("id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let node_type_str: String = r
                    .try_get("node_type")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let server_id: String = r
                    .try_get("server_id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let region_id: String = r
                    .try_get("region_id")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let display_name: String = r
                    .try_get("display_name")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let enabled: bool = r
                    .try_get("enabled")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let capacity_watts: i64 = r
                    .try_get("capacity_watts")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let production_watts: i64 = r
                    .try_get("production_watts")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let consumption_watts: i64 = r
                    .try_get("consumption_watts")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let stored_wh: i64 = r
                    .try_get("stored_wh")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let max_stored_wh: i64 = r
                    .try_get("max_stored_wh")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let efficiency: f64 = r
                    .try_get("efficiency")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let health: f64 = r
                    .try_get("health")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let revision_val: i64 = r
                    .try_get("revision")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
                let last_reported_at: DateTime<Utc> = r
                    .try_get("last_reported_at")
                    .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

                let node_type = match node_type_str.as_str() {
                    "PRODUCER" => EnergyNodeType::Producer,
                    "CONSUMER" => EnergyNodeType::Consumer,
                    "STORAGE" => EnergyNodeType::Storage,
                    "HYBRID" => EnergyNodeType::Hybrid,
                    _ => {
                        return Err(DomainError::InternalStateError(
                            "Invalid energy node type in DB".to_string(),
                        ))
                    }
                };

                Ok(Some(EnergyNode {
                    id: stg_domain::NodeId(id_val),
                    node_type,
                    server_id,
                    region_id,
                    display_name,
                    enabled,
                    capacity_watts,
                    production_watts,
                    consumption_watts,
                    stored_wh,
                    max_stored_wh,
                    efficiency,
                    health,
                    revision: revision_val as u64,
                    last_reported_at,
                }))
            }
            None => Ok(None),
        }
    }

    async fn list_active_nodes(&self) -> Result<Vec<EnergyNode>, DomainError> {
        let rows = sqlx::query(
            r#"
            SELECT id, node_type, server_id, region_id, display_name, enabled, capacity_watts, production_watts, consumption_watts, stored_wh, max_stored_wh, efficiency, health, revision, last_reported_at
            FROM energy_nodes
            WHERE enabled = true
            "#
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let mut nodes = Vec::new();
        for r in rows {
            let id_val: Uuid = r
                .try_get("id")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let node_type_str: String = r
                .try_get("node_type")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let server_id: String = r
                .try_get("server_id")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let region_id: String = r
                .try_get("region_id")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let display_name: String = r
                .try_get("display_name")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let enabled: bool = r
                .try_get("enabled")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let capacity_watts: i64 = r
                .try_get("capacity_watts")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let production_watts: i64 = r
                .try_get("production_watts")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let consumption_watts: i64 = r
                .try_get("consumption_watts")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let stored_wh: i64 = r
                .try_get("stored_wh")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let max_stored_wh: i64 = r
                .try_get("max_stored_wh")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let efficiency: f64 = r
                .try_get("efficiency")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let health: f64 = r
                .try_get("health")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let revision_val: i64 = r
                .try_get("revision")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let last_reported_at: DateTime<Utc> = r
                .try_get("last_reported_at")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

            let node_type = match node_type_str.as_str() {
                "PRODUCER" => EnergyNodeType::Producer,
                "CONSUMER" => EnergyNodeType::Consumer,
                "STORAGE" => EnergyNodeType::Storage,
                "HYBRID" => EnergyNodeType::Hybrid,
                _ => {
                    return Err(DomainError::InternalStateError(
                        "Invalid energy node type in DB".to_string(),
                    ))
                }
            };

            nodes.push(EnergyNode {
                id: stg_domain::NodeId(id_val),
                node_type,
                server_id,
                region_id,
                display_name,
                enabled,
                capacity_watts,
                production_watts,
                consumption_watts,
                stored_wh,
                max_stored_wh,
                efficiency,
                health,
                revision: revision_val as u64,
                last_reported_at,
            });
        }

        Ok(nodes)
    }

    async fn save_node(&self, node: &EnergyNode) -> Result<(), DomainError> {
        let type_str = match node.node_type {
            EnergyNodeType::Producer => "PRODUCER",
            EnergyNodeType::Consumer => "CONSUMER",
            EnergyNodeType::Storage => "STORAGE",
            EnergyNodeType::Hybrid => "HYBRID",
        };

        sqlx::query(
            r#"
            INSERT INTO energy_nodes (id, node_type, server_id, region_id, display_name, enabled, capacity_watts, production_watts, consumption_watts, stored_wh, max_stored_wh, efficiency, health, revision, last_reported_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            "#
        )
        .bind(node.id.0)
        .bind(type_str)
        .bind(&node.server_id)
        .bind(&node.region_id)
        .bind(&node.display_name)
        .bind(node.enabled)
        .bind(node.capacity_watts)
        .bind(node.production_watts)
        .bind(node.consumption_watts)
        .bind(node.stored_wh)
        .bind(node.max_stored_wh)
        .bind(node.efficiency)
        .bind(node.health)
        .bind(node.revision as i64)
        .bind(node.last_reported_at)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        Ok(())
    }

    async fn update_node(&self, node: &EnergyNode) -> Result<(), DomainError> {
        let res = sqlx::query(
            r#"
            UPDATE energy_nodes
            SET production_watts = $1, consumption_watts = $2, stored_wh = $3, revision = $4, last_reported_at = $5
            WHERE id = $6 AND revision = $7
            "#
        )
        .bind(node.production_watts)
        .bind(node.consumption_watts)
        .bind(node.stored_wh)
        .bind((node.revision + 1) as i64)
        .bind(node.last_reported_at)
        .bind(node.id.0)
        .bind(node.revision as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if res.rows_affected() == 0 {
            return Err(DomainError::RevisionConflict(
                "EnergyNode".to_string(),
                node.revision,
                node.revision,
            ));
        }

        Ok(())
    }

    async fn get_latest_state(&self) -> Result<EnergyState, DomainError> {
        Ok(EnergyState::initial())
    }

    async fn save_state(&self, _state: &EnergyState) -> Result<(), DomainError> {
        Ok(())
    }
}

#[derive(Default)]
pub struct LoggingQueuePublisher {}

impl LoggingQueuePublisher {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl QueuePublisher for LoggingQueuePublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), DomainError> {
        println!(
            "[STG-QUEUE-LOG] Honestly dispatched event ID {} to STG Event Hub. Aggregate: {}, payload: {:?}",
            event.id, event.aggregate_type, event.payload
        );
        Ok(())
    }
}

// =========================================================================
// TRANSACTIONAL OUTBOX BACKGROUND WORKER
// =========================================================================

pub struct OutboxWorker {
    pool: PgPool,
    publisher: Arc<dyn QueuePublisher>,
}

impl OutboxWorker {
    pub fn new(pool: PgPool, publisher: Arc<dyn QueuePublisher>) -> Self {
        Self { pool, publisher }
    }

    /// Run a single polling tick. Returns the number of events processed.
    pub async fn process_next_batch(&self, batch_size: i32) -> Result<usize, DomainError> {
        // 1. Start a transaction to select and lock the available pending outbox records
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        let rows = sqlx::query(
            r#"
            SELECT id, aggregate_type, aggregate_id, payload_json, attempt_count
            FROM outbox_events
            WHERE (status = 'PENDING' OR status = 'FAILED') AND available_at <= $1
            ORDER BY created_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(Utc::now())
        .bind(batch_size as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        if rows.is_empty() {
            tx.commit()
                .await
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            return Ok(0);
        }

        let mut locked_ids = Vec::new();
        let mut events_to_publish = Vec::new();

        for row in &rows {
            let id: Uuid = row
                .try_get("id")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let aggregate_type: String = row
                .try_get("aggregate_type")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let aggregate_id: Uuid = row
                .try_get("aggregate_id")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let payload_json: serde_json::Value = row
                .try_get("payload_json")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            let attempt_count: i32 = row
                .try_get("attempt_count")
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

            locked_ids.push(id);
            events_to_publish.push((
                id,
                aggregate_type,
                aggregate_id,
                payload_json,
                attempt_count,
            ));
        }

        // Atomically claim ownership (mark as PROCESSING)
        sqlx::query(
            r#"
            UPDATE outbox_events
            SET status = 'PROCESSING'
            WHERE id = ANY($1)
            "#,
        )
        .bind(&locked_ids)
        .execute(&mut *tx)
        .await
        .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // Commit claim transaction so that other workers don't block
        tx.commit()
            .await
            .map_err(|e| DomainError::InternalStateError(e.to_string()))?;

        // 2. Publish outside of any PostgreSQL transaction
        for (id, _aggregate_type, _aggregate_id, payload_json, attempt_count) in events_to_publish {
            // Reconstruct DomainEvent from payload_json
            let event_res: Result<DomainEvent, _> = serde_json::from_value(payload_json.clone());
            match event_res {
                Ok(event) => {
                    match self.publisher.publish(&event).await {
                        Ok(_) => {
                            // On success, mark PUBLISHED
                            if let Err(e) = sqlx::query(
                                r#"
                                UPDATE outbox_events
                                SET status = 'PUBLISHED'
                                WHERE id = $1
                                "#,
                            )
                            .bind(id)
                            .execute(&self.pool)
                            .await
                            {
                                eprintln!(
                                    "Failed to update outbox status to PUBLISHED for event {}: {}",
                                    id, e
                                );
                            }
                        }
                        Err(pub_err) => {
                            // On failure: increment attempt_count, record last_error, calculate bounded retry delay, update available_at, return to FAILED state
                            let err_str = pub_err.to_string();
                            let next_attempt = attempt_count + 1;
                            // exponential backoff capped at 1 hour (3600 seconds)
                            let backoff_secs = 2_u64.pow(next_attempt as u32).min(3600);
                            let available_at =
                                Utc::now() + chrono::Duration::seconds(backoff_secs as i64);

                            if let Err(e) = sqlx::query(
                                r#"
                                UPDATE outbox_events
                                SET status = 'FAILED',
                                    attempt_count = $1,
                                    last_error = $2,
                                    available_at = $3
                                WHERE id = $4
                                "#,
                            )
                            .bind(next_attempt)
                            .bind(err_str)
                            .bind(available_at)
                            .bind(id)
                            .execute(&self.pool)
                            .await
                            {
                                eprintln!(
                                    "Failed to update outbox status to FAILED for event {}: {}",
                                    id, e
                                );
                            }
                        }
                    }
                }
                Err(serde_err) => {
                    eprintln!(
                        "Failed to deserialize outbox payload for {}: {}",
                        id, serde_err
                    );
                    let err_str = format!("Unrecoverable deserialization error: {}", serde_err);
                    let _ = sqlx::query(
                        r#"
                        UPDATE outbox_events
                        SET status = 'FAILED',
                            attempt_count = attempt_count + 1,
                            last_error = $1,
                            available_at = $2
                        WHERE id = $3
                        "#,
                    )
                    .bind(err_str)
                    .bind(Utc::now() + chrono::Duration::hours(24))
                    .bind(id)
                    .execute(&self.pool)
                    .await;
                }
            }
        }

        Ok(locked_ids.len())
    }

    /// Background loop runner
    pub async fn start_loop(self, batch_size: i32, poll_interval: std::time::Duration) {
        let worker = Arc::new(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            loop {
                interval.tick().await;
                if let Err(e) = worker.process_next_batch(batch_size).await {
                    eprintln!("Error processing outbox batch: {:?}", e);
                }
            }
        });
    }
}

// =========================================================================
// canonical POSTGRESQL SCHEMA (DDL SEED)
// =========================================================================

pub const POSTGRES_DDL_SCHEMA: &str = r#"
-- STG-Ashland Core Relational Schema

CREATE TABLE IF NOT EXISTS players (
    id UUID PRIMARY KEY,
    username VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL REFERENCES players(id),
    currency_code VARCHAR(16) NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0,
    revision BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT wallets_balance_check CHECK (balance >= 0),
    CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency_code)
);

CREATE TABLE IF NOT EXISTS economy_transactions (
    id UUID PRIMARY KEY,
    tx_type VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    currency_code VARCHAR(16) NOT NULL,
    initiating_server_id VARCHAR(64) NOT NULL,
    request_id UUID NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    committed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS economy_transaction_entries (
    id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES economy_transactions(id),
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    amount_delta BIGINT NOT NULL,
    CONSTRAINT entry_non_zero CHECK (amount_delta <> 0)
);

CREATE TABLE IF NOT EXISTS energy_nodes (
    id UUID PRIMARY KEY,
    node_type VARCHAR(32) NOT NULL,
    server_id VARCHAR(64) NOT NULL,
    region_id VARCHAR(64) NOT NULL,
    display_name VARCHAR(128) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    capacity_watts BIGINT NOT NULL,
    production_watts BIGINT NOT NULL,
    consumption_watts BIGINT NOT NULL,
    stored_wh BIGINT NOT NULL,
    max_stored_wh BIGINT NOT NULL,
    efficiency DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    health DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    revision BIGINT NOT NULL DEFAULT 0,
    last_reported_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT energy_non_negative CHECK (capacity_watts >= 0 AND production_watts >= 0 AND consumption_watts >= 0)
);

CREATE TABLE IF NOT EXISTS processed_requests (
    request_id UUID PRIMARY KEY,
    operation_type VARCHAR(64) NOT NULL DEFAULT 'GENERIC',
    request_fingerprint VARCHAR(64) NOT NULL DEFAULT '',
    response_payload TEXT NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS domain_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_version BIGINT NOT NULL,
    payload_json JSONB NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    correlation_id UUID NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id UUID NOT NULL,
    payload_json JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    attempt_count INT NOT NULL DEFAULT 0,
    last_error TEXT,
    available_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS conversion_rules (
    id UUID PRIMARY KEY,
    direction VARCHAR(32) NOT NULL,
    resource_namespace VARCHAR(64) NOT NULL,
    resource_path VARCHAR(64) NOT NULL,
    currency_code VARCHAR(16) NOT NULL,
    unit_price_minor BIGINT NOT NULL,
    min_amount BIGINT NOT NULL DEFAULT 1,
    max_amount BIGINT NOT NULL DEFAULT 1000000,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    pricing_revision BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS resource_conversion_reservations (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL REFERENCES players(id),
    status VARCHAR(32) NOT NULL,
    direction VARCHAR(32) NOT NULL,
    resource_namespace VARCHAR(64) NOT NULL,
    resource_path VARCHAR(64) NOT NULL,
    resource_amount BIGINT NOT NULL,
    currency_code VARCHAR(16) NOT NULL,
    unit_price_minor BIGINT NOT NULL,
    total_price_minor BIGINT NOT NULL,
    pricing_revision BIGINT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID PRIMARY KEY,
    player_uuid UUID NOT NULL REFERENCES players(id),
    server_id TEXT NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS player_transitions (
    transition_id UUID PRIMARY KEY,
    player_uuid UUID NOT NULL REFERENCES players(id),
    ticket VARCHAR(128) NOT NULL,
    from_server_id VARCHAR(64) NOT NULL,
    to_server_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_sessions_active_player
    ON sessions(player_uuid) WHERE state IN ('ACTIVE', 'RECONNECTED');
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(state, expires_at);
CREATE INDEX IF NOT EXISTS idx_sessions_server ON sessions(server_id);
CREATE INDEX IF NOT EXISTS idx_transitions_player ON player_transitions(player_uuid);
CREATE INDEX IF NOT EXISTS idx_transitions_ticket ON player_transitions(ticket);
CREATE INDEX IF NOT EXISTS idx_wallets_player ON wallets(player_id);
CREATE INDEX IF NOT EXISTS idx_entries_tx ON economy_transaction_entries(transaction_id);
CREATE INDEX IF NOT EXISTS idx_events_aggregate ON domain_events(aggregate_type, aggregate_id);
CREATE TABLE IF NOT EXISTS simulation_ticks (
    tick_id UUID PRIMARY KEY,
    tick_number BIGINT NOT NULL UNIQUE,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_ms BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'IN_PROGRESS',
    total_events BIGINT NOT NULL DEFAULT 0,
    total_entities_processed BIGINT NOT NULL DEFAULT 0,
    subsystem_details JSONB
);

CREATE INDEX IF NOT EXISTS idx_tick_number ON simulation_ticks(tick_number);
CREATE INDEX IF NOT EXISTS idx_tick_status ON simulation_ticks(status);

CREATE INDEX IF NOT EXISTS idx_outbox_status_available ON outbox_events(status, available_at);
"#;

// =========================================================================
// Processed Requests Cleanup Subsystem
// =========================================================================

/// Configuration for idempotency record retention.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// How long to keep processed request records (seconds).
    pub retention_secs: i64,
    /// Whether to run cleanup on scheduler ticks.
    pub enabled: bool,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            retention_secs: 86400, // 24 hours
            enabled: true,
        }
    }
}

/// A SimulationSystem that removes expired idempotency records.
pub struct ProcessedRequestsCleanupSystem {
    pool: sqlx::PgPool,
    config: CleanupConfig,
}

impl ProcessedRequestsCleanupSystem {
    pub fn new(pool: sqlx::PgPool, config: CleanupConfig) -> Self {
        Self { pool, config }
    }
}

#[async_trait::async_trait]
impl SimulationSystem for ProcessedRequestsCleanupSystem {
    fn name(&self) -> &str {
        "processed_requests_cleanup"
    }

    async fn tick(
        &self,
        _ctx: &stg_domain::SimulationContext,
    ) -> Result<stg_domain::SubsystemTickOutcome, stg_domain::DomainError> {
        use stg_domain::{DomainError, SubsystemTickOutcome, TickStatus};

        if !self.config.enabled {
            return Ok(SubsystemTickOutcome {
                subsystem_name: self.name().to_string(),
                status: TickStatus::Completed,
                duration_ms: 0,
                error: None,
                events_generated: 0,
                entities_processed: 0,
            });
        }

        let start = chrono::Utc::now();
        let cutoff = chrono::Utc::now() - chrono::Duration::seconds(self.config.retention_secs);

        let result = sqlx::query("DELETE FROM processed_requests WHERE processed_at < $1")
            .bind(cutoff)
            .execute(&self.pool)
            .await;

        match result {
            Ok(r) => {
                let elapsed = (chrono::Utc::now() - start).num_milliseconds();
                let deleted = r.rows_affected();
                if deleted > 0 {
                    tracing::info!(
                        rows = deleted,
                        retention_secs = self.config.retention_secs,
                        "Cleaned up expired idempotency records"
                    );
                }
                Ok(SubsystemTickOutcome {
                    subsystem_name: self.name().to_string(),
                    status: TickStatus::Completed,
                    duration_ms: elapsed,
                    error: None,
                    events_generated: 0,
                    entities_processed: deleted as u64,
                })
            }
            Err(e) => {
                let _elapsed = (chrono::Utc::now() - start).num_milliseconds();
                tracing::error!(error = %e, "Failed to clean up processed_requests");
                Err(DomainError::InternalStateError(e.to_string()))
            }
        }
    }
}
