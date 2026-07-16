use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

// =========================================================================
// DOMAIN ERRORS
// =========================================================================

#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum DomainError {
    #[error("Internal state error: {0}")]
    InternalStateError(String),

    #[error("Player not found: {0}")]
    PlayerNotFound(Uuid),

    #[error("Invalid amount: {0}")]
    InvalidAmount(i64),

    #[error("Revision conflict in {0}: expected {1}, actual {2}")]
    RevisionConflict(String, u64, u64),

    #[error("Energy node not found: {0}")]
    EnergyNodeNotFound(Uuid),

    #[error("Insufficient funds in wallet {wallet_id}: current balance {balance}, required {required}")]
    InsufficientFunds {
        wallet_id: Uuid,
        balance: i64,
        required: i64,
    },

    #[error("Ledger imbalance: entries must sum to zero")]
    LedgerImbalance,

    #[error("Arithmetic overflow during money calculation")]
    ArithmeticOverflow,

    #[error("Idempotency conflict for request {request_id}: {details}")]
    IdempotencyConflict {
        request_id: Uuid,
        details: String,
    },

    #[error("Conversion reservation not found: {0}")]
    ConversionReservationNotFound(Uuid),

    #[error("Conversion reservation in invalid state: {0}")]
    ConversionReservationInvalidState(String),

    #[error("Conversion reservation expired: {0}")]
    ConversionReservationExpired(Uuid),

    #[error("Conversion rule not found")]
    ConversionRuleNotFound,

    #[error("Server identity mismatch: expected {expected}, actual {actual}")]
    ServerIdentityMismatch {
        expected: String,
        actual: String,
    },
}

// =========================================================================
// PLAYER DOMAIN ENTITIES
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlayerId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlayerStatus {
    Active,
    Locked,
    Transitioning,
    Banned,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Player {
    pub id: PlayerId,
    pub username: String,
    pub status: PlayerStatus,
    pub created_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub revision: u64,
}

// =========================================================================
// ECONOMY / WALLET DOMAIN ENTITIES
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WalletId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money(pub i64);

impl Money {
    pub fn zero() -> Self {
        Self(0)
    }

    pub fn new(val: i64) -> Result<Self, DomainError> {
        if val < 0 {
            Err(DomainError::InvalidAmount(val))
        } else {
            Ok(Self(val))
        }
    }

    pub fn minor_units(&self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Wallet {
    pub id: WalletId,
    pub player_id: PlayerId,
    pub currency_code: String,
    pub balance: Money,
    pub revision: u64,
}

impl Wallet {
    pub fn withdraw(&mut self, amount: Money) -> Result<(), DomainError> {
        if self.balance.0 < amount.0 {
            return Err(DomainError::InsufficientFunds {
                wallet_id: self.id.0,
                balance: self.balance.0,
                required: amount.0,
            });
        }
        self.balance.0 = self.balance.0.checked_sub(amount.0)
            .ok_or(DomainError::ArithmeticOverflow)?;
        Ok(())
    }

    pub fn deposit(&mut self, amount: Money) -> Result<(), DomainError> {
        self.balance.0 = self.balance.0.checked_add(amount.0)
            .ok_or(DomainError::ArithmeticOverflow)?;
        Ok(())
    }
}

// =========================================================================
// LEDGER & TRANSACTION ENTITIES
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EconomyTransactionStatus {
    Pending,
    Committed,
    Rejected,
    Reversed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EconomyTransactionType {
    AdminCredit,
    AdminDebit,
    ResourceConversion,
    Purchase,
    Sale,
    PlayerTransfer,
    SystemReward,
    SystemPenalty,
    EnergySettlement,
    Refund,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub wallet_id: WalletId,
    pub amount_delta: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EconomyTransaction {
    pub id: TransactionId,
    pub tx_type: EconomyTransactionType,
    pub status: EconomyTransactionStatus,
    pub currency_code: String,
    pub entries: Vec<LedgerEntry>,
    pub idempotency_key: Uuid,
    pub initiating_server: String,
    pub created_at: DateTime<Utc>,
    pub committed_at: Option<DateTime<Utc>>,
}

impl EconomyTransaction {
    pub fn create_and_validate(
        id: TransactionId,
        tx_type: EconomyTransactionType,
        currency_code: String,
        entries: Vec<LedgerEntry>,
        idempotency_key: Uuid,
        initiating_server: String,
        _metadata: HashMap<String, String>,
    ) -> Result<Self, DomainError> {
        let mut sum: i64 = 0;
        for entry in &entries {
            sum = sum.checked_add(entry.amount_delta)
                .ok_or(DomainError::ArithmeticOverflow)?;
        }
        if sum != 0 {
            return Err(DomainError::LedgerImbalance);
        }

        Ok(Self {
            id,
            tx_type,
            status: EconomyTransactionStatus::Pending,
            currency_code,
            entries,
            idempotency_key,
            initiating_server,
            created_at: Utc::now(),
            committed_at: None,
        })
    }

    pub fn commit(&mut self) -> Result<(), DomainError> {
        self.status = EconomyTransactionStatus::Committed;
        self.committed_at = Some(Utc::now());
        Ok(())
    }
}

// =========================================================================
// ENERGY DOMAIN ENTITIES
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnergyMode {
    Normal,
    Surplus,
    Deficit,
    Critical,
    Collapse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnergyNodeType {
    Producer,
    Consumer,
    Storage,
    Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnergyNode {
    pub id: NodeId,
    pub node_type: EnergyNodeType,
    pub server_id: String,
    pub region_id: String,
    pub display_name: String,
    pub enabled: bool,
    pub capacity_watts: i64,
    pub production_watts: i64,
    pub consumption_watts: i64,
    pub stored_wh: i64,
    pub max_stored_wh: i64,
    pub efficiency: f64,
    pub health: f64,
    pub revision: u64,
    pub last_reported_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnergyState {
    pub mode: EnergyMode,
    pub simulation_tick: u64,
}

impl EnergyState {
    pub fn initial() -> Self {
        Self {
            mode: EnergyMode::Normal,
            simulation_tick: 0,
        }
    }

    pub fn calculate_tick(&self, _nodes: &[EnergyNode]) -> Self {
        Self {
            mode: self.mode,
            simulation_tick: self.simulation_tick + 1,
        }
    }
}

// =========================================================================
// DOMAIN EVENTS
// =========================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DomainEventPayload {
    PlayerRegistered {
        id: Uuid,
        username: String,
    },
    MoneyTransferred {
        from_wallet: Uuid,
        to_wallet: Uuid,
        currency: String,
        amount: i64,
    },
    EnergyModeChanged {
        old_mode: EnergyMode,
        new_mode: EnergyMode,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEvent {
    pub id: Uuid,
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub aggregate_version: u64,
    pub payload: DomainEventPayload,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: Uuid,
}

// =========================================================================
// CONVERSION & TRANSITION PLUGINS (FOR BACKWARDS COMPATIBILITY)
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConversionDirection {
    Unspecified,
    ResourceToCurrency,
    CurrencyToResource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRef {
    pub namespace: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMutation {
    pub resource: ResourceRef,
    pub amount_delta: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionQuote {
    pub quote_id: String,
    pub resource: ResourceRef,
    pub resource_amount: i64,
    pub currency_code: String,
    pub unit_price_minor: i64,
    pub total_price_minor: i64,
    pub pricing_revision: u64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionRule {
    pub id: Uuid,
    pub direction: ConversionDirection,
    pub resource_namespace: String,
    pub resource_path: String,
    pub currency_code: String,
    pub unit_price_minor: i64,
    pub min_amount: i64,
    pub max_amount: i64,
    pub enabled: bool,
    pub pricing_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReservationId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReservationStatus {
    Prepared,
    Committed,
    Aborted,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceConversionReservation {
    pub id: ReservationId,
    pub player_id: PlayerId,
    pub status: ReservationStatus,
    pub quote: ConversionQuote,
    pub required_mutations: Vec<ResourceMutation>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransitionId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransitionStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerTransition {
    pub id: TransitionId,
    pub ticket: String,
    pub status: TransitionStatus,
    pub player_id: PlayerId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wallet_deposit_and_withdraw() {
        let player_id = PlayerId(Uuid::new_v4());
        let mut wallet = Wallet {
            id: WalletId(Uuid::new_v4()),
            player_id,
            currency_code: "USD".to_string(),
            balance: Money(100),
            revision: 0,
        };

        // Standard deposit
        assert!(wallet.deposit(Money(50)).is_ok());
        assert_eq!(wallet.balance.0, 150);

        // Standard withdraw
        assert!(wallet.withdraw(Money(40)).is_ok());
        assert_eq!(wallet.balance.0, 110);

        // Insufficient funds
        let err = wallet.withdraw(Money(120)).unwrap_err();
        match err {
            DomainError::InsufficientFunds { wallet_id, balance, required } => {
                assert_eq!(wallet_id, wallet.id.0);
                assert_eq!(balance, 110);
                assert_eq!(required, 120);
            }
            _ => panic!("Expected InsufficientFunds error"),
        }
    }

    #[test]
    fn test_transaction_balancing() {
        let tx_id = TransactionId(Uuid::new_v4());
        let wallet1 = WalletId(Uuid::new_v4());
        let wallet2 = WalletId(Uuid::new_v4());

        // Balanced transaction
        let entries = vec![
            LedgerEntry {
                wallet_id: wallet1,
                amount_delta: -50,
            },
            LedgerEntry {
                wallet_id: wallet2,
                amount_delta: 50,
            },
        ];
        let tx = EconomyTransaction::create_and_validate(
            tx_id,
            EconomyTransactionType::PlayerTransfer,
            "USD".to_string(),
            entries,
            Uuid::new_v4(),
            "server-1".to_string(),
            std::collections::HashMap::new(),
        );
        assert!(tx.is_ok());

        // Imbalanced transaction
        let imbalanced_entries = vec![
            LedgerEntry {
                wallet_id: wallet1,
                amount_delta: -50,
            },
            LedgerEntry {
                wallet_id: wallet2,
                amount_delta: 40,
            },
        ];
        let tx_err = EconomyTransaction::create_and_validate(
            tx_id,
            EconomyTransactionType::PlayerTransfer,
            "USD".to_string(),
            imbalanced_entries,
            Uuid::new_v4(),
            "server-1".to_string(),
            std::collections::HashMap::new(),
        ).unwrap_err();

        match tx_err {
            DomainError::LedgerImbalance => {}
            _ => panic!("Expected LedgerImbalance error"),
        }
    }
}

