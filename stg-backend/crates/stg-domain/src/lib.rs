use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

pub mod locale;
pub use locale::Locale;

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

    #[error(
        "Insufficient funds in wallet {wallet_id}: current balance {balance}, required {required}"
    )]
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
    IdempotencyConflict { request_id: Uuid, details: String },

    #[error("Conversion reservation not found: {0}")]
    ConversionReservationNotFound(Uuid),

    #[error("Conversion reservation in invalid state: {0}")]
    ConversionReservationInvalidState(String),

    #[error("Conversion reservation expired: {0}")]
    ConversionReservationExpired(Uuid),

    #[error("Conversion rule not found")]
    ConversionRuleNotFound,

    #[error("Server identity mismatch: expected {expected}, actual {actual}")]
    ServerIdentityMismatch { expected: String, actual: String },

    #[error("Player already has an active session: {0}")]
    DuplicateSession(Uuid),

    #[error("Session not found: {0}")]
    SessionNotFound(Uuid),

    #[error("Session already terminated: {0}")]
    SessionTerminated(Uuid),

    #[error("Session expired: {0}")]
    SessionExpired(Uuid),

    #[error("Simulation tick already exists: {0}")]
    DuplicateTick(u64),

    #[error("Simulation tick not found: {0}")]
    TickNotFound(u64),

    #[error("Simulation scheduler already running")]
    SchedulerAlreadyRunning,

    #[error("Subsystem failure: {subsystem}, error: {error}")]
    SubsystemFailure { subsystem: String, error: String },
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
        self.balance.0 = self
            .balance
            .0
            .checked_sub(amount.0)
            .ok_or(DomainError::ArithmeticOverflow)?;
        Ok(())
    }

    pub fn deposit(&mut self, amount: Money) -> Result<(), DomainError> {
        self.balance.0 = self
            .balance
            .0
            .checked_add(amount.0)
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
            sum = sum
                .checked_add(entry.amount_delta)
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

    pub fn calculate_tick(&self, nodes: &[EnergyNode]) -> Self {
        if nodes.is_empty() {
            return Self {
                mode: self.mode,
                simulation_tick: self.simulation_tick + 1,
            };
        }

        let mut total_production: i64 = 0;
        let mut total_consumption: i64 = 0;
        let mut total_storage_capacity: i64 = 0;
        let mut total_stored: i64 = 0;
        let mut efficiency_sum: f64 = 0.0;
        let mut efficiency_count: u64 = 0;

        for node in nodes {
            if !node.enabled {
                continue;
            }
            let health_factor = node.health.clamp(0.0, 1.0);

            match node.node_type {
                EnergyNodeType::Producer => {
                    total_production += (node.production_watts as f64 * health_factor) as i64;
                    efficiency_sum += node.efficiency;
                    efficiency_count += 1;
                }
                EnergyNodeType::Consumer => {
                    total_consumption += (node.consumption_watts as f64 * health_factor) as i64;
                }
                EnergyNodeType::Storage => {
                    total_storage_capacity += node.max_stored_wh;
                    total_stored += node.stored_wh;
                }
                EnergyNodeType::Hybrid => {
                    total_production += (node.production_watts as f64 * health_factor) as i64;
                    total_consumption += (node.consumption_watts as f64 * health_factor) as i64;
                    total_storage_capacity += node.max_stored_wh;
                    total_stored += node.stored_wh;
                    efficiency_sum += node.efficiency;
                    efficiency_count += 1;
                }
            }
        }

        let avg_efficiency = if efficiency_count > 0 {
            (efficiency_sum / efficiency_count as f64).clamp(0.0, 1.0)
        } else {
            1.0
        };

        // Apply efficiency to production
        let effective_production = (total_production as f64 * avg_efficiency) as i64;
        let unmet_demand = (total_consumption - effective_production).max(0);

        // Deterministic mode calculation (no floating-point in branching)
        let new_mode = if unmet_demand == 0 {
            if effective_production > total_consumption && total_storage_capacity > total_stored {
                EnergyMode::Surplus
            } else {
                EnergyMode::Normal
            }
        } else if total_stored >= unmet_demand {
            EnergyMode::Deficit
        } else {
            // Critical: unmet demand exceeds stored reserves
            EnergyMode::Critical
        };

        Self {
            mode: new_mode,
            simulation_tick: self.simulation_tick + 1,
        }
    }
}

/// Snapshot of economy-wide state for the Economy RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EconomySnapshot {
    pub currencies: Vec<CurrencyInfo>,
    pub pricing_revision: u64,
    pub energy_mode: EnergyMode,
}

/// Currency metadata for economy snapshot responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrencyInfo {
    pub code: String,
    pub symbol: String,
    pub precision: u32,
    pub total_in_circulation: u64,
    pub exchange_rate_to_usd: f64,
    pub enabled: bool,
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
    SessionCreated {
        session_id: Uuid,
        player_id: Uuid,
        server_id: String,
    },
    SessionTerminated {
        session_id: Uuid,
        player_id: Uuid,
        reason: String,
    },
    SessionReconnected {
        session_id: Uuid,
        player_id: Uuid,
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

// =========================================================================
// PLAYER SESSION DOMAIN ENTITIES
// =========================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum SessionState {
    Active = 0,
    Expired = 1,
    Terminated = 2,
    Reconnected = 3,
    Transitioning = 4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerSession {
    pub session_id: SessionId,
    pub player_id: PlayerId,
    pub server_id: String,
    pub state: SessionState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub revision: u64,
}

impl PlayerSession {
    pub fn new(player_id: PlayerId, server_id: String, heartbeat_timeout_secs: i64) -> Self {
        let now = Utc::now();
        Self {
            session_id: SessionId(Uuid::new_v4()),
            player_id,
            server_id,
            state: SessionState::Active,
            created_at: now,
            updated_at: now,
            last_heartbeat: now,
            expires_at: now + chrono::Duration::seconds(heartbeat_timeout_secs),
            revision: 0,
        }
    }

    pub fn is_active(&self) -> bool {
        self.state == SessionState::Active || self.state == SessionState::Reconnected
    }

    pub fn is_expired(&self) -> bool {
        self.state == SessionState::Expired || Utc::now() > self.expires_at
    }

    pub fn heartbeat(&mut self, heartbeat_timeout_secs: i64) {
        let now = Utc::now();
        self.last_heartbeat = now;
        self.expires_at = now + chrono::Duration::seconds(heartbeat_timeout_secs);
        self.updated_at = now;
        self.revision += 1;
    }

    pub fn terminate(&mut self) {
        self.state = SessionState::Terminated;
        self.updated_at = Utc::now();
        self.revision += 1;
    }

    pub fn mark_reconnected(&mut self, heartbeat_timeout_secs: i64) {
        self.state = SessionState::Reconnected;
        let now = Utc::now();
        self.last_heartbeat = now;
        self.expires_at = now + chrono::Duration::seconds(heartbeat_timeout_secs);
        self.updated_at = now;
        self.revision += 1;
    }

    pub fn mark_expired(&mut self) {
        self.state = SessionState::Expired;
        self.updated_at = Utc::now();
        self.revision += 1;
    }

    pub fn mark_transitioning(&mut self, new_server_id: String) {
        self.state = SessionState::Transitioning;
        self.server_id = new_server_id;
        self.updated_at = Utc::now();
        self.revision += 1;
    }

    pub fn restore_from_transition(&mut self, original_server_id: String) {
        self.state = SessionState::Active;
        self.server_id = original_server_id;
        self.updated_at = Utc::now();
        self.revision += 1;
    }

    /// Returns true if this session can be reconnected (still active/reconnected and not yet expired)
    pub fn can_reconnect(&self) -> bool {
        self.is_active() && Utc::now() <= self.expires_at
    }
}

// New domain errors for sessions
// (appended to DomainError enum below)

// =========================================================================
// SIMULATION ENGINE DOMAIN ENTITIES
// =========================================================================

/// Unique identifier for a simulation tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TickId(pub Uuid);

/// Status of a completed (or in-progress) simulation tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TickStatus {
    /// Tick is currently executing
    InProgress,
    /// All subsystems completed successfully
    Completed,
    /// One or more subsystems partially failed
    PartialFailure,
    /// Tick was aborted due to catastrophic failure
    Failed,
}

/// Execution outcome for a single subsystem within a tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsystemTickOutcome {
    pub subsystem_name: String,
    pub status: TickStatus,
    pub duration_ms: i64,
    pub error: Option<String>,
    pub events_generated: u64,
    pub entities_processed: u64,
}

/// Full metrics for one simulation tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickMetrics {
    pub tick_id: TickId,
    pub tick_number: u64,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub duration_ms: i64,
    pub status: TickStatus,
    pub subsystems: Vec<SubsystemTickOutcome>,
    pub total_events: u64,
    pub total_entities_processed: u64,
}

impl TickMetrics {
    pub fn new(tick_id: TickId, tick_number: u64) -> Self {
        Self {
            tick_id,
            tick_number,
            started_at: chrono::Utc::now(),
            finished_at: None,
            duration_ms: 0,
            status: TickStatus::InProgress,
            subsystems: Vec::new(),
            total_events: 0,
            total_entities_processed: 0,
        }
    }

    pub fn record_subsystem(&mut self, outcome: SubsystemTickOutcome) {
        self.total_events += outcome.events_generated;
        self.total_entities_processed += outcome.entities_processed;
        self.subsystems.push(outcome);
    }

    pub fn finalize(&mut self, status: TickStatus) {
        self.finished_at = Some(chrono::Utc::now());
        self.duration_ms = (self.finished_at.unwrap() - self.started_at).num_milliseconds();
        self.status = status;
    }
}

/// Context passed to every subsystem during a tick.
#[derive(Debug, Clone)]
pub struct SimulationContext {
    /// Monotonically increasing tick number
    pub tick_number: u64,
    /// When this tick started
    pub now: chrono::DateTime<chrono::Utc>,
    /// Configured tick duration (for timing awareness)
    pub tick_duration_ms: i64,
    /// Unique idempotency key for this tick
    pub request_id: Uuid,
    /// Correlation id linking this tick to its trigger
    pub correlation_id: Uuid,
}

impl SimulationContext {
    pub fn new(tick_number: u64, tick_duration_ms: i64, correlation_id: Uuid) -> Self {
        Self {
            tick_number,
            now: chrono::Utc::now(),
            tick_duration_ms,
            request_id: Uuid::new_v4(),
            correlation_id,
        }
    }
}

/// Every simulation subsystem must implement this trait.
/// The scheduler never knows implementation details.
#[async_trait::async_trait]
pub trait SimulationSystem: Send + Sync {
    /// Human-readable name for logging and metrics.
    fn name(&self) -> &str;

    /// Execute this subsystem's logic for one tick.
    /// Returns the number of domain events generated.
    async fn tick(&self, ctx: &SimulationContext) -> Result<SubsystemTickOutcome, DomainError>;
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
            DomainError::InsufficientFunds {
                wallet_id,
                balance,
                required,
            } => {
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
        )
        .unwrap_err();

        match tx_err {
            DomainError::LedgerImbalance => {}
            _ => panic!("Expected LedgerImbalance error"),
        }
    }
}
