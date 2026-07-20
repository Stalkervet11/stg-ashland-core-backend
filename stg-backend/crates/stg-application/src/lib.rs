#![allow(clippy::too_many_arguments)]
pub mod locale;
pub mod scheduler;
pub mod transaction;

use async_trait::async_trait;
use chrono::Utc;
use sha2::{Digest, Sha256};
use stg_domain::{
    ConversionDirection, ConversionQuote, ConversionRule, DomainError, DomainEvent,
    DomainEventPayload, EconomySnapshot, EconomyTransaction, EnergyNode, EnergyState, Money,
    Player, PlayerId, PlayerSession, PlayerStatus, PlayerTransition, ReservationId,
    ReservationStatus, ResourceConversionReservation, ResourceRef, SessionId, SessionState,
    TransactionId, TransitionId, TransitionStatus, Wallet, WalletId,
};
use uuid::Uuid;

pub use locale::{LocalizationProvider, MessageKey};

pub use transaction::{
    DomainEventCollector, OutboxRecord, OutboxStorage, TransactionHandle, TransactionManager,
};

// =========================================================================
// REPOSITORY PORTS (INTERFACES FOR DEPENDENCY INVERSION)
// =========================================================================

#[async_trait]
pub trait PlayerRepository: Send + Sync {
    async fn find_by_id(&self, id: PlayerId) -> Result<Option<Player>, DomainError>;
    async fn save(&self, player: &Player) -> Result<(), DomainError>;
    async fn update(&self, player: &Player) -> Result<(), DomainError>;
}

#[async_trait]
pub trait WalletRepository: Send + Sync {
    async fn find_by_id(&self, id: WalletId) -> Result<Option<Wallet>, DomainError>;
    async fn find_by_player_and_currency(
        &self,
        player_id: PlayerId,
        currency_code: &str,
    ) -> Result<Option<Wallet>, DomainError>;
    async fn save(&self, wallet: &Wallet) -> Result<(), DomainError>;
    async fn update(&self, wallet: &Wallet) -> Result<(), DomainError>;
}

#[async_trait]
pub trait TransactionRepository: Send + Sync {
    async fn commit_transaction_with_balances(
        &self,
        transaction: &EconomyTransaction,
        updated_wallets: &[Wallet],
    ) -> Result<(), DomainError>;

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
    ) -> Result<EconomyTransaction, DomainError>;

    async fn find_by_id(
        &self,
        id: TransactionId,
    ) -> Result<Option<EconomyTransaction>, DomainError>;
    async fn is_idempotent_processed(&self, key: Uuid) -> Result<bool, DomainError>;
    async fn save_processed_request(&self, key: Uuid, result_json: &str)
        -> Result<(), DomainError>;
    async fn get_processed_result(&self, key: Uuid) -> Result<Option<String>, DomainError>;
}

#[async_trait]
pub trait EnergyRepository: Send + Sync {
    async fn find_node_by_id(&self, id: Uuid) -> Result<Option<EnergyNode>, DomainError>;
    async fn list_active_nodes(&self) -> Result<Vec<EnergyNode>, DomainError>;
    async fn save_node(&self, node: &EnergyNode) -> Result<(), DomainError>;
    async fn update_node(&self, node: &EnergyNode) -> Result<(), DomainError>;
    async fn get_latest_state(&self) -> Result<EnergyState, DomainError>;
    async fn save_state(&self, state: &EnergyState) -> Result<(), DomainError>;
}

#[async_trait]
pub trait TransitionRepository: Send + Sync {
    async fn find_by_id(&self, id: TransitionId) -> Result<Option<PlayerTransition>, DomainError>;
    async fn find_by_ticket(&self, ticket: &str) -> Result<Option<PlayerTransition>, DomainError>;
    async fn save_transition(&self, transition: &PlayerTransition) -> Result<(), DomainError>;
    async fn update_transition(&self, transition: &PlayerTransition) -> Result<(), DomainError>;
}

#[async_trait]
pub trait SessionRepository: Send + Sync {
    /// Find active session for a player (Active or Reconnected state)
    async fn find_active_by_player(
        &self,
        player_id: PlayerId,
    ) -> Result<Option<PlayerSession>, DomainError>;
    /// Find any session by session id
    async fn find_by_id(&self, session_id: SessionId)
        -> Result<Option<PlayerSession>, DomainError>;
    /// Insert a new session
    async fn save(&self, session: &PlayerSession) -> Result<(), DomainError>;
    /// Update session with optimistic locking
    async fn update(&self, session: &PlayerSession) -> Result<(), DomainError>;
    /// Find active session for player with FOR UPDATE lock (for atomic operations)
    async fn find_active_by_player_for_update(
        &self,
        tx: &mut dyn TransactionHandle,
        player_id: PlayerId,
    ) -> Result<Option<PlayerSession>, DomainError>;
    /// Update session within an existing transaction
    async fn update_in_tx(
        &self,
        tx: &mut dyn TransactionHandle,
        session: &PlayerSession,
    ) -> Result<(), DomainError>;
    /// Save session within an existing transaction
    async fn save_in_tx(
        &self,
        tx: &mut dyn TransactionHandle,
        session: &PlayerSession,
    ) -> Result<(), DomainError>;
    /// Expire all sessions past their expiration time
    async fn expire_stale_sessions(&self) -> Result<u64, DomainError>;
    /// Atomic begin_transition with advisory lock and optimistic locking
    async fn begin_transition_atomic(
        &self,
        player_id: PlayerId,
        from_server_id: &str,
        to_server_id: &str,
        ticket: &str,
    ) -> Result<PlayerSession, DomainError>;
}

#[async_trait]
pub trait EventLogRepository: Send + Sync {
    async fn append_event(&self, event: &DomainEvent) -> Result<(), DomainError>;
}

#[async_trait]
pub trait ConversionRepository: Send + Sync {
    async fn find_rule(
        &self,
        namespace: &str,
        path: &str,
        direction: ConversionDirection,
    ) -> Result<Option<ConversionRule>, DomainError>;
    async fn save_reservation(
        &self,
        reservation: &ResourceConversionReservation,
    ) -> Result<(), DomainError>;
    async fn find_reservation_by_id(
        &self,
        id: ReservationId,
    ) -> Result<Option<ResourceConversionReservation>, DomainError>;
    async fn abort_reservation(
        &self,
        id: ReservationId,
    ) -> Result<ResourceConversionReservation, DomainError>;
    async fn commit_conversion_atomic(
        &self,
        idempotency_key: Uuid,
        fingerprint: &str,
        reservation_id: ReservationId,
        initiating_server: &str,
        correlation_id: Uuid,
    ) -> Result<EconomyTransaction, DomainError>;
}

#[async_trait]
pub trait QueuePublisher: Send + Sync {
    async fn publish(&self, event: &DomainEvent) -> Result<(), DomainError>;
}

// =========================================================================
// APPLICATION SERVICES (USE CASES)
// =========================================================================

pub struct PlayerService {
    player_repo: Box<dyn PlayerRepository>,
    wallet_repo: Box<dyn WalletRepository>,
    event_repo: Box<dyn EventLogRepository>,
    queue_pub: Box<dyn QueuePublisher>,
}

impl PlayerService {
    pub fn new(
        player_repo: Box<dyn PlayerRepository>,
        wallet_repo: Box<dyn WalletRepository>,
        event_repo: Box<dyn EventLogRepository>,
        queue_pub: Box<dyn QueuePublisher>,
    ) -> Self {
        Self {
            player_repo,
            wallet_repo,
            event_repo,
            queue_pub,
        }
    }

    pub async fn register_player(
        &self,
        player_uuid: Uuid,
        username: String,
        correlation_id: Uuid,
    ) -> Result<Player, DomainError> {
        let player_id = PlayerId(player_uuid);
        if let Some(existing) = self.player_repo.find_by_id(player_id).await? {
            return Ok(existing);
        }

        let player = Player {
            id: player_id,
            username: username.clone(),
            status: PlayerStatus::Active,
            created_at: Utc::now(),
            last_seen_at: Utc::now(),
            revision: 0,
        };

        self.player_repo.save(&player).await?;

        let wallet = Wallet {
            id: WalletId(Uuid::new_v4()),
            player_id,
            currency_code: "ASH".to_string(),
            balance: Money::zero(),
            revision: 0,
        };
        self.wallet_repo.save(&wallet).await?;

        let event = DomainEvent {
            id: Uuid::new_v4(),
            aggregate_type: "Player".to_string(),
            aggregate_id: player_uuid,
            aggregate_version: 0,
            payload: DomainEventPayload::PlayerRegistered {
                id: player_uuid,
                username,
            },
            occurred_at: Utc::now(),
            correlation_id,
        };

        self.event_repo.append_event(&event).await?;
        self.queue_pub.publish(&event).await?;

        Ok(player)
    }

    pub async fn get_player_snapshot(
        &self,
        player_uuid: Uuid,
    ) -> Result<(Player, Vec<Wallet>), DomainError> {
        let player_id = PlayerId(player_uuid);
        let player = self
            .player_repo
            .find_by_id(player_id)
            .await?
            .ok_or(DomainError::PlayerNotFound(player_uuid))?;

        let ash_wallet = self
            .wallet_repo
            .find_by_player_and_currency(player_id, "ASH")
            .await?;

        let mut wallets = Vec::new();
        if let Some(w) = ash_wallet {
            wallets.push(w);
        }

        Ok((player, wallets))
    }
}

pub struct ConversionService {
    player_repo: Box<dyn PlayerRepository>,
    conv_repo: Box<dyn ConversionRepository>,
}

pub fn calculate_conversion_fingerprint(
    player_id: PlayerId,
    namespace: &str,
    path: &str,
    amount: i64,
    direction: ConversionDirection,
    initiating_server: &str,
    correlation_id: Uuid,
) -> String {
    let mut hasher = Sha256::new();
    let payload = format!(
        "{}:{}:{}:{}:{:?}:{}:{}",
        player_id.0, namespace, path, amount, direction, initiating_server, correlation_id
    );
    hasher.update(payload.as_bytes());
    let result = hasher.finalize();
    format!("{:x}", result)
}

impl ConversionService {
    pub fn new(
        player_repo: Box<dyn PlayerRepository>,
        conv_repo: Box<dyn ConversionRepository>,
    ) -> Self {
        Self {
            player_repo,
            conv_repo,
        }
    }

    pub async fn prepare_conversion(
        &self,
        player_id: PlayerId,
        namespace: &str,
        path: &str,
        amount: i64,
        direction: ConversionDirection,
        idempotency_key: Uuid,
        initiating_server: &str,
        correlation_id: Uuid,
    ) -> Result<ResourceConversionReservation, DomainError> {
        if amount <= 0 {
            return Err(DomainError::InvalidAmount(amount));
        }

        if self.player_repo.find_by_id(player_id).await?.is_none() {
            return Err(DomainError::PlayerNotFound(player_id.0));
        }

        let rule = self
            .conv_repo
            .find_rule(namespace, path, direction)
            .await?
            .ok_or(DomainError::ConversionRuleNotFound)?;

        if !rule.enabled {
            return Err(DomainError::ConversionRuleNotFound);
        }

        if amount < rule.min_amount || amount > rule.max_amount {
            return Err(DomainError::InvalidAmount(amount));
        }

        let reservation_id = ReservationId(idempotency_key);

        if let Some(existing) = self
            .conv_repo
            .find_reservation_by_id(reservation_id)
            .await?
        {
            return Ok(existing);
        }

        let total_price = (amount as i128)
            .checked_mul(rule.unit_price_minor as i128)
            .ok_or(DomainError::ArithmeticOverflow)?;

        if total_price > i64::MAX as i128 {
            return Err(DomainError::ArithmeticOverflow);
        }

        let total_price_minor = total_price as i64;

        let quote = ConversionQuote {
            quote_id: Uuid::new_v4().to_string(),
            resource: ResourceRef {
                namespace: namespace.to_string(),
                path: path.to_string(),
            },
            resource_amount: amount,
            currency_code: rule.currency_code,
            unit_price_minor: rule.unit_price_minor,
            total_price_minor,
            pricing_revision: rule.pricing_revision,
            expires_at: Utc::now() + chrono::Duration::minutes(5),
        };

        let mut required_mutations = Vec::new();
        let amount_delta = match direction {
            ConversionDirection::ResourceToCurrency => -amount,
            ConversionDirection::CurrencyToResource => amount,
            _ => 0,
        };

        required_mutations.push(stg_domain::ResourceMutation {
            resource: quote.resource.clone(),
            amount_delta,
        });

        let reservation = ResourceConversionReservation {
            id: reservation_id,
            player_id,
            status: ReservationStatus::Prepared,
            quote,
            required_mutations,
            expires_at: Utc::now() + chrono::Duration::minutes(5),
        };

        self.conv_repo.save_reservation(&reservation).await?;

        let _fingerprint = calculate_conversion_fingerprint(
            player_id,
            namespace,
            path,
            amount,
            direction,
            initiating_server,
            correlation_id,
        );

        Ok(reservation)
    }

    pub async fn commit_conversion(
        &self,
        idempotency_key: Uuid,
        reservation_id: ReservationId,
        initiating_server: &str,
        correlation_id: Uuid,
    ) -> Result<EconomyTransaction, DomainError> {
        let fingerprint = format!("COMMIT:{}", reservation_id.0);

        self.conv_repo
            .commit_conversion_atomic(
                idempotency_key,
                &fingerprint,
                reservation_id,
                initiating_server,
                correlation_id,
            )
            .await
    }

    pub async fn abort_conversion(
        &self,
        reservation_id: ReservationId,
    ) -> Result<ResourceConversionReservation, DomainError> {
        self.conv_repo.abort_reservation(reservation_id).await
    }
}

pub struct EnergyService {
    energy_repo: Box<dyn EnergyRepository>,
}

impl EnergyService {
    pub fn new(energy_repo: Box<dyn EnergyRepository>) -> Self {
        Self { energy_repo }
    }

    pub async fn register_node(&self, node: EnergyNode) -> Result<(), DomainError> {
        self.energy_repo.save_node(&node).await
    }

    pub async fn report_observation(
        &self,
        node_id: Uuid,
        prod: i64,
        cons: i64,
        stored: i64,
    ) -> Result<(), DomainError> {
        let mut node = self
            .energy_repo
            .find_node_by_id(node_id)
            .await?
            .ok_or(DomainError::EnergyNodeNotFound(node_id))?;
        node.production_watts = prod;
        node.consumption_watts = cons;
        node.stored_wh = stored;
        node.last_reported_at = Utc::now();
        self.energy_repo.update_node(&node).await
    }

    pub async fn get_state(&self) -> Result<EnergyState, DomainError> {
        match self.energy_repo.get_latest_state().await {
            Ok(state) => Ok(state),
            Err(_) => Ok(EnergyState::initial()),
        }
    }
}

#[allow(dead_code)]
pub struct EconomyService {
    player_repo: Box<dyn PlayerRepository>,
    wallet_repo: Box<dyn WalletRepository>,
    tx_repo: Box<dyn TransactionRepository>,
    event_repo: Box<dyn EventLogRepository>,
    queue_pub: Box<dyn QueuePublisher>,
}

pub fn calculate_fingerprint(
    from_player: PlayerId,
    to_player: PlayerId,
    currency: &str,
    amount_minor: i64,
    initiating_server: &str,
    correlation_id: Uuid,
) -> String {
    let mut hasher = Sha256::new();
    let payload = format!(
        "{}:{}:{}:{}:{}:{}",
        from_player.0, to_player.0, currency, amount_minor, initiating_server, correlation_id
    );
    hasher.update(payload.as_bytes());
    let result = hasher.finalize();
    format!("{:x}", result)
}

impl EconomyService {
    pub fn new(
        player_repo: Box<dyn PlayerRepository>,
        wallet_repo: Box<dyn WalletRepository>,
        tx_repo: Box<dyn TransactionRepository>,
        event_repo: Box<dyn EventLogRepository>,
        queue_pub: Box<dyn QueuePublisher>,
    ) -> Self {
        Self {
            player_repo,
            wallet_repo,
            tx_repo,
            event_repo,
            queue_pub,
        }
    }

    /// Returns a snapshot of the current economy state.
    /// Currency aggregation requires the WalletRepository::list_all() extension point.
    /// Currently returns an empty snapshot; to enable real data add list_all() to
    /// WalletRepository trait and replace the placeholder below.
    pub async fn get_economy_state(&self) -> Result<EconomySnapshot, DomainError> {
        // When WalletRepository::list_all() is available:
        //   let wallets = self.wallet_repo.list_all().await?;
        //   aggregate currencies by code, sum balances, populate CurrencyInfo list.
        Ok(EconomySnapshot {
            currencies: Vec::new(),
            pricing_revision: 0,
            energy_mode: stg_domain::EnergyMode::Normal,
        })
    }

    pub async fn transfer_money(
        &self,
        from_player: PlayerId,
        to_player: PlayerId,
        currency: &str,
        amount_minor: i64,
        idempotency_key: Uuid,
        initiating_server: String,
        correlation_id: Uuid,
    ) -> Result<EconomyTransaction, DomainError> {
        if amount_minor <= 0 {
            return Err(DomainError::InvalidAmount(amount_minor));
        }

        let fingerprint = calculate_fingerprint(
            from_player,
            to_player,
            currency,
            amount_minor,
            &initiating_server,
            correlation_id,
        );

        self.tx_repo
            .execute_transfer_atomic(
                idempotency_key,
                &fingerprint,
                from_player,
                to_player,
                currency,
                amount_minor,
                &initiating_server,
                correlation_id,
            )
            .await
    }
}

pub struct EnergySimulationService {
    energy_repo: Box<dyn EnergyRepository>,
    event_repo: Box<dyn EventLogRepository>,
    queue_pub: Box<dyn QueuePublisher>,
}

impl EnergySimulationService {
    pub fn new(
        energy_repo: Box<dyn EnergyRepository>,
        event_repo: Box<dyn EventLogRepository>,
        queue_pub: Box<dyn QueuePublisher>,
    ) -> Self {
        Self {
            energy_repo,
            event_repo,
            queue_pub,
        }
    }

    pub async fn execute_tick(&self, correlation_id: Uuid) -> Result<EnergyState, DomainError> {
        let current_state = self.energy_repo.get_latest_state().await?;
        let active_nodes = self.energy_repo.list_active_nodes().await?;

        let next_state = current_state.calculate_tick(&active_nodes);

        self.energy_repo.save_state(&next_state).await?;

        if current_state.mode != next_state.mode {
            let event = DomainEvent {
                id: Uuid::new_v4(),
                aggregate_type: "Energy".to_string(),
                aggregate_id: Uuid::nil(),
                aggregate_version: next_state.simulation_tick,
                payload: DomainEventPayload::EnergyModeChanged {
                    old_mode: current_state.mode,
                    new_mode: next_state.mode,
                },
                occurred_at: Utc::now(),
                correlation_id,
            };

            self.event_repo.append_event(&event).await?;
            self.queue_pub.publish(&event).await?;
        }

        Ok(next_state)
    }

    pub async fn report_node_observation(
        &self,
        node_uuid: Uuid,
        production: i64,
        consumption: i64,
        storage: i64,
    ) -> Result<EnergyNode, DomainError> {
        let mut node = self
            .energy_repo
            .find_node_by_id(node_uuid)
            .await?
            .ok_or(DomainError::EnergyNodeNotFound(node_uuid))?;

        node.production_watts = production.clamp(0, node.capacity_watts);
        node.consumption_watts = consumption.clamp(0, node.capacity_watts);
        node.stored_wh = storage.clamp(0, node.max_stored_wh);
        node.last_reported_at = Utc::now();
        node.revision += 1;

        self.energy_repo.update_node(&node).await?;

        Ok(node)
    }
}

// =========================================================================
// TRANSITION SERVICE (USE CASE)
// =========================================================================

pub struct TransitionService {
    transition_repo: Box<dyn TransitionRepository>,
    player_repo: Box<dyn PlayerRepository>,
}

impl TransitionService {
    pub fn new(
        transition_repo: Box<dyn TransitionRepository>,
        player_repo: Box<dyn PlayerRepository>,
    ) -> Self {
        Self {
            transition_repo,
            player_repo,
        }
    }

    pub async fn begin_transition(
        &self,
        player_uuid: Uuid,
        _target_server_id: String,
        _payload_format: String,
        _payload_version: u32,
        _opaque_payload: Vec<u8>,
        _initiating_server: &str,
    ) -> Result<PlayerTransition, DomainError> {
        let player_id = PlayerId(player_uuid);
        let _player = self
            .player_repo
            .find_by_id(player_id)
            .await?
            .ok_or(DomainError::PlayerNotFound(player_uuid))?;

        let ticket = Uuid::new_v4().to_string();
        let transition = PlayerTransition {
            id: TransitionId(Uuid::new_v4()),
            ticket: ticket.clone(),
            status: TransitionStatus::Pending,
            player_id,
        };

        self.transition_repo.save_transition(&transition).await?;
        Ok(transition)
    }

    pub async fn claim_transition(&self, ticket: &str) -> Result<PlayerTransition, DomainError> {
        self.transition_repo
            .find_by_ticket(ticket)
            .await?
            .ok_or(DomainError::InternalStateError(
                "Transition ticket not found".into(),
            ))
    }

    pub async fn commit_transition(
        &self,
        transition_id: Uuid,
    ) -> Result<PlayerTransition, DomainError> {
        let mut transition = self
            .transition_repo
            .find_by_id(TransitionId(transition_id))
            .await?
            .ok_or(DomainError::InternalStateError(
                "Transition not found".into(),
            ))?;

        transition.status = TransitionStatus::Completed;
        self.transition_repo.update_transition(&transition).await?;
        Ok(transition)
    }

    pub async fn abort_transition(
        &self,
        transition_id: Uuid,
        _reason: &str,
    ) -> Result<PlayerTransition, DomainError> {
        let mut transition = self
            .transition_repo
            .find_by_id(TransitionId(transition_id))
            .await?
            .ok_or(DomainError::InternalStateError(
                "Transition not found".into(),
            ))?;

        transition.status = TransitionStatus::Failed;
        self.transition_repo.update_transition(&transition).await?;
        Ok(transition)
    }
}

// =========================================================================
// WORLD SNAPSHOT SERVICE (USE CASE)
// =========================================================================

pub struct WorldSnapshotService {
    energy_repo: Box<dyn EnergyRepository>,
}

impl WorldSnapshotService {
    pub fn new(energy_repo: Box<dyn EnergyRepository>) -> Self {
        Self { energy_repo }
    }

    pub async fn get_world_snapshot(&self) -> Result<EnergyState, DomainError> {
        match self.energy_repo.get_latest_state().await {
            Ok(state) => Ok(state),
            Err(_) => Ok(EnergyState::initial()),
        }
    }
}

// =========================================================================
// SESSION SERVICE (USE CASES)
// =========================================================================

/// Configuration for the session service.
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// How long a session can go without heartbeat before expiring (seconds)
    pub heartbeat_timeout_secs: i64,
    /// Maximum grace period for reconnection after expiry (seconds). 0 = no grace.
    pub reconnect_grace_secs: i64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout_secs: 300, // 5 minutes
            reconnect_grace_secs: 60,    // 1 minute grace after expiry
        }
    }
}

pub struct SessionService {
    session_repo: Box<dyn SessionRepository>,
    player_repo: Box<dyn PlayerRepository>,
    tx_manager: Box<dyn TransactionManager>,
    config: SessionConfig,
}

impl SessionService {
    pub fn new(
        session_repo: Box<dyn SessionRepository>,
        player_repo: Box<dyn PlayerRepository>,
        tx_manager: Box<dyn TransactionManager>,
        config: SessionConfig,
    ) -> Self {
        Self {
            session_repo,
            player_repo,
            tx_manager,
            config,
        }
    }

    /// Create a new session for a player.
    /// Fails if the player already has an active session (DuplicateSession).
    pub async fn create_session(
        &self,
        player_id: PlayerId,
        server_id: String,
    ) -> Result<PlayerSession, DomainError> {
        // Verify player exists
        if self.player_repo.find_by_id(player_id).await?.is_none() {
            return Err(DomainError::PlayerNotFound(player_id.0));
        }

        // Use transaction for atomic check-then-insert
        let mut tx = self.tx_manager.begin_transaction().await?;
        let existing = self
            .session_repo
            .find_active_by_player_for_update(&mut *tx, player_id)
            .await?;

        if let Some(session) = existing {
            if session.is_expired() {
                let mut expired = session;
                expired.mark_expired();
                self.session_repo.update_in_tx(&mut *tx, &expired).await?;
                tx.commit().await?;
            } else {
                tx.rollback().await?;
                return Err(DomainError::DuplicateSession(session.session_id.0));
            }
        } else {
            tx.commit().await?;
        }

        let session = PlayerSession::new(player_id, server_id, self.config.heartbeat_timeout_secs);
        self.session_repo.save(&session).await?;
        Ok(session)
    }

    /// Heartbeat: extends the session expiration and updates last_heartbeat.
    pub async fn heartbeat(&self, session_id: SessionId) -> Result<PlayerSession, DomainError> {
        let mut session = self
            .session_repo
            .find_by_id(session_id)
            .await?
            .ok_or(DomainError::SessionNotFound(session_id.0))?;

        if session.state == SessionState::Terminated {
            return Err(DomainError::SessionTerminated(session_id.0));
        }

        if session.is_expired() {
            session.mark_expired();
            self.session_repo.update(&session).await?;
            return Err(DomainError::SessionExpired(session_id.0));
        }

        session.heartbeat(self.config.heartbeat_timeout_secs);
        self.session_repo.update(&session).await?;
        Ok(session)
    }

    /// Terminate a session (explicit logout or admin action).
    pub async fn terminate_session(
        &self,
        session_id: SessionId,
        _reason: String,
    ) -> Result<PlayerSession, DomainError> {
        let mut session = self
            .session_repo
            .find_by_id(session_id)
            .await?
            .ok_or(DomainError::SessionNotFound(session_id.0))?;

        if session.state == SessionState::Terminated {
            return Err(DomainError::SessionTerminated(session_id.0));
        }

        session.terminate();
        self.session_repo.update(&session).await?;
        Ok(session)
    }

    /// Reconnect: if existing session is still valid, restore it.
    /// If expired, create a new session (after cleaning up old one).
    pub async fn reconnect(
        &self,
        player_id: PlayerId,
        server_id: String,
    ) -> Result<PlayerSession, DomainError> {
        // Verify player exists
        if self.player_repo.find_by_id(player_id).await?.is_none() {
            return Err(DomainError::PlayerNotFound(player_id.0));
        }

        // Use transaction for atomic check-then-insert/update
        let mut tx = self.tx_manager.begin_transaction().await?;
        let existing = self
            .session_repo
            .find_active_by_player_for_update(&mut *tx, player_id)
            .await?;

        match existing {
            Some(session) if session.can_reconnect() => {
                let mut session = session;
                session.mark_reconnected(self.config.heartbeat_timeout_secs);
                session.server_id = server_id;
                self.session_repo.update_in_tx(&mut *tx, &session).await?;
                tx.commit().await?;
                Ok(session)
            }
            Some(mut session) => {
                session.mark_expired();
                self.session_repo.update_in_tx(&mut *tx, &session).await?;
                tx.commit().await?;

                let new_session =
                    PlayerSession::new(player_id, server_id, self.config.heartbeat_timeout_secs);
                self.session_repo.save(&new_session).await?;
                Ok(new_session)
            }
            None => {
                tx.commit().await?;
                let session =
                    PlayerSession::new(player_id, server_id, self.config.heartbeat_timeout_secs);
                self.session_repo.save(&session).await?;
                Ok(session)
            }
        }
    }

    /// BeginTransition: move session ownership between servers.
    /// Uses advisory lock + optimistic locking to prevent race conditions.
    pub async fn begin_transition(
        &self,
        player_id: PlayerId,
        from_server_id: String,
        to_server_id: String,
        ticket: String,
    ) -> Result<PlayerSession, DomainError> {
        // The actual transactional work is done in the repository
        // to ensure advisory locks + optimistic locking in one transaction.
        // For now, we delegate to a higher-level orchestration.
        // The implementation uses the SessionRepository's find_active_by_player_for_update
        // combined with a transaction.

        // We'll implement this via the SessionRepository's transactional methods
        // which handle the advisory lock internally.
        self.session_repo
            .begin_transition_atomic(player_id, &from_server_id, &to_server_id, &ticket)
            .await
    }

    /// Expire all stale sessions (called by background worker).
    pub async fn expire_stale_sessions(&self) -> Result<u64, DomainError> {
        self.session_repo.expire_stale_sessions().await
    }

    /// Find active session for a player.
    pub async fn find_active_session(
        &self,
        player_id: PlayerId,
    ) -> Result<Option<PlayerSession>, DomainError> {
        self.session_repo.find_active_by_player(player_id).await
    }
}

// Re-exports from scheduler module
pub use scheduler::{
    SchedulerConfig, SchedulerDashboard, SimulationScheduler, SubsystemDashboardEntry,
    TickRepository,
};
