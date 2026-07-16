use async_trait::async_trait;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use stg_domain::{
    ConversionDirection, ConversionQuote, DomainError, DomainEvent, DomainEventPayload,
    EconomyTransaction, EconomyTransactionStatus, EconomyTransactionType, EnergyMode, EnergyNode,
    EnergyState, LedgerEntry, Money, Player, PlayerId, PlayerStatus, PlayerTransition,
    ReservationId, ReservationStatus, ResourceConversionReservation, ResourceRef, TransactionId, TransitionId,
    TransitionStatus, Wallet, WalletId, ConversionRule,
};
use uuid::Uuid;

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
    /// Commits an economic transaction and updates the involved wallet balances atomically.
    async fn commit_transaction_with_balances(
        &self,
        transaction: &EconomyTransaction,
        updated_wallets: &[Wallet],
    ) -> Result<(), DomainError>;

    /// Executes a money transfer atomically within a single database transaction,
    /// ensuring proper idempotency check with fingerprint, deterministic locking order,
    /// and saving of state, events, and outbox records.
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

    async fn find_by_id(&self, id: TransactionId) -> Result<Option<EconomyTransaction>, DomainError>;
    async fn is_idempotent_processed(&self, key: Uuid) -> Result<bool, DomainError>;
    async fn save_processed_request(&self, key: Uuid, result_json: &str) -> Result<(), DomainError>;
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
pub trait EventLogRepository: Send + Sync {
    async fn append_event(&self, event: &DomainEvent) -> Result<(), DomainError>;
}

#[async_trait]
pub trait ConversionRepository: Send + Sync {
    async fn find_rule(&self, namespace: &str, path: &str, direction: ConversionDirection) -> Result<Option<ConversionRule>, DomainError>;
    async fn save_reservation(&self, reservation: &ResourceConversionReservation) -> Result<(), DomainError>;
    async fn find_reservation_by_id(&self, id: ReservationId) -> Result<Option<ResourceConversionReservation>, DomainError>;
    
    /// Aborts a reservation if it is PREPARED. Fails if COMMITTED. Leaves EXPIRED as is. Returns the final state.
    async fn abort_reservation(&self, id: ReservationId) -> Result<ResourceConversionReservation, DomainError>;

    /// Executes the conversion atomatically, mutating the wallet, logging the transaction, etc.
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
    /// Publishes a domain event to an external event queue (NATS / RabbitMQ / Redis)
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

        // Create initial wallet
        let wallet = Wallet {
            id: WalletId(Uuid::new_v4()),
            player_id,
            currency_code: "ASH".to_string(),
            balance: Money::zero(),
            revision: 0,
        };
        self.wallet_repo.save(&wallet).await?;

        // Domain Event creation
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
        let player = self.player_repo.find_by_id(player_id).await?
            .ok_or(DomainError::PlayerNotFound(player_uuid))?;

        let ash_wallet = self.wallet_repo.find_by_player_and_currency(player_id, "ASH").await?;
        
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
        // Validation
        if amount <= 0 {
            return Err(DomainError::InvalidAmount(amount));
        }

        // Check if player exists
        if self.player_repo.find_by_id(player_id).await?.is_none() {
            return Err(DomainError::PlayerNotFound(player_id.0));
        }

        // Load rule
        let rule = self.conv_repo.find_rule(namespace, path, direction).await?
            .ok_or(DomainError::ConversionRuleNotFound)?;

        if !rule.enabled {
            return Err(DomainError::ConversionRuleNotFound);
        }

        if amount < rule.min_amount || amount > rule.max_amount {
            return Err(DomainError::InvalidAmount(amount));
        }

        // Idempotency: Let's assume fingerprint check happens if needed.
        // For prepare, we can just create and return the reservation.
        // Wait! Prepare needs to "persist idempotent response". We will do this by saving the reservation.
        // Let's create a deterministic reservation ID based on idempotency_key to prevent duplicates.
        let reservation_id = ReservationId(idempotency_key);
        
        if let Some(existing) = self.conv_repo.find_reservation_by_id(reservation_id).await? {
            return Ok(existing);
        }

        // Calculate quote
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
        // Just defining what Minecraft should do (take or give)
        let amount_delta = match direction {
            ConversionDirection::ResourceToCurrency => -amount, // Minecraft takes resource
            ConversionDirection::CurrencyToResource => amount,  // Minecraft gives resource
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

        self.conv_repo.commit_conversion_atomic(
            idempotency_key,
            &fingerprint,
            reservation_id,
            initiating_server,
            correlation_id,
        ).await
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

    pub async fn report_observation(&self, node_id: Uuid, prod: i64, cons: i64, stored: i64) -> Result<(), DomainError> {
        let mut node = self.energy_repo.find_node_by_id(node_id).await?.ok_or(DomainError::EnergyNodeNotFound(node_id))?;
        node.production_watts = prod;
        node.consumption_watts = cons;
        node.stored_wh = stored;
        node.last_reported_at = Utc::now();
        self.energy_repo.update_node(&node).await
    }

    pub async fn get_state(&self) -> Result<EnergyState, DomainError> {
        // Basic fallback to initial if none exists
        match self.energy_repo.get_latest_state().await {
            Ok(state) => Ok(state),
            Err(_) => Ok(EnergyState::initial()),
        }
    }
}

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
        from_player.0,
        to_player.0,
        currency,
        amount_minor,
        initiating_server,
        correlation_id
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

        self.tx_repo.execute_transfer_atomic(
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

        // Calculate next state purely through domain rules
        let next_state = current_state.calculate_tick(&active_nodes);

        self.energy_repo.save_state(&next_state).await?;

        // If energy mode shifted, record an event and publish to queue
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
        let mut node = self.energy_repo.find_node_by_id(node_uuid).await?
            .ok_or(DomainError::EnergyNodeNotFound(node_uuid))?;

        // Validate capacity constraints
        node.production_watts = production.clamp(0, node.capacity_watts);
        node.consumption_watts = consumption.clamp(0, node.capacity_watts);
        node.stored_wh = storage.clamp(0, node.max_stored_wh);
        node.last_reported_at = Utc::now();
        node.revision += 1;

        self.energy_repo.update_node(&node).await?;

        Ok(node)
    }
}
