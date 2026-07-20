// STG-Core Unary RPC Implementation
//
// This module implements the StgUnary gRPC service trait with all
// request-response operations (no streaming). Extracted from main.rs
// to be a shared, modular component.

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use stg_application::{
    ConversionService, EconomyService, EnergyService, PlayerService, SessionService,
    TransitionService, WorldSnapshotService, LocalizationProvider, MessageKey,
};
use stg_domain::{
    ConversionDirection, DomainError, Locale, PlayerId, ReservationId,
};
use stg_infrastructure::{ResourceBundleLocalizationProvider, SystemMetrics, RequestContext};
use stg_proto::stg::v1::{
    stg_unary_server::StgUnary,
    abort_transition_request, abort_transition_response,
    AbortResourceConversionRequest, AbortResourceConversionResponse,
    AbortTransitionRequest, AbortTransitionResponse,
    BeginTransitionRequest, BeginTransitionResponse,
    ClaimTransitionRequest, ClaimTransitionResponse,
    CommitResourceConversionRequest, CommitTransitionRequest, CommitTransitionResponse,
    EconomyState, EnergyNode, EnergyState,
    GetEconomyStateRequest, GetEnergyStateRequest,
    GetPlayerSnapshotRequest, GetWorldRevisionRequest, GetWorldSnapshotRequest,
    PlayerSnapshot,
    PrepareResourceConversionRequest, PrepareResourceConversionResponse,
    RegisterEnergyNodeRequest, RegisterPlayerRequest, RegisterPlayerResponse,
    ReportEnergyObservationRequest, ReportEnergyObservationResponse,
    TransactionResult, TransferMoneyRequest,
    WalletBalance, WorldRevision, WorldSnapshot,
};

use stg_proto::stg::v1::{
    backend_message, server_message,
    BackendHello, BackendMessage, ServerHello, ServerMessage,
    register_player_response,
};

// =========================================================================
// Global Event Sequence
// =========================================================================

/// Monotonically increasing global event sequence number.
/// Every BackendMessage sent to any game server increments this counter.
static GLOBAL_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Returns the next sequence number (1-based).
pub fn next_sequence() -> u64 {
    GLOBAL_SEQUENCE.fetch_add(1, Ordering::SeqCst)
}

/// Returns the current sequence number without incrementing.
pub fn current_sequence() -> u64 {
    GLOBAL_SEQUENCE.load(Ordering::SeqCst)
}

// =========================================================================
// Domain Error -> tonic::Status (localized)
// =========================================================================

pub fn map_domain_error_localized(
    e: &DomainError,
    l10n: &dyn LocalizationProvider,
    locale: Locale,
) -> Status {
    let code = match e {
        DomainError::PlayerNotFound(_) => tonic::Code::NotFound,
        DomainError::InvalidAmount(_) => tonic::Code::InvalidArgument,
        DomainError::InsufficientFunds { .. } => tonic::Code::FailedPrecondition,
        DomainError::RevisionConflict(..) => tonic::Code::Aborted,
        DomainError::ConversionRuleNotFound => tonic::Code::NotFound,
        DomainError::ConversionReservationNotFound(_) => tonic::Code::NotFound,
        DomainError::ConversionReservationInvalidState(_) => tonic::Code::FailedPrecondition,
        DomainError::ConversionReservationExpired(_) => tonic::Code::FailedPrecondition,
        DomainError::EnergyNodeNotFound(_) => tonic::Code::NotFound,
        DomainError::LedgerImbalance => tonic::Code::Internal,
        DomainError::ArithmeticOverflow => tonic::Code::Internal,
        DomainError::IdempotencyConflict { .. } => tonic::Code::AlreadyExists,
        DomainError::ServerIdentityMismatch { .. } => tonic::Code::PermissionDenied,
        DomainError::SessionNotFound(_) => tonic::Code::NotFound,
        DomainError::SessionExpired(_) => tonic::Code::FailedPrecondition,
        DomainError::SessionTerminated(_) => tonic::Code::FailedPrecondition,
        DomainError::DuplicateSession(_) => tonic::Code::AlreadyExists,
        _ => tonic::Code::Internal,
    };
    let message = localize_domain_error(e, l10n, locale);
    Status::new(code, message)
}

fn localize_domain_error(
    e: &DomainError,
    l10n: &dyn LocalizationProvider,
    locale: Locale,
) -> String {
    match e {
        DomainError::PlayerNotFound(id) => {
            let base = l10n.localize(&MessageKey::new("errors.player.not_found"), locale);
            format!("{}: {}", base, id)
        }
        DomainError::InvalidAmount(amt) => {
            let base = l10n.localize(&MessageKey::new("errors.amount.invalid"), locale);
            format!("{}: {}", base, amt)
        }
        DomainError::InsufficientFunds { wallet_id, balance, required } => {
            l10n.localize(&MessageKey::new("errors.insufficient_funds"), locale)
                .replace("{balance}", &balance.to_string())
                .replace("{required}", &required.to_string())
                + &format!(" (wallet: {})", wallet_id)
        }
        DomainError::RevisionConflict(entity, expected, actual) => l10n
            .localize(&MessageKey::new("errors.revision_conflict"), locale)
            .replace("{entity}", entity)
            .replace("{expected}", &expected.to_string())
            .replace("{actual}", &actual.to_string()),
        DomainError::EnergyNodeNotFound(id) => {
            let base = l10n.localize(&MessageKey::new("errors.energy.node_not_found"), locale);
            format!("{}: {}", base, id)
        }
        DomainError::LedgerImbalance => {
            l10n.localize(&MessageKey::new("errors.ledger_imbalance"), locale)
        }
        DomainError::ArithmeticOverflow => {
            l10n.localize(&MessageKey::new("errors.arithmetic_overflow"), locale)
        }
        DomainError::IdempotencyConflict { request_id, .. } => l10n
            .localize(&MessageKey::new("errors.idempotency_conflict"), locale)
            .replace("{request_id}", &request_id.to_string()),
        DomainError::ConversionRuleNotFound => {
            l10n.localize(&MessageKey::new("errors.conversion.rule_not_found"), locale)
        }
        DomainError::ConversionReservationNotFound(id) => {
            let base = l10n.localize(
                &MessageKey::new("errors.conversion.reservation_not_found"), locale);
            format!("{}: {}", base, id)
        }
        DomainError::ConversionReservationInvalidState(_) => l10n.localize(
            &MessageKey::new("errors.conversion.reservation_invalid_state"), locale),
        DomainError::ConversionReservationExpired(id) => {
            let base = l10n.localize(
                &MessageKey::new("errors.conversion.reservation_expired"), locale);
            format!("{}: {}", base, id)
        }
        DomainError::DuplicateSession(id) => {
            let base = l10n.localize(&MessageKey::new("errors.session.duplicate"), locale);
            format!("{}: {}", base, id)
        }
        DomainError::SessionNotFound(id) => {
            let base = l10n.localize(&MessageKey::new("errors.session.not_found"), locale);
            format!("{}: {}", base, id)
        }
        DomainError::SessionTerminated(id) => {
            let base = l10n.localize(&MessageKey::new("errors.session.terminated"), locale);
            format!("{}: {}", base, id)
        }
        DomainError::SessionExpired(id) => {
            let base = l10n.localize(&MessageKey::new("errors.session.expired"), locale);
            format!("{}: {}", base, id)
        }
        DomainError::ServerIdentityMismatch { expected, actual } => l10n
            .localize(&MessageKey::new("errors.server.identity_mismatch"), locale)
            .replace("{expected}", expected)
            .replace("{actual}", actual),
        _ => l10n.localize(&MessageKey::new("errors.internal_state"), locale),
    }
}

pub fn extract_locale<T>(request: &Request<T>) -> Locale {
    request
        .metadata()
        .get("accept-language")
        .and_then(|v| v.to_str().ok())
        .and_then(Locale::parse)
        .unwrap_or_default()
}

fn map_energy_mode(mode: stg_domain::EnergyMode) -> i32 {
    match mode {
        stg_domain::EnergyMode::Normal => stg_proto::stg::v1::EnergyMode::EnergyNormal as i32,
        stg_domain::EnergyMode::Surplus => stg_proto::stg::v1::EnergyMode::EnergySurplus as i32,
        stg_domain::EnergyMode::Deficit => stg_proto::stg::v1::EnergyMode::EnergyDeficit as i32,
        stg_domain::EnergyMode::Critical => stg_proto::stg::v1::EnergyMode::EnergyCritical as i32,
        stg_domain::EnergyMode::Collapse => stg_proto::stg::v1::EnergyMode::EnergyCollapse as i32,
    }
}

fn map_transition_error(e: &DomainError) -> stg_proto::stg::v1::ErrorCode {
    match e {
        DomainError::PlayerNotFound(_) => stg_proto::stg::v1::ErrorCode::PlayerNotFound,
        DomainError::InternalStateError(_) => stg_proto::stg::v1::ErrorCode::InternalError,
        DomainError::RevisionConflict(..) => stg_proto::stg::v1::ErrorCode::RevisionConflict,
        DomainError::ServerIdentityMismatch { .. } => stg_proto::stg::v1::ErrorCode::ServerUnauthorized,
        DomainError::SessionNotFound(_) => stg_proto::stg::v1::ErrorCode::TransitionNotFound,
        DomainError::SessionExpired(_) => stg_proto::stg::v1::ErrorCode::InternalError,
        _ => stg_proto::stg::v1::ErrorCode::InternalError,
    }
}

// =========================================================================
// StgUnaryImpl — All unary RPC handlers
// =========================================================================

/// The canonical implementation of the STGUnary gRPC service.
/// All unary (request-response) operations are handled by this struct.
pub struct StgUnaryImpl {
    pub player_service: Arc<PlayerService>,
    pub economy_service: Arc<EconomyService>,
    pub energy_service: Arc<EnergyService>,
    pub conversion_service: Arc<ConversionService>,
    pub transition_service: Arc<TransitionService>,
    pub world_snapshot_service: Arc<WorldSnapshotService>,
    pub session_service: Arc<SessionService>,
    pub l10n: Arc<ResourceBundleLocalizationProvider>,
    pub metrics: Arc<SystemMetrics>,
    pub server_id: String,
    pub world_id: String,
}

impl StgUnaryImpl {
    pub fn new(
        player_service: Arc<PlayerService>,
        economy_service: Arc<EconomyService>,
        energy_service: Arc<EnergyService>,
        conversion_service: Arc<ConversionService>,
        transition_service: Arc<TransitionService>,
        world_snapshot_service: Arc<WorldSnapshotService>,
        session_service: Arc<SessionService>,
        l10n: Arc<ResourceBundleLocalizationProvider>,
        metrics: Arc<SystemMetrics>,
        server_id: String,
        world_id: String,
    ) -> Self {
        Self {
            player_service,
            economy_service,
            energy_service,
            conversion_service,
            transition_service,
            world_snapshot_service,
            session_service,
            l10n,
            metrics,
            server_id,
            world_id,
        }
    }

    /// Connect handler for the streaming service — delegates to this method.
    /// This is called by `StgStreamingAdapter::connect`.
    pub async fn connect_inner(
        &self,
        _request: Request<tonic::Streaming<stg_proto::stg::v1::ServerMessage>>,
    ) -> Result<Response<tonic::codec::Streaming<stg_proto::stg::v1::BackendMessage>>, Status> {
        // TODO: Implement full bidirectional streaming logic.
        // For now, return unimplemented to maintain compilation.
        Err(Status::unimplemented("Streaming connect not yet implemented in modular refactor"))
    }
}

#[tonic::async_trait]
impl StgUnary for StgUnaryImpl {
    // -------------------------------------------------------------
    // register_player
    // -------------------------------------------------------------

    async fn register_player(
        &self,
        request: Request<RegisterPlayerRequest>,
    ) -> Result<Response<RegisterPlayerResponse>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let ctx = RequestContext::new(self.server_id.clone());
        let locale = extract_locale(&request);
        let req = request.into_inner();
        let player_uuid = Uuid::parse_str(&req.player_uuid).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.player_uuid.invalid"), locale),
            )
        })?;
        let correlation_id = Uuid::new_v4();

        match self.player_service.register_player(player_uuid, req.username, correlation_id).await {
            Ok(p) => {
                tracing::info!(request_id = %ctx.request_id, player_id = %p.id.0, duration_ms = ctx.elapsed_ms(), "Player registered");
                Ok(Response::new(RegisterPlayerResponse {
                    result: Some(
                        register_player_response::Result::Player(
                            PlayerSnapshot {
                                identity: Some(stg_proto::stg::v1::PlayerIdentity {
                                    uuid: p.id.0.to_string(),
                                    username: p.username,
                                    status: stg_proto::stg::v1::PlayerStatus::Active as i32,
                                    created_at: Some(prost_types::Timestamp {
                                        seconds: p.created_at.timestamp(),
                                        nanos: p.created_at.timestamp_subsec_nanos() as i32,
                                    }),
                                    last_seen_at: Some(prost_types::Timestamp {
                                        seconds: p.last_seen_at.timestamp(),
                                        nanos: p.last_seen_at.timestamp_subsec_nanos() as i32,
                                    }),
                                }),
                                wallets: vec![],
                                global_reputation: 0,
                                faction_reputation: HashMap::new(),
                                integer_stats: HashMap::new(),
                                string_stats: HashMap::new(),
                                current_server_id: None,
                                transition: None,
                                revision: p.revision,
                                last_updated: Some(prost_types::Timestamp {
                                    seconds: chrono::Utc::now().timestamp(),
                                    nanos: 0,
                                }),
                            },
                        ),
                    ),
                }))
            }
            Err(e) => {
                tracing::warn!(request_id = %ctx.request_id, error = %e, "RegisterPlayer failed");
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(RegisterPlayerResponse {
                    result: Some(register_player_response::Result::Error(
                        stg_proto::stg::v1::Error {
                            code: stg_proto::stg::v1::ErrorCode::InternalError as i32,
                            message: e.to_string(),
                        },
                    )),
                }))
            }
        }
    }

    // -------------------------------------------------------------
    // get_player_snapshot
    // -------------------------------------------------------------

    async fn get_player_snapshot(
        &self,
        request: Request<GetPlayerSnapshotRequest>,
    ) -> Result<Response<PlayerSnapshot>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let ctx = RequestContext::new(self.server_id.clone());
        let locale = extract_locale(&request);
        let req = request.into_inner();
        let player_uuid = Uuid::parse_str(&req.player_uuid).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.player_uuid.invalid"), locale),
            )
        })?;

        match self.player_service.get_player_snapshot(player_uuid).await {
            Ok((player, wallets)) => {
                tracing::info!(request_id = %ctx.request_id, player_id = %player.id.0, duration_ms = ctx.elapsed_ms(), "GetPlayerSnapshot");
                let pb_wallets: Vec<WalletBalance> = wallets
                    .into_iter()
                    .map(|w| WalletBalance {
                        currency_code: w.currency_code,
                        amount_minor: w.balance.minor_units(),
                        revision: w.revision,
                    })
                    .collect();
                Ok(Response::new(PlayerSnapshot {
                    identity: Some(stg_proto::stg::v1::PlayerIdentity {
                        uuid: player.id.0.to_string(),
                        username: player.username,
                        status: stg_proto::stg::v1::PlayerStatus::Active as i32,
                        created_at: Some(prost_types::Timestamp {
                            seconds: player.created_at.timestamp(),
                            nanos: player.created_at.timestamp_subsec_nanos() as i32,
                        }),
                        last_seen_at: Some(prost_types::Timestamp {
                            seconds: player.last_seen_at.timestamp(),
                            nanos: player.last_seen_at.timestamp_subsec_nanos() as i32,
                        }),
                    }),
                    wallets: pb_wallets,
                    global_reputation: 0,
                    faction_reputation: HashMap::new(),
                    integer_stats: HashMap::new(),
                    string_stats: HashMap::new(),
                    current_server_id: None,
                    transition: None,
                    revision: player.revision,
                    last_updated: Some(prost_types::Timestamp {
                        seconds: chrono::Utc::now().timestamp(),
                        nanos: 0,
                    }),
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Err(map_domain_error_localized(&e, &*self.l10n, locale))
            }
        }
    }

    // -------------------------------------------------------------
    // get_world_snapshot
    // -------------------------------------------------------------

    async fn get_world_snapshot(
        &self,
        request: Request<GetWorldSnapshotRequest>,
    ) -> Result<Response<WorldSnapshot>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let _req = request.into_inner();

        match self.world_snapshot_service.get_world_snapshot().await {
            Ok(energy) => {
                let now_ts = Some(prost_types::Timestamp {
                    seconds: chrono::Utc::now().timestamp(),
                    nanos: 0,
                });
                tracing::info!(request_id = %ctx.request_id, duration_ms = ctx.elapsed_ms(), "GetWorldSnapshot");
                Ok(Response::new(WorldSnapshot {
                    world_id: self.world_id.clone(),
                    mode: stg_proto::stg::v1::WorldMode::WorldStable as i32,
                    energy: Some(EnergyState {
                        total_production_watts: 0,
                        total_consumption_watts: 0,
                        global_reserve_wh: 0,
                        global_reserve_capacity_wh: 0,
                        unmet_demand_watts: 0,
                        efficiency: 1.0,
                        mode: map_energy_mode(energy.mode),
                        simulation_tick: energy.simulation_tick,
                        revision: energy.simulation_tick,
                        warnings: vec![],
                        updated_at: now_ts,
                    }),
                    economy: Some(EconomyState {
                        currencies: vec![],
                        energy_mode: map_energy_mode(energy.mode),
                        pricing_revision: 0,
                        updated_at: now_ts,
                    }),
                    active_events: vec![],
                    simulation_tick: energy.simulation_tick,
                    revision: energy.simulation_tick,
                    updated_at: now_ts,
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Err(map_domain_error_localized(&e, &*self.l10n, locale))
            }
        }
    }

    // -------------------------------------------------------------
    // get_world_revision
    // -------------------------------------------------------------

    async fn get_world_revision(
        &self,
        request: Request<GetWorldRevisionRequest>,
    ) -> Result<Response<WorldRevision>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let _locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let _req = request.into_inner();

        let seq = current_sequence();
        tracing::info!(request_id = %ctx.request_id, world_revision = seq, duration_ms = ctx.elapsed_ms(), "GetWorldRevision");
        Ok(Response::new(WorldRevision {
            world_revision: seq,
            sequence_number: seq,
            server_time: Some(prost_types::Timestamp {
                seconds: chrono::Utc::now().timestamp(),
                nanos: 0,
            }),
        }))
    }

    // -------------------------------------------------------------
    // get_economy_state
    // -------------------------------------------------------------

    async fn get_economy_state(
        &self,
        request: Request<GetEconomyStateRequest>,
    ) -> Result<Response<EconomyState>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let _req = request.into_inner();

        match self.economy_service.get_economy_state().await {
            Ok(state) => {
                tracing::info!(request_id = %ctx.request_id, duration_ms = ctx.elapsed_ms(), "GetEconomyState");
                Ok(Response::new(EconomyState {
                    currencies: state.currencies.into_iter()
                        .map(|c| stg_proto::stg::v1::Currency {
                            code: c.code,
                            display_name: c.symbol,
                            minor_unit_scale: c.precision,
                            enabled: c.enabled,
                        })
                        .collect(),
                    energy_mode: map_energy_mode(state.energy_mode),
                    pricing_revision: state.pricing_revision,
                    updated_at: Some(prost_types::Timestamp {
                        seconds: chrono::Utc::now().timestamp(),
                        nanos: 0,
                    }),
                }))
            }
            Err(e) => {
                tracing::warn!(request_id = %ctx.request_id, error = %e, "GetEconomyState failed");
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Err(map_domain_error_localized(&e, &*self.l10n, locale))
            }
        }
    }

    // -------------------------------------------------------------
    // transfer_money
    // -------------------------------------------------------------

    async fn transfer_money(
        &self,
        request: Request<TransferMoneyRequest>,
    ) -> Result<Response<TransactionResult>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let req = request.into_inner();
        let from_uuid = Uuid::parse_str(&req.source_player_uuid).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.player_uuid.invalid"), locale),
            )
        })?;
        let to_uuid = Uuid::parse_str(&req.destination_player_uuid).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.player_uuid.invalid"), locale),
            )
        })?;
        let context = req.context
            .ok_or_else(|| Status::invalid_argument("Missing RequestContext"))?;
        let request_id = Uuid::parse_str(&context.request_id).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.request_id.invalid"), locale),
            )
        })?;
        let correlation_id = Uuid::parse_str(&context.correlation_id).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.correlation_id.invalid"), locale),
            )
        })?;
        let ctx = RequestContext::new(self.server_id.clone())
            .with_correlation(correlation_id)
            .with_player(from_uuid);

        match self.economy_service.transfer_money(
            stg_domain::PlayerId(from_uuid),
            stg_domain::PlayerId(to_uuid),
            &req.currency_code,
            req.amount_minor,
            request_id,
            context.server_id,
            correlation_id,
        ).await {
            Ok(tx) => {
                self.metrics.transactions_processed.fetch_add(1, Ordering::Relaxed);
                tracing::info!(request_id = %ctx.request_id, tx_id = %tx.id.0, from = %from_uuid, to = %to_uuid, amount = req.amount_minor, duration_ms = ctx.elapsed_ms(), "Transfer completed");
                Ok(Response::new(TransactionResult {
                    result: Some(stg_proto::stg::v1::transaction_result::Result::Transaction(
                        stg_proto::stg::v1::EconomyTransaction {
                            transaction_id: tx.id.0.to_string(),
                            r#type: tx.tx_type as i32,
                            status: tx.status as i32,
                            currency_code: tx.currency_code,
                            amount_minor: req.amount_minor,
                            source_account_id: Some(req.source_player_uuid),
                            destination_account_id: Some(req.destination_player_uuid),
                            initiating_server_id: tx.initiating_server,
                            request_id: tx.idempotency_key.to_string(),
                            metadata: HashMap::new(),
                            created_at: Some(prost_types::Timestamp {
                                seconds: tx.created_at.timestamp(),
                                nanos: tx.created_at.timestamp_subsec_nanos() as i32,
                            }),
                            committed_at: tx.committed_at.map(|c| prost_types::Timestamp {
                                seconds: c.timestamp(),
                                nanos: c.timestamp_subsec_nanos() as i32,
                            }),
                        },
                    )),
                }))
            }
            Err(e) => {
                self.metrics.transactions_failed.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(request_id = %ctx.request_id, error = %e, "Transfer failed");
                Ok(Response::new(TransactionResult {
                    result: Some(stg_proto::stg::v1::transaction_result::Result::Error(
                        stg_proto::stg::v1::Error {
                            code: stg_proto::stg::v1::ErrorCode::InsufficientFunds as i32,
                            message: e.to_string(),
                        },
                    )),
                }))
            }
        }
    }

    // -------------------------------------------------------------
    // prepare_resource_conversion
    // -------------------------------------------------------------

    async fn prepare_resource_conversion(
        &self,
        request: Request<PrepareResourceConversionRequest>,
    ) -> Result<Response<PrepareResourceConversionResponse>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let ctx = RequestContext::new(self.server_id.clone());
        let locale = extract_locale(&request);
        let req = request.into_inner();
        let player_uuid = Uuid::parse_str(&req.player_uuid).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.player_uuid.invalid"), locale),
            )
        })?;
        let resource = req.resource
            .ok_or_else(|| Status::invalid_argument("Missing resource"))?;
        let direction = match req.direction {
            x if x == stg_proto::stg::v1::ConversionDirection::ResourceToCurrency as i32 => {
                ConversionDirection::ResourceToCurrency
            }
            x if x == stg_proto::stg::v1::ConversionDirection::CurrencyToResource as i32 => {
                ConversionDirection::CurrencyToResource
            }
            _ => ConversionDirection::Unspecified,
        };
        let context = req.context
            .ok_or_else(|| Status::invalid_argument("Missing RequestContext"))?;
        let request_id = Uuid::parse_str(&context.request_id)
            .map_err(|_| Status::invalid_argument("Invalid request ID"))?;
        let correlation_id = Uuid::parse_str(&context.correlation_id)
            .map_err(|_| Status::invalid_argument("Invalid correlation ID"))?;

        match self.conversion_service.prepare_conversion(
            PlayerId(player_uuid),
            &resource.namespace,
            &resource.path,
            req.resource_amount,
            direction,
            request_id,
            &context.server_id,
            correlation_id,
        ).await {
            Ok(reservation) => {
                tracing::info!(request_id = %ctx.request_id, player_id = %player_uuid, duration_ms = ctx.elapsed_ms(), "PrepareResourceConversion");
                let pb_mutations: Vec<stg_proto::stg::v1::ResourceMutation> = reservation
                    .required_mutations
                    .into_iter()
                    .map(|m| stg_proto::stg::v1::ResourceMutation {
                        resource: Some(stg_proto::stg::v1::ResourceRef {
                            namespace: m.resource.namespace,
                            path: m.resource.path,
                        }),
                        amount_delta: m.amount_delta,
                    })
                    .collect();
                let pb_reservation = stg_proto::stg::v1::ResourceConversionReservation {
                    reservation_id: reservation.id.0.to_string(),
                    player_uuid: reservation.player_id.0.to_string(),
                    quote: Some(stg_proto::stg::v1::ConversionQuote {
                        quote_id: reservation.quote.quote_id,
                        resource: Some(stg_proto::stg::v1::ResourceRef {
                            namespace: reservation.quote.resource.namespace,
                            path: reservation.quote.resource.path,
                        }),
                        resource_amount: reservation.quote.resource_amount,
                        currency_code: reservation.quote.currency_code,
                        unit_price_minor: reservation.quote.unit_price_minor,
                        total_price_minor: reservation.quote.total_price_minor,
                        pricing_revision: reservation.quote.pricing_revision,
                        expires_at: Some(prost_types::Timestamp {
                            seconds: reservation.quote.expires_at.timestamp(),
                            nanos: reservation.quote.expires_at.timestamp_subsec_nanos() as i32,
                        }),
                    }),
                    required_mutations: pb_mutations,
                    expires_at: Some(prost_types::Timestamp {
                        seconds: reservation.quote.expires_at.timestamp(),
                        nanos: reservation.quote.expires_at.timestamp_subsec_nanos() as i32,
                    }),
                };
                Ok(Response::new(PrepareResourceConversionResponse {
                    result: Some(stg_proto::stg::v1::prepare_resource_conversion_response::Result::Reservation(pb_reservation)),
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(PrepareResourceConversionResponse {
                    result: Some(stg_proto::stg::v1::prepare_resource_conversion_response::Result::Error(
                        stg_proto::stg::v1::Error {
                            code: stg_proto::stg::v1::ErrorCode::ConversionRuleNotFound as i32,
                            message: e.to_string(),
                        },
                    )),
                }))
            }
        }
    }

    // -------------------------------------------------------------
    // commit_resource_conversion
    // -------------------------------------------------------------

    async fn commit_resource_conversion(
        &self,
        request: Request<CommitResourceConversionRequest>,
    ) -> Result<Response<TransactionResult>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let req = request.into_inner();
        let reservation_uuid = Uuid::parse_str(&req.reservation_id).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.reservation_id.invalid"), locale),
            )
        })?;
        let context = req.context
            .ok_or_else(|| Status::invalid_argument("Missing RequestContext"))?;
        let request_id = Uuid::parse_str(&context.request_id)
            .map_err(|_| Status::invalid_argument("Invalid request ID"))?;
        let correlation_id = Uuid::parse_str(&context.correlation_id)
            .map_err(|_| Status::invalid_argument("Invalid correlation ID"))?;

        match self.conversion_service.commit_conversion(
            request_id,
            ReservationId(reservation_uuid),
            &context.server_id,
            correlation_id,
        ).await {
            Ok(tx) => {
                self.metrics.transactions_processed.fetch_add(1, Ordering::Relaxed);
                tracing::info!(request_id = %ctx.request_id, tx_id = %tx.id.0, duration_ms = ctx.elapsed_ms(), "CommitResourceConversion");
                Ok(Response::new(TransactionResult {
                    result: Some(stg_proto::stg::v1::transaction_result::Result::Transaction(
                        stg_proto::stg::v1::EconomyTransaction {
                            transaction_id: tx.id.0.to_string(),
                            r#type: tx.tx_type as i32,
                            status: tx.status as i32,
                            currency_code: tx.currency_code,
                            amount_minor: 0,
                            source_account_id: None,
                            destination_account_id: None,
                            initiating_server_id: tx.initiating_server,
                            request_id: tx.idempotency_key.to_string(),
                            metadata: HashMap::new(),
                            created_at: Some(prost_types::Timestamp {
                                seconds: tx.created_at.timestamp(),
                                nanos: tx.created_at.timestamp_subsec_nanos() as i32,
                            }),
                            committed_at: tx.committed_at.map(|c| prost_types::Timestamp {
                                seconds: c.timestamp(),
                                nanos: c.timestamp_subsec_nanos() as i32,
                            }),
                        },
                    )),
                }))
            }
            Err(e) => {
                self.metrics.transactions_failed.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(TransactionResult {
                    result: Some(stg_proto::stg::v1::transaction_result::Result::Error(
                        stg_proto::stg::v1::Error {
                            code: stg_proto::stg::v1::ErrorCode::InternalError as i32,
                            message: e.to_string(),
                        },
                    )),
                }))
            }
        }
    }

    // -------------------------------------------------------------
    // abort_resource_conversion
    // -------------------------------------------------------------

    async fn abort_resource_conversion(
        &self,
        request: Request<AbortResourceConversionRequest>,
    ) -> Result<Response<AbortResourceConversionResponse>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let req = request.into_inner();
        let reservation_uuid = Uuid::parse_str(&req.reservation_id).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.reservation_id.invalid"), locale),
            )
        })?;
        match self.conversion_service.abort_conversion(ReservationId(reservation_uuid)).await {
            Ok(_) => {
                tracing::info!(request_id = %ctx.request_id, reservation_id = %reservation_uuid, duration_ms = ctx.elapsed_ms(), "AbortResourceConversion");
                Ok(Response::new(AbortResourceConversionResponse {
                    aborted: true,
                    error: None,
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(AbortResourceConversionResponse {
                    aborted: false,
                    error: Some(stg_proto::stg::v1::Error {
                        code: stg_proto::stg::v1::ErrorCode::InternalError as i32,
                        message: e.to_string(),
                    }),
                }))
            }
        }
    }

    // -------------------------------------------------------------
    // register_energy_node
    // -------------------------------------------------------------

    async fn register_energy_node(
        &self,
        request: Request<RegisterEnergyNodeRequest>,
    ) -> Result<Response<EnergyNode>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let ctx = RequestContext::new(self.server_id.clone());
        let locale = extract_locale(&request);
        let req = request.into_inner();
        let node_type = match req.r#type {
            x if x == stg_proto::stg::v1::EnergyNodeType::EnergyProducer as i32 => {
                stg_domain::EnergyNodeType::Producer
            }
            x if x == stg_proto::stg::v1::EnergyNodeType::EnergyConsumer as i32 => {
                stg_domain::EnergyNodeType::Consumer
            }
            x if x == stg_proto::stg::v1::EnergyNodeType::EnergyStorage as i32 => {
                stg_domain::EnergyNodeType::Storage
            }
            _ => stg_domain::EnergyNodeType::Hybrid,
        };
        let node = stg_domain::EnergyNode {
            id: stg_domain::NodeId(Uuid::new_v4()),
            node_type,
            server_id: self.server_id.clone(),
            region_id: req.region_id.clone(),
            display_name: req.display_name.clone(),
            enabled: true,
            capacity_watts: req.capacity_watts,
            production_watts: 0,
            consumption_watts: 0,
            stored_wh: 0,
            max_stored_wh: req.max_stored_wh,
            efficiency: 1.0,
            health: 1.0,
            revision: 0,
            last_reported_at: chrono::Utc::now(),
        };
        match self.energy_service.register_node(node).await {
            Ok(()) => {
                tracing::info!(request_id = %ctx.request_id, duration_ms = ctx.elapsed_ms(), "RegisterEnergyNode");
                Ok(Response::new(EnergyNode {
                    node_id: Uuid::new_v4().to_string(),
                    r#type: req.r#type,
                    server_id: self.server_id.clone(),
                    region_id: req.region_id.clone(),
                    display_name: req.display_name.clone(),
                    enabled: true,
                    capacity_watts: req.capacity_watts,
                    production_watts: 0,
                    consumption_watts: 0,
                    stored_wh: 0,
                    max_stored_wh: req.max_stored_wh,
                    efficiency: 1.0,
                    health: 1.0,
                    revision: 0,
                    last_reported_at: Some(prost_types::Timestamp {
                        seconds: chrono::Utc::now().timestamp(),
                        nanos: 0,
                    }),
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Err(map_domain_error_localized(&e, &*self.l10n, locale))
            }
        }
    }

    // -------------------------------------------------------------
    // report_energy_observation
    // -------------------------------------------------------------

    async fn report_energy_observation(
        &self,
        request: Request<ReportEnergyObservationRequest>,
    ) -> Result<Response<ReportEnergyObservationResponse>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let req = request.into_inner();
        let obs = req.observation.ok_or_else(|| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.observation.missing"), locale),
            )
        })?;
        let node_uuid = Uuid::parse_str(&obs.node_id).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.node_id.invalid"), locale),
            )
        })?;
        match self.energy_service.report_observation(
            node_uuid,
            obs.observed_production_watts,
            obs.observed_consumption_watts,
            obs.observed_storage_wh,
        ).await {
            Ok(()) => {
                tracing::info!(request_id = %ctx.request_id, node_id = %node_uuid, duration_ms = ctx.elapsed_ms(), "ReportEnergyObservation");
                Ok(Response::new(ReportEnergyObservationResponse {
                    result: Some(stg_proto::stg::v1::report_energy_observation_response::Result::Node(
                        EnergyNode {
                            node_id: node_uuid.to_string(),
                            r#type: stg_proto::stg::v1::EnergyNodeType::EnergyProducer as i32,
                            server_id: self.server_id.clone(),
                            region_id: "".to_string(),
                            display_name: "".to_string(),
                            enabled: true,
                            capacity_watts: obs.observed_production_watts,
                            production_watts: obs.observed_production_watts,
                            consumption_watts: obs.observed_consumption_watts,
                            stored_wh: obs.observed_storage_wh,
                            max_stored_wh: 0,
                            efficiency: 1.0,
                            health: 1.0,
                            revision: obs.expected_revision,
                            last_reported_at: Some(prost_types::Timestamp {
                                seconds: chrono::Utc::now().timestamp(),
                                nanos: 0,
                            }),
                        },
                    )),
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(ReportEnergyObservationResponse {
                    result: Some(stg_proto::stg::v1::report_energy_observation_response::Result::Error(
                        stg_proto::stg::v1::Error {
                            code: stg_proto::stg::v1::ErrorCode::InternalError as i32,
                            message: e.to_string(),
                        },
                    )),
                }))
            }
        }
    }

    // -------------------------------------------------------------
    // get_energy_state
    // -------------------------------------------------------------

    async fn get_energy_state(
        &self,
        request: Request<GetEnergyStateRequest>,
    ) -> Result<Response<EnergyState>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let _req = request.into_inner();
        match self.energy_service.get_state().await {
            Ok(state) => {
                tracing::info!(request_id = %ctx.request_id, tick = state.simulation_tick, duration_ms = ctx.elapsed_ms(), "GetEnergyState");
                Ok(Response::new(EnergyState {
                    total_production_watts: 0,
                    total_consumption_watts: 0,
                    global_reserve_wh: 0,
                    global_reserve_capacity_wh: 0,
                    unmet_demand_watts: 0,
                    efficiency: 1.0,
                    mode: state.mode as i32,
                    simulation_tick: state.simulation_tick,
                    revision: state.simulation_tick,
                    warnings: vec![],
                    updated_at: Some(prost_types::Timestamp {
                        seconds: chrono::Utc::now().timestamp(),
                        nanos: 0,
                    }),
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Err(map_domain_error_localized(&e, &*self.l10n, locale))
            }
        }
    }

    // -------------------------------------------------------------
    // begin_transition
    // -------------------------------------------------------------

    async fn begin_transition(
        &self,
        request: Request<BeginTransitionRequest>,
    ) -> Result<Response<BeginTransitionResponse>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let ctx = RequestContext::new(self.server_id.clone());
        let locale = extract_locale(&request);
        let req = request.into_inner();
        let player_uuid = Uuid::parse_str(&req.player_uuid).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.player_uuid.invalid"), locale),
            )
        })?;
        let context = req.context
            .ok_or_else(|| Status::invalid_argument("Missing RequestContext"))?;
        let target_server = req.target_server_id.clone();
        let format = req.payload_format.clone();
        let payload = req.opaque_payload.clone();

        match self.transition_service.begin_transition(
            player_uuid,
            target_server,
            format,
            req.payload_version,
            payload,
            &context.server_id,
        ).await {
            Ok(transition) => {
                tracing::info!(request_id = %ctx.request_id, transition_id = %transition.id.0, ticket = %transition.ticket, duration_ms = ctx.elapsed_ms(), "BeginTransition");
                Ok(Response::new(BeginTransitionResponse {
                    result: Some(stg_proto::stg::v1::begin_transition_response::Result::Transition(
                        stg_proto::stg::v1::PlayerTransition {
                            transition_id: transition.id.0.to_string(),
                            player_uuid: transition.player_id.0.to_string(),
                            source_server_id: self.server_id.clone(),
                            target_server_id: req.target_server_id,
                            status: stg_proto::stg::v1::TransitionStatus::TransitionPreparing as i32,
                            ticket: transition.ticket,
                            payload_format: req.payload_format,
                            payload_version: req.payload_version,
                            opaque_payload: req.opaque_payload,
                            created_at: Some(prost_types::Timestamp {
                                seconds: chrono::Utc::now().timestamp(),
                                nanos: 0,
                            }),
                            expires_at: Some(prost_types::Timestamp {
                                seconds: (chrono::Utc::now() + chrono::Duration::minutes(5)).timestamp(),
                                nanos: 0,
                            }),
                        },
                    )),
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(BeginTransitionResponse {
                    result: Some(stg_proto::stg::v1::begin_transition_response::Result::Error(
                        stg_proto::stg::v1::Error {
                            code: map_transition_error(&e) as i32,
                            message: e.to_string(),
                        },
                    )),
                }))
            }
        }
    }

    // -------------------------------------------------------------
    // claim_transition
    // -------------------------------------------------------------

    async fn claim_transition(
        &self,
        request: Request<ClaimTransitionRequest>,
    ) -> Result<Response<ClaimTransitionResponse>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let req = request.into_inner();
        match self.transition_service.claim_transition(&req.ticket).await {
            Ok(transition) => {
                tracing::info!(request_id = %ctx.request_id, ticket = %req.ticket, duration_ms = ctx.elapsed_ms(), "ClaimTransition");
                Ok(Response::new(ClaimTransitionResponse {
                    result: Some(stg_proto::stg::v1::claim_transition_response::Result::Transition(
                        stg_proto::stg::v1::PlayerTransition {
                            transition_id: transition.id.0.to_string(),
                            player_uuid: transition.player_id.0.to_string(),
                            source_server_id: self.server_id.clone(),
                            target_server_id: "".to_string(),
                            status: stg_proto::stg::v1::TransitionStatus::TransitionPreparing as i32,
                            ticket: transition.ticket,
                            payload_format: "".to_string(),
                            payload_version: 0,
                            opaque_payload: vec![],
                            created_at: Some(prost_types::Timestamp {
                                seconds: chrono::Utc::now().timestamp(),
                                nanos: 0,
                            }),
                            expires_at: None,
                        },
                    )),
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(ClaimTransitionResponse {
                    result: Some(stg_proto::stg::v1::claim_transition_response::Result::Error(
                        stg_proto::stg::v1::Error {
                            code: map_transition_error(&e) as i32,
                            message: e.to_string(),
                        },
                    )),
                }))
            }
        }
    }

    // -------------------------------------------------------------
    // commit_transition
    // -------------------------------------------------------------

    async fn commit_transition(
        &self,
        request: Request<CommitTransitionRequest>,
    ) -> Result<Response<CommitTransitionResponse>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let req = request.into_inner();
        let transition_uuid = Uuid::parse_str(&req.transition_id).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.transition_id.invalid"), locale),
            )
        })?;
        match self.transition_service.commit_transition(transition_uuid).await {
            Ok(transition) => {
                tracing::info!(request_id = %ctx.request_id, transition_id = %transition.id.0, duration_ms = ctx.elapsed_ms(), "CommitTransition");
                Ok(Response::new(CommitTransitionResponse {
                    result: Some(stg_proto::stg::v1::commit_transition_response::Result::Transition(
                        stg_proto::stg::v1::PlayerTransition {
                            transition_id: transition.id.0.to_string(),
                            player_uuid: transition.player_id.0.to_string(),
                            source_server_id: self.server_id.clone(),
                            target_server_id: "".to_string(),
                            status: stg_proto::stg::v1::TransitionStatus::TransitionCommitted as i32,
                            ticket: transition.ticket,
                            payload_format: "".to_string(),
                            payload_version: 0,
                            opaque_payload: vec![],
                            created_at: Some(prost_types::Timestamp {
                                seconds: chrono::Utc::now().timestamp(),
                                nanos: 0,
                            }),
                            expires_at: None,
                        },
                    )),
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(CommitTransitionResponse {
                    result: Some(stg_proto::stg::v1::commit_transition_response::Result::Error(
                        stg_proto::stg::v1::Error {
                            code: map_transition_error(&e) as i32,
                            message: e.to_string(),
                        },
                    )),
                }))
            }
        }
    }

    // -------------------------------------------------------------
    // abort_transition
    // -------------------------------------------------------------

    async fn abort_transition(
        &self,
        request: Request<AbortTransitionRequest>,
    ) -> Result<Response<AbortTransitionResponse>, Status> {
        self.metrics.rpc_requests.fetch_add(1, Ordering::Relaxed);
        let locale = extract_locale(&request);
        let ctx = RequestContext::new(self.server_id.clone());
        let req = request.into_inner();
        let transition_uuid = Uuid::parse_str(&req.transition_id).map_err(|_| {
            Status::invalid_argument(
                self.l10n.localize(&MessageKey::new("validation.transition_id.invalid"), locale),
            )
        })?;
        match self.transition_service.abort_transition(transition_uuid, &req.reason).await {
            Ok(transition) => {
                tracing::info!(request_id = %ctx.request_id, transition_id = %transition.id.0, reason = %req.reason, duration_ms = ctx.elapsed_ms(), "AbortTransition");
                Ok(Response::new(AbortTransitionResponse {
                    result: Some(stg_proto::stg::v1::abort_transition_response::Result::Transition(
                        stg_proto::stg::v1::PlayerTransition {
                            transition_id: transition.id.0.to_string(),
                            player_uuid: transition.player_id.0.to_string(),
                            source_server_id: self.server_id.clone(),
                            target_server_id: "".to_string(),
                            status: stg_proto::stg::v1::TransitionStatus::TransitionAborted as i32,
                            ticket: transition.ticket,
                            payload_format: "".to_string(),
                            payload_version: 0,
                            opaque_payload: vec![],
                            created_at: Some(prost_types::Timestamp {
                                seconds: chrono::Utc::now().timestamp(),
                                nanos: 0,
                            }),
                            expires_at: None,
                        },
                    )),
                }))
            }
            Err(e) => {
                self.metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
                Ok(Response::new(AbortTransitionResponse {
                    result: Some(stg_proto::stg::v1::abort_transition_response::Result::Error(
                        stg_proto::stg::v1::Error {
                            code: map_transition_error(&e) as i32,
                            message: e.to_string(),
                        },
                    )),
                }))
            }
        }
    }
}
