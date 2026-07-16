use stg_proto::stg::v1::{
    stg_backend_server::StgBackend,
    BeginTransitionRequest, BeginTransitionResponse, ClaimTransitionRequest,
    ClaimTransitionResponse, CommitResourceConversionRequest, CommitTransitionRequest,
    CommitTransitionResponse, AbortTransitionRequest, AbortTransitionResponse,
    EnergyNode, EnergyState, EconomyState, PlayerSnapshot,
    PrepareResourceConversionRequest, PrepareResourceConversionResponse,
    RegisterEnergyNodeRequest, RegisterPlayerRequest, RegisterPlayerResponse,
    ReportEnergyObservationRequest, ReportEnergyObservationResponse,
    TransactionResult, TransferMoneyRequest, WorldSnapshot,
    GetEnergyStateRequest, GetEconomyStateRequest, GetPlayerSnapshotRequest, GetWorldSnapshotRequest,
    AbortResourceConversionRequest, AbortResourceConversionResponse,
    prepare_resource_conversion_response, transaction_result, BackendMessage, ServerMessage, Error as ProtoError, ErrorCode,
};
use tonic::{Request, Response, Status, Streaming};
use stg_application::{EconomyService, ConversionService, PlayerService, EnergyService};
use stg_domain::{PlayerId, ConversionDirection};
use uuid::Uuid;
use std::sync::Arc;
use crate::grpc::auth::AuthenticatedServer;
use crate::grpc::error::map_domain_error;

pub struct StgGrpcServer {
    pub player_service: Arc<PlayerService>,
    pub economy_service: Arc<EconomyService>,
    pub conversion_service: Arc<ConversionService>,
    pub energy_service: Arc<EnergyService>,
}

fn extract_server_id<T>(req: &Request<T>) -> Result<String, Status> {
    req.extensions()
        .get::<AuthenticatedServer>()
        .map(|s| s.0.clone())
        .ok_or_else(|| Status::unauthenticated("Missing authenticated server identity"))
}

fn parse_uuid(id: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(id).map_err(|_| Status::invalid_argument("Invalid UUID format"))
}

#[tonic::async_trait]
impl StgBackend for StgGrpcServer {
    type ConnectStream = std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<BackendMessage, Status>> + Send + 'static>>;

    async fn connect(
        &self,
        _request: Request<Streaming<ServerMessage>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        Err(Status::unimplemented("Connect realtime streaming not implemented"))
    }

    async fn register_player(
        &self,
        request: Request<RegisterPlayerRequest>,
    ) -> Result<Response<RegisterPlayerResponse>, Status> {
        let _server_id = extract_server_id(&request)?;

        let req = request.into_inner();
        let _ctx = req.context.ok_or_else(|| Status::invalid_argument("Missing context"))?;
        
        let player_uuid = parse_uuid(&req.player_uuid)?;
        let correlation_id = parse_uuid(&_ctx.correlation_id)?;

        let player = self.player_service.register_player(player_uuid, req.username, correlation_id)
            .await
            .map_err(map_domain_error)?;

        Ok(Response::new(RegisterPlayerResponse {
            result: Some(stg_proto::stg::v1::register_player_response::Result::Player(stg_proto::stg::v1::PlayerSnapshot {
                identity: Some(stg_proto::stg::v1::PlayerIdentity {
                    uuid: player.id.0.to_string(),
                    username: player.username,
                    status: stg_proto::stg::v1::PlayerStatus::Active.into(), // simplified
                    created_at: Some(prost_types::Timestamp { seconds: player.created_at.timestamp(), nanos: player.created_at.timestamp_subsec_nanos() as i32 }),
                    last_seen_at: Some(prost_types::Timestamp { seconds: player.last_seen_at.timestamp(), nanos: player.last_seen_at.timestamp_subsec_nanos() as i32 }),
                }),
                wallets: vec![],
                global_reputation: 0,
                faction_reputation: std::collections::HashMap::new(),
                integer_stats: std::collections::HashMap::new(),
                string_stats: std::collections::HashMap::new(),
                current_server_id: Some("".to_string()),
                revision: 0,
                transition: None,
            })),
        }))
    }

    async fn get_player_snapshot(
        &self,
        request: Request<GetPlayerSnapshotRequest>,
    ) -> Result<Response<PlayerSnapshot>, Status> {
        let req = request.into_inner();
        let player_uuid = parse_uuid(&req.player_uuid)?;

        let (player, wallets) = self.player_service.get_player_snapshot(player_uuid).await.map_err(map_domain_error)?;

        let proto_identity = stg_proto::stg::v1::PlayerIdentity {
            uuid: player.id.0.to_string(),
            username: player.username,
            status: stg_proto::stg::v1::PlayerStatus::Active.into(), // simplified
            created_at: Some(prost_types::Timestamp { seconds: player.created_at.timestamp(), nanos: player.created_at.timestamp_subsec_nanos() as i32 }),
            last_seen_at: Some(prost_types::Timestamp { seconds: player.last_seen_at.timestamp(), nanos: player.last_seen_at.timestamp_subsec_nanos() as i32 }),
        };

        let proto_wallets = wallets.into_iter().map(|w| stg_proto::stg::v1::WalletBalance {
            currency_code: w.currency_code,
            amount_minor: w.balance.0,
            revision: w.revision,
        }).collect();

        Ok(Response::new(PlayerSnapshot {
            identity: Some(proto_identity),
            wallets: proto_wallets,
            global_reputation: 0,
            faction_reputation: std::collections::HashMap::new(),
            integer_stats: std::collections::HashMap::new(),
            string_stats: std::collections::HashMap::new(),
            current_server_id: Some("".to_string()),
            revision: 0,
            transition: None,
        }))
    }

    async fn transfer_money(
        &self,
        request: Request<TransferMoneyRequest>,
    ) -> Result<Response<TransactionResult>, Status> {
        let server_id = extract_server_id(&request)?;

        let req = request.into_inner();
        let ctx = req.context.ok_or_else(|| Status::invalid_argument("Missing context"))?;

        // verify context server_id matches authenticated server
        if ctx.server_id != server_id {
            return Err(Status::permission_denied("Server ID mismatch"));
        }

        let from_player = parse_uuid(&req.source_player_uuid)?;
        let to_player = parse_uuid(&req.destination_player_uuid)?;
        let idempotency_key = parse_uuid(&ctx.request_id)?;
        let correlation_id = parse_uuid(&ctx.correlation_id)?;

        let tx = self.economy_service.transfer_money(
            PlayerId(from_player),
            PlayerId(to_player),
            &req.currency_code,
            req.amount_minor,
            idempotency_key,
            server_id.clone(),
            correlation_id,
        ).await.map_err(map_domain_error)?;

        let (created_at, committed_at) = (
            Some(prost_types::Timestamp { seconds: tx.created_at.timestamp(), nanos: tx.created_at.timestamp_subsec_nanos() as i32 }),
            tx.committed_at.map(|t| prost_types::Timestamp { seconds: t.timestamp(), nanos: t.timestamp_subsec_nanos() as i32 }),
        );

        Ok(Response::new(TransactionResult {
            result: Some(transaction_result::Result::Transaction(stg_proto::stg::v1::EconomyTransaction {
                transaction_id: tx.id.0.to_string(),
                r#type: stg_proto::stg::v1::EconomyTransactionType::PlayerTransfer.into(),
                status: stg_proto::stg::v1::EconomyTransactionStatus::TransactionCommitted.into(),
                currency_code: req.currency_code,
                amount_minor: req.amount_minor,
                source_account_id: Some(req.source_player_uuid),
                destination_account_id: Some(req.destination_player_uuid),
                initiating_server_id: server_id,
                request_id: ctx.request_id,
                metadata: std::collections::HashMap::new(),
                created_at,
                committed_at,
            }))
        }))
    }

    async fn prepare_resource_conversion(
        &self,
        request: Request<PrepareResourceConversionRequest>,
    ) -> Result<Response<PrepareResourceConversionResponse>, Status> {
        let server_id = extract_server_id(&request)?;

        let req = request.into_inner();
        let ctx = req.context.as_ref().ok_or_else(|| Status::invalid_argument("Missing context"))?;

        if ctx.server_id != server_id {
            return Err(Status::permission_denied("Server ID mismatch"));
        }

        let player_id = parse_uuid(&req.player_uuid)?;
        let idempotency_key = parse_uuid(&ctx.request_id)?;
        let correlation_id = parse_uuid(&ctx.correlation_id)?;
        let resource = req.resource.ok_or_else(|| Status::invalid_argument("Missing resource"))?;

        let direction = match stg_proto::stg::v1::ConversionDirection::try_from(req.direction) {
            Ok(stg_proto::stg::v1::ConversionDirection::ResourceToCurrency) => ConversionDirection::ResourceToCurrency,
            Ok(stg_proto::stg::v1::ConversionDirection::CurrencyToResource) => ConversionDirection::CurrencyToResource,
            _ => return Err(Status::invalid_argument("Invalid direction")),
        };

        match self.conversion_service.prepare_conversion(
            PlayerId(player_id),
            &resource.namespace,
            &resource.path,
            req.resource_amount,
            direction,
            idempotency_key,
            &server_id,
            correlation_id,
        ).await {
            Ok(res) => {
                let quote = res.quote;
                let reservation = stg_proto::stg::v1::ResourceConversionReservation {
                    reservation_id: res.id.0.to_string(),
                    player_uuid: req.player_uuid,
                    quote: Some(stg_proto::stg::v1::ConversionQuote {
                        quote_id: quote.quote_id,
                        resource: Some(stg_proto::stg::v1::ResourceRef { namespace: quote.resource.namespace, path: quote.resource.path }),
                        resource_amount: quote.resource_amount,
                        currency_code: quote.currency_code,
                        unit_price_minor: quote.unit_price_minor,
                        total_price_minor: quote.total_price_minor,
                        pricing_revision: quote.pricing_revision,
                        expires_at: Some(prost_types::Timestamp { seconds: quote.expires_at.timestamp(), nanos: quote.expires_at.timestamp_subsec_nanos() as i32 }),
                    }),
                    required_mutations: res.required_mutations.into_iter().map(|m| stg_proto::stg::v1::ResourceMutation {
                        resource: Some(stg_proto::stg::v1::ResourceRef { namespace: m.resource.namespace, path: m.resource.path }),
                        amount_delta: m.amount_delta,
                    }).collect(),
                    expires_at: Some(prost_types::Timestamp { seconds: res.expires_at.timestamp(), nanos: res.expires_at.timestamp_subsec_nanos() as i32 }),
                };

                Ok(Response::new(PrepareResourceConversionResponse {
                    result: Some(prepare_resource_conversion_response::Result::Reservation(reservation)),
                }))
            }
            Err(e) => {
                let status = map_domain_error(e);
                Ok(Response::new(PrepareResourceConversionResponse {
                    result: Some(prepare_resource_conversion_response::Result::Error(ProtoError {
                        code: ErrorCode::InternalError.into(),
                        message: status.message().to_string(),
                    }))
                }))
            }
        }
    }

    async fn commit_resource_conversion(
        &self,
        request: Request<CommitResourceConversionRequest>,
    ) -> Result<Response<TransactionResult>, Status> {
        let server_id = extract_server_id(&request)?;

        let req = request.into_inner();
        let ctx = req.context.as_ref().ok_or_else(|| Status::invalid_argument("Missing context"))?;

        if ctx.server_id != server_id {
            return Err(Status::permission_denied("Server ID mismatch"));
        }

        let idempotency_key = parse_uuid(&ctx.request_id)?;
        let correlation_id = parse_uuid(&ctx.correlation_id)?;
        let reservation_id = parse_uuid(&req.reservation_id)?;

        let tx = self.conversion_service.commit_conversion(
            idempotency_key,
            stg_domain::ReservationId(reservation_id),
            &server_id,
            correlation_id,
        ).await.map_err(map_domain_error)?;

        let (created_at, committed_at) = (
            Some(prost_types::Timestamp { seconds: tx.created_at.timestamp(), nanos: tx.created_at.timestamp_subsec_nanos() as i32 }),
            tx.committed_at.map(|t| prost_types::Timestamp { seconds: t.timestamp(), nanos: t.timestamp_subsec_nanos() as i32 }),
        );

        // Find the amount in ledger entries
        let amount_minor = tx.entries.iter().filter(|e| e.amount_delta > 0).map(|e| e.amount_delta).sum::<i64>();

        Ok(Response::new(TransactionResult {
            result: Some(transaction_result::Result::Transaction(stg_proto::stg::v1::EconomyTransaction {
                transaction_id: tx.id.0.to_string(),
                r#type: stg_proto::stg::v1::EconomyTransactionType::ResourceConversion.into(),
                status: stg_proto::stg::v1::EconomyTransactionStatus::TransactionCommitted.into(),
                currency_code: tx.currency_code.clone(),
                amount_minor,
                source_account_id: None,
                destination_account_id: None,
                initiating_server_id: server_id,
                request_id: ctx.request_id.clone(),
                metadata: std::collections::HashMap::new(),
                created_at,
                committed_at,
            }))
        }))
    }

    async fn abort_resource_conversion(
        &self,
        request: Request<AbortResourceConversionRequest>,
    ) -> Result<Response<AbortResourceConversionResponse>, Status> {
        let server_id = extract_server_id(&request)?;

        let req = request.into_inner();
        let ctx = req.context.as_ref().ok_or_else(|| Status::invalid_argument("Missing context"))?;

        if ctx.server_id != server_id {
            return Err(Status::permission_denied("Server ID mismatch"));
        }

        let reservation_id = parse_uuid(&req.reservation_id)?;

        match self.conversion_service.abort_conversion(stg_domain::ReservationId(reservation_id)).await {
            Ok(_) => Ok(Response::new(AbortResourceConversionResponse {
                aborted: true,
                error: None,
            })),
            Err(e) => {
                let status = map_domain_error(e);
                Ok(Response::new(AbortResourceConversionResponse {
                    aborted: false,
                    error: Some(ProtoError {
                        code: ErrorCode::InternalError.into(),
                        message: status.message().to_string(),
                    }),
                }))
            }
        }
    }

    async fn register_energy_node(
        &self,
        request: Request<RegisterEnergyNodeRequest>,
    ) -> Result<Response<EnergyNode>, Status> {
        let server_id = extract_server_id(&request)?;
        let req = request.into_inner();
        
        let node_type = match stg_proto::stg::v1::EnergyNodeType::try_from(req.r#type) {
            Ok(stg_proto::stg::v1::EnergyNodeType::EnergyProducer) => stg_domain::EnergyNodeType::Producer,
            Ok(stg_proto::stg::v1::EnergyNodeType::EnergyConsumer) => stg_domain::EnergyNodeType::Consumer,
            Ok(stg_proto::stg::v1::EnergyNodeType::EnergyStorage) => stg_domain::EnergyNodeType::Storage,
            Ok(stg_proto::stg::v1::EnergyNodeType::EnergyHybrid) => stg_domain::EnergyNodeType::Hybrid,
            _ => return Err(Status::invalid_argument("Invalid node type")),
        };

        let node = stg_domain::EnergyNode {
            id: stg_domain::NodeId(Uuid::new_v4()),
            node_type,
            server_id: server_id.clone(),
            region_id: req.region_id,
            display_name: req.display_name,
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

        self.energy_service.register_node(node.clone()).await.map_err(map_domain_error)?;

        Ok(Response::new(EnergyNode {
            node_id: node.id.0.to_string(),
            r#type: req.r#type,
            server_id: node.server_id,
            region_id: node.region_id,
            display_name: node.display_name,
            enabled: node.enabled,
            capacity_watts: node.capacity_watts,
            production_watts: node.production_watts,
            consumption_watts: node.consumption_watts,
            stored_wh: node.stored_wh,
            max_stored_wh: node.max_stored_wh,
            efficiency: node.efficiency,
            health: node.health,
            revision: node.revision,
            last_reported_at: Some(prost_types::Timestamp { seconds: node.last_reported_at.timestamp(), nanos: node.last_reported_at.timestamp_subsec_nanos() as i32 }),
        }))
    }

    async fn report_energy_observation(
        &self,
        request: Request<ReportEnergyObservationRequest>,
    ) -> Result<Response<ReportEnergyObservationResponse>, Status> {
        let _server_id = extract_server_id(&request)?;
        let req = request.into_inner();
        let obs = req.observation.ok_or_else(|| Status::invalid_argument("Missing observation"))?;
        let node_id = parse_uuid(&obs.node_id)?;

        self.energy_service.report_observation(node_id, obs.observed_production_watts, obs.observed_consumption_watts, obs.observed_storage_wh)
            .await
            .map_err(map_domain_error)?;

        Ok(Response::new(ReportEnergyObservationResponse {
            result: None
        }))
    }

    async fn get_energy_state(
        &self,
        _request: Request<GetEnergyStateRequest>,
    ) -> Result<Response<EnergyState>, Status> {
        let state = self.energy_service.get_state().await.map_err(map_domain_error)?;
        
        let mode = match state.mode {
            stg_domain::EnergyMode::Normal => stg_proto::stg::v1::EnergyMode::EnergyNormal,
            stg_domain::EnergyMode::Surplus => stg_proto::stg::v1::EnergyMode::EnergySurplus,
            stg_domain::EnergyMode::Deficit => stg_proto::stg::v1::EnergyMode::EnergyDeficit,
            stg_domain::EnergyMode::Critical => stg_proto::stg::v1::EnergyMode::EnergyCritical,
            stg_domain::EnergyMode::Collapse => stg_proto::stg::v1::EnergyMode::EnergyCollapse,
        };

        Ok(Response::new(EnergyState {
            total_production_watts: 0,
            total_consumption_watts: 0,
            global_reserve_wh: 0,
            global_reserve_capacity_wh: 0,
            unmet_demand_watts: 0,
            efficiency: 1.0,
            mode: mode.into(),
            simulation_tick: state.simulation_tick,
            revision: 0,
            warnings: vec![],
            updated_at: Some(prost_types::Timestamp { seconds: chrono::Utc::now().timestamp(), nanos: 0 }),
        }))
    }

    async fn get_economy_state(
        &self,
        _request: Request<GetEconomyStateRequest>,
    ) -> Result<Response<EconomyState>, Status> {
        Ok(Response::new(EconomyState {
            currencies: vec![],
            energy_mode: stg_proto::stg::v1::EnergyMode::EnergyNormal.into(),
            pricing_revision: 0,
            updated_at: Some(prost_types::Timestamp { seconds: chrono::Utc::now().timestamp(), nanos: 0 }),
        }))
    }

    async fn get_world_snapshot(
        &self,
        _request: Request<GetWorldSnapshotRequest>,
    ) -> Result<Response<WorldSnapshot>, Status> {
        Ok(Response::new(WorldSnapshot {
            world_id: "stg-main".to_string(),
            mode: stg_proto::stg::v1::WorldMode::WorldStable.into(),
            energy: None,
            economy: None,
            active_events: vec![],
            simulation_tick: 0,
            revision: 0,
            updated_at: Some(prost_types::Timestamp { seconds: chrono::Utc::now().timestamp(), nanos: 0 }),
        }))
    }

    async fn begin_transition(
        &self,
        _request: Request<BeginTransitionRequest>,
    ) -> Result<Response<BeginTransitionResponse>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn claim_transition(
        &self,
        _request: Request<ClaimTransitionRequest>,
    ) -> Result<Response<ClaimTransitionResponse>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn commit_transition(
        &self,
        _request: Request<CommitTransitionRequest>,
    ) -> Result<Response<CommitTransitionResponse>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn abort_transition(
        &self,
        _request: Request<AbortTransitionRequest>,
    ) -> Result<Response<AbortTransitionResponse>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}
