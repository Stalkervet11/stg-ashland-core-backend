use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use sqlx::postgres::PgPoolOptions;
use stg-application::{
    EconomyService, EnergySimulationService, PlayerService,
};
use stg-infrastructure::{
    MockQueuePublisher, PostgresPlayerRepository, PostgresTransactionRepository,
    PostgresWalletRepository, POSTGRES_DDL_SCHEMA,
};
use uuid::Uuid;

// Import generated tonic server trait and types
use stg-proto::v1::stg_backend_server::{StgBackend, StgBackendServer};
use stg-proto::v1::{
    GetPlayerSnapshotRequest, GetWorldSnapshotRequest, PlayerSnapshot, RegisterPlayerRequest,
    RegisterPlayerResponse, TransferMoneyRequest, TransactionResult, WorldSnapshot,
};

// =========================================================================
// GRPC SERVICE IMPLEMENTATION (CONTROLLER ROUTER)
// =========================================================================

pub struct StgBackendImpl {
    player_service: Arc<PlayerService>,
    economy_service: Arc<EconomyService>,
}

#[tonic::async_trait]
impl StgBackend for StgBackendImpl {
    type ConnectStream = tokio_stream::wrappers::ReceiverStream<Result<stg-proto::v1::BackendMessage, tonic::Status>>;

    async fn Connect(
        &self,
        _request: tonic::Request<tonic::Streaming<stg-proto::v1::ServerMessage>>,
    ) -> Result<tonic::Response<Self::ConnectStream>, tonic::Status> {
        info!("STG-Bridge: New bidirectional server connection initialized.");
        // Bidirectional stream setup would reside here
        Err(tonic::Status::unimplemented("Connect is fully defined in proto, mock streaming active in panel"))
    }

    async fn RegisterPlayer(
        &self,
        request: tonic::Request<RegisterPlayerRequest>,
    ) -> Result<tonic::Response<RegisterPlayerResponse>, tonic::Status> {
        let req = request.into_inner();
        let player_uuid = Uuid::parse_str(&req.player_uuid)
            .map_err(|_| tonic::Status::invalid_argument("Invalid player UUID format"))?;

        let correlation_id = Uuid::new_v4();

        match self.player_service.register_player(player_uuid, req.username, correlation_id).await {
            Ok(p) => {
                let response = RegisterPlayerResponse {
                    result: Some(stg-proto::v1::register_player_response::Result::Player(
                        PlayerSnapshot {
                            identity: Some(stg-proto::v1::PlayerIdentity {
                                uuid: p.id.0.to_string(),
                                username: p.username,
                                status: stg-proto::v1::PlayerStatus::PlayerActive as i32,
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
                            faction_reputation: std::collections::HashMap::new(),
                            integer_stats: std::collections::HashMap::new(),
                            string_stats: std::collections::HashMap::new(),
                            current_server_id: None,
                            transition: None,
                            revision: p.revision,
                        },
                    )),
                };
                Ok(tonic::Response::new(response))
            }
            Err(e) => {
                let response = RegisterPlayerResponse {
                    result: Some(stg-proto::v1::register_player_response::Result::Error(
                        stg-proto::v1::Error {
                            code: stg-proto::v1::ErrorCode::InternalError as i32,
                            message: e.to_string(),
                        },
                    )),
                };
                Ok(tonic::Response::new(response))
            }
        }
    }

    async fn GetPlayerSnapshot(
        &self,
        _request: tonic::Request<GetPlayerSnapshotRequest>,
    ) -> Result<tonic::Response<PlayerSnapshot>, tonic::Status> {
        Err(tonic::Status::unimplemented("GetPlayerSnapshot"))
    }

    async fn GetWorldSnapshot(
        &self,
        _request: tonic::Request<GetWorldSnapshotRequest>,
    ) -> Result<tonic::Response<WorldSnapshot>, tonic::Status> {
        Err(tonic::Status::unimplemented("GetWorldSnapshot"))
    }

    async fn GetEconomyState(
        &self,
        _request: tonic::Request<stg-proto::v1::GetEconomyStateRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::EconomyState>, tonic::Status> {
        Err(tonic::Status::unimplemented("GetEconomyState"))
    }

    async fn TransferMoney(
        &self,
        request: tonic::Request<TransferMoneyRequest>,
    ) -> Result<tonic::Response<TransactionResult>, tonic::Status> {
        let req = request.into_inner();
        let from_uuid = Uuid::parse_str(&req.source_player_uuid)
            .map_err(|_| tonic::Status::invalid_argument("Invalid source player UUID"))?;
        let to_uuid = Uuid::parse_str(&req.destination_player_uuid)
            .map_err(|_| tonic::Status::invalid_argument("Invalid destination player UUID"))?;

        let context = req.context.ok_or_else(|| tonic::Status::invalid_argument("Missing RequestContext"))?;
        let request_id = Uuid::parse_str(&context.request_id)
            .map_err(|_| tonic::Status::invalid_argument("Invalid request ID UUID"))?;
        let correlation_id = Uuid::parse_str(&context.correlation_id)
            .map_err(|_| tonic::Status::invalid_argument("Invalid correlation ID UUID"))?;

        match self.economy_service.transfer_money(
            stg-domain::PlayerId(from_uuid),
            stg-domain::PlayerId(to_uuid),
            &req.currency_code,
            req.amount_minor,
            request_id,
            context.server_id,
            correlation_id,
        ).await {
            Ok(tx) => {
                let response = TransactionResult {
                    result: Some(stg-proto::v1::transaction_result::Result::Transaction(
                        stg-proto::v1::EconomyTransaction {
                            transaction_id: tx.id.0.to_string(),
                            type_code: tx.tx_type as i32,
                            status: tx.status as i32,
                            currency_code: tx.currency_code,
                            amount_minor: req.amount_minor,
                            source_account_id: None,
                            destination_account_id: None,
                            initiating_server_id: tx.initiating_server,
                            request_id: tx.idempotency_key.to_string(),
                            metadata: tx.metadata,
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
                };
                Ok(tonic::Response::new(response))
            }
            Err(e) => {
                let response = TransactionResult {
                    result: Some(stg-proto::v1::transaction_result::Result::Error(
                        stg-proto::v1::Error {
                            code: stg-proto::v1::ErrorCode::InsufficientFunds as i32,
                            message: e.to_string(),
                        },
                    )),
                };
                Ok(tonic::Response::new(response))
            }
        }
    }

    async fn PrepareResourceConversion(
        &self,
        _request: tonic::Request<stg-proto::v1::PrepareResourceConversionRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::PrepareResourceConversionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("PrepareResourceConversion"))
    }

    async fn CommitResourceConversion(
        &self,
        _request: tonic::Request<stg-proto::v1::CommitResourceConversionRequest>,
    ) -> Result<tonic::Response<TransactionResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("CommitResourceConversion"))
    }

    async fn AbortResourceConversion(
        &self,
        _request: tonic::Request<stg-proto::v1::AbortResourceConversionRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::AbortResourceConversionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("AbortResourceConversion"))
    }

    async fn RegisterEnergyNode(
        &self,
        _request: tonic::Request<stg-proto::v1::RegisterEnergyNodeRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::EnergyNode>, tonic::Status> {
        Err(tonic::Status::unimplemented("RegisterEnergyNode"))
    }

    async fn ReportEnergyObservation(
        &self,
        _request: tonic::Request<stg-proto::v1::ReportEnergyObservationRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::ReportEnergyObservationResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("ReportEnergyObservation"))
    }

    async fn GetEnergyState(
        &self,
        _request: tonic::Request<stg-proto::v1::GetEnergyStateRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::EnergyState>, tonic::Status> {
        Err(tonic::Status::unimplemented("GetEnergyState"))
    }

    async fn BeginTransition(
        &self,
        _request: tonic::Request<stg-proto::v1::BeginTransitionRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::BeginTransitionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("BeginTransition"))
    }

    async fn ClaimTransition(
        &self,
        _request: tonic::Request<stg-proto::v1::ClaimTransitionRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::ClaimTransitionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("ClaimTransition"))
    }

    async fn CommitTransition(
        &self,
        _request: tonic::Request<stg-proto::v1::CommitTransitionRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::CommitTransitionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("CommitTransition"))
    }

    async fn AbortTransition(
        &self,
        _request: tonic::Request<stg-proto::v1::AbortTransitionRequest>,
    ) -> Result<tonic::Response<stg-proto::v1::AbortTransitionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("AbortTransition"))
    }
}

// =========================================================================
// APPLICATION SERVER ENTRYPOINT
// =========================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize structured logging Subscriber
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("STG-Core starting up...");

    // 2. Load configuration from environment
    dotenv::dotenv().ok();
    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5432/stg_ashland".to_string()
    });
    let bind_addr: SocketAddr = std::env::var("GRPC_BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
        .parse()?;

    info!("DATABASE_URL: loaded successfully.");
    info!("GRPC BIND ADDR: {}", bind_addr);

    // 3. Establish SQLx PG Pool
    let pool = PgPoolOptions::new()
        .max_connections(25)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&db_url)
        .await?;

    info!("PostgreSQL connection established successfully.");

    // 4. Run database DDL bootstrap migrations
    sqlx::query(POSTGRES_DDL_SCHEMA)
        .execute(&pool)
        .await?;
    info!("PostgreSQL schema migrations applied successfully.");

    // 5. Instantiate Domain/Application services
    let player_repo = Box::new(PostgresPlayerRepository::new(pool.clone()));
    let wallet_repo = Box::new(PostgresWalletRepository::new(pool.clone()));
    let tx_repo = Box::new(PostgresTransactionRepository::new(pool.clone()));

    // Create shared event repositories
    // In production, we build postgres implementations
    let event_repo = Arc::new(stg-infrastructure::PostgresEventLogRepository::new(pool.clone()));
    let queue_pub = Arc::new(MockQueuePublisher::new());

    let player_service = Arc::new(PlayerService::new(
        player_repo,
        wallet_repo.clone(),
        event_repo.clone(),
        queue_pub.clone(),
    ));

    let economy_service = Arc::new(EconomyService::new(
        player_repo.clone(),
        wallet_repo.clone(),
        tx_repo,
        event_repo.clone(),
        queue_pub.clone(),
    ));

    let energy_repo = Arc::new(stg-infrastructure::PostgresEnergyRepository::new(pool.clone()));
    let energy_service = Arc::new(EnergySimulationService::new(
        energy_repo,
        event_repo.clone(),
        queue_pub.clone(),
    ));

    // 6. Schedule background global Energy Simulation loop
    let simulation_service = energy_service.clone();
    tokio::spawn(async move {
        let mut sim_interval = interval(Duration::from_secs(5));
        info!("Global Energy Simulation Loop started. Tick interval: 5s.");
        loop {
            sim_interval.tick().await;
            let tick_correlation_id = Uuid::new_v4();
            if let Err(e) = simulation_service.execute_tick(tick_correlation_id).await {
                error!("CRITICAL: Error occurred inside energy simulation tick: {:?}", e);
            }
        }
    });

    // 7. Boot Tonic gRPC server
    let backend_server = StgBackendImpl {
        player_service,
        economy_service,
    };

    info!("Starting high-load Tonic gRPC service at {}...", bind_addr);
    tonic::transport::Server::builder()
        .add_service(StgBackendServer::new(backend_server))
        .serve(bind_addr)
        .await?;

    Ok(())
}
