// STG-Core Server: main entry point for the Ashland backend.
//
// Wires together all service implementations and registers them as
// three gRPC services on a shared port:
//   - STGAuth (server authentication)
//   - STGUnary (all unary RPCs)
//   - STGStreaming (bidirectional realtime channel)

mod health;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::FmtSubscriber;

use sqlx::postgres::PgPoolOptions;
use stg_application::{
    ConversionService, EconomyService, EnergyService, EnergySimulationService, PlayerService,
    QueuePublisher, SchedulerConfig, SessionConfig, SessionService, SimulationScheduler,
    TickRepository, TransitionService, WorldSnapshotService,
};
use stg_domain::{DomainError, SimulationContext, SimulationSystem, SubsystemTickOutcome, TickStatus};
use stg_infrastructure::{
    OutboxWorker, PostgresConversionRepository, PostgresEnergyRepository,
    PostgresEventLogRepository, PostgresPlayerRepository, PostgresSessionRepository,
    PostgresTickRepository, PostgresTransactionManager, PostgresTransactionRepository,
    PostgresTransitionRepository, PostgresWalletRepository,
    SystemMetrics,
};
use stg_proto::stg::v1::{
    stg_auth_server::StgAuthServer,
    stg_unary_server::StgUnaryServer,
    stg_streaming_server::StgStreamingServer,
};

use stg_api::grpc::auth::StgAuthHandler;
use stg_api::grpc::unary::StgUnaryImpl;
use stg_api::grpc::streaming::StgStreamingAdapter;
use stg_api::grpc::publisher::LoggingQueuePublisher;

const OUTBOX_BATCH_SIZE: i32 = 20;

// =========================================================================
// Migration runner
// =========================================================================

async fn run_migrations(pool: &sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::migrate!("../../migrations").run(pool).await?;
    info!("PostgreSQL schema migrations applied successfully.");
    Ok(())
}

// =========================================================================
// MAIN
// =========================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("STG-Core starting up...");
    dotenv::dotenv().ok();
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/stg_ashland".to_string());
    let bind_addr: SocketAddr = std::env::var("GRPC_BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
        .parse()?;

    info!(version = health::VERSION, db = %db_url, bind = %bind_addr, "Configuration loaded");

    let pool = PgPoolOptions::new()
        .max_connections(25)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&db_url)
        .await?;

    info!("PostgreSQL connection established");
    run_migrations(&pool).await?;
    let db_healthy = health::check_database(&pool).await;
    info!(healthy = db_healthy, "Database health check");

    let metrics = Arc::new(SystemMetrics::new());

    // --- Server Identity & Auth configuration ---
    let server_id = std::env::var("SERVER_ID")
        .unwrap_or_else(|_| panic!("SERVER_ID environment variable must be set"));
    let server_token = std::env::var("SERVER_TOKEN")
        .unwrap_or_else(|_| panic!("SERVER_TOKEN environment variable must be set"));
    let mut server_tokens = HashMap::new();
    server_tokens.insert(server_id.clone(), server_token.clone());
    let server_tokens = Arc::new(server_tokens);
    info!("Auth configured: SERVER_ID={}", server_id);

    // --- Build all services ---
    let publisher = LoggingQueuePublisher;

    let player_service = Arc::new(PlayerService::new(
        Box::new(PostgresPlayerRepository::new(pool.clone())),
        Box::new(PostgresWalletRepository::new(pool.clone())),
        Box::new(PostgresEventLogRepository::new(pool.clone())),
        Box::new(publisher.clone()),
    ));

    let economy_service = Arc::new(EconomyService::new(
        Box::new(PostgresPlayerRepository::new(pool.clone())),
        Box::new(PostgresWalletRepository::new(pool.clone())),
        Box::new(PostgresTransactionRepository::new(pool.clone())),
        Box::new(PostgresEventLogRepository::new(pool.clone())),
        Box::new(publisher.clone()),
    ));

    let session_service = Arc::new(SessionService::new(
        Box::new(PostgresSessionRepository::new(pool.clone())),
        Box::new(PostgresPlayerRepository::new(pool.clone())),
        Box::new(PostgresTransactionManager::new(pool.clone())),
        SessionConfig::default(),
    ));

    // Session expiration is now orchestrated exclusively via SimulationScheduler
    // (see SessionExpirationAdapter registered below).

    let energy_service = Arc::new(EnergyService::new(Box::new(
        PostgresEnergyRepository::new(pool.clone()),
    )));

    let conversion_service = Arc::new(ConversionService::new(
        Box::new(PostgresPlayerRepository::new(pool.clone())),
        Box::new(PostgresConversionRepository::new(pool.clone())),
    ));

    let transition_service = Arc::new(TransitionService::new(
        Box::new(PostgresTransitionRepository::new(pool.clone())),
        Box::new(PostgresPlayerRepository::new(pool.clone())),
    ));

    let world_snapshot_service = Arc::new(WorldSnapshotService::new(Box::new(
        PostgresEnergyRepository::new(pool.clone()),
    )));

    let energy_sim_service = Arc::new(EnergySimulationService::new(
        Box::new(PostgresEnergyRepository::new(pool.clone())),
        Box::new(PostgresEventLogRepository::new(pool.clone())),
        Box::new(publisher.clone()),
    ));

    // --- Simulation Engine ---
    struct SessionExpirationAdapter {
        session_service: Arc<SessionService>,
    }
    #[async_trait::async_trait]
    impl SimulationSystem for SessionExpirationAdapter {
        fn name(&self) -> &str { "SessionExpiration" }
        async fn tick(&self, _ctx: &SimulationContext) -> Result<SubsystemTickOutcome, DomainError> {
            let count = self.session_service.expire_stale_sessions().await?;
            Ok(SubsystemTickOutcome {
                subsystem_name: "SessionExpiration".into(),
                status: TickStatus::Completed,
                duration_ms: 0,
                error: None,
                events_generated: count,
                entities_processed: count,
            })
        }
    }

    struct EnergySimAdapter {
        energy_sim: Arc<EnergySimulationService>,
    }
    #[async_trait::async_trait]
    impl SimulationSystem for EnergySimAdapter {
        fn name(&self) -> &str { "EnergySimulation" }
        async fn tick(&self, ctx: &SimulationContext) -> Result<SubsystemTickOutcome, DomainError> {
            let state = self.energy_sim.execute_tick(ctx.correlation_id).await?;
            Ok(SubsystemTickOutcome {
                subsystem_name: "EnergySimulation".into(),
                status: TickStatus::Completed,
                duration_ms: 0,
                error: None,
                events_generated: if state.simulation_tick > 0 { 1 } else { 0 },
                entities_processed: 0,
            })
        }
    }

    struct SupplyChainAdapter;
    #[async_trait::async_trait]
    impl SimulationSystem for SupplyChainAdapter {
        fn name(&self) -> &str { "SupplyChain" }
        async fn tick(&self, _ctx: &SimulationContext) -> Result<SubsystemTickOutcome, DomainError> {
            Ok(SubsystemTickOutcome {
                subsystem_name: "SupplyChain".into(),
                status: TickStatus::Completed,
                duration_ms: 0,
                error: None,
                events_generated: 0,
                entities_processed: 0,
            })
        }
    }

    struct EconomyAdapter;
    #[async_trait::async_trait]
    impl SimulationSystem for EconomyAdapter {
        fn name(&self) -> &str { "EconomyUpdate" }
        async fn tick(&self, _ctx: &SimulationContext) -> Result<SubsystemTickOutcome, DomainError> {
            Ok(SubsystemTickOutcome {
                subsystem_name: "EconomyUpdate".into(),
                status: TickStatus::Completed,
                duration_ms: 0,
                error: None,
                events_generated: 0,
                entities_processed: 0,
            })
        }
    }

    struct WorldEventsAdapter;
    #[async_trait::async_trait]
    impl SimulationSystem for WorldEventsAdapter {
        fn name(&self) -> &str { "WorldEvents" }
        async fn tick(&self, _ctx: &SimulationContext) -> Result<SubsystemTickOutcome, DomainError> {
            Ok(SubsystemTickOutcome {
                subsystem_name: "WorldEvents".into(),
                status: TickStatus::Completed,
                duration_ms: 0,
                error: None,
                events_generated: 0,
                entities_processed: 0,
            })
        }
    }

    struct OutboxAdapter {
        outbox_worker: Arc<OutboxWorker>,
        batch_size: i32,
        metrics: Arc<SystemMetrics>,
    }
    #[async_trait::async_trait]
    impl SimulationSystem for OutboxAdapter {
        fn name(&self) -> &str { "Outbox" }
        async fn tick(&self, _ctx: &SimulationContext) -> Result<SubsystemTickOutcome, DomainError> {
            match self.outbox_worker.process_next_batch(self.batch_size).await {
                Ok(count) => {
                    if count > 0 {
                        self.metrics.outbox_published.fetch_add(count as u64, Ordering::Relaxed);
                    }
                    Ok(SubsystemTickOutcome {
                        subsystem_name: "Outbox".into(),
                        status: TickStatus::Completed,
                        duration_ms: 0,
                        error: None,
                        events_generated: count,
                        entities_processed: count,
                    })
                }
                Err(e) => {
                    Err(e)
                }
            }
        }
    }

    let tick_repo: Arc<dyn TickRepository> = Arc::new(PostgresTickRepository::new(pool.clone()));

    let scheduler_config = SchedulerConfig {
        tick_interval_ms: 5000,
        auto_start: true,
        max_failures_per_tick: 10,
    };

    let mut scheduler = SimulationScheduler::new(scheduler_config, tick_repo);
    scheduler.register_system(Box::new(SessionExpirationAdapter {
        session_service: session_service.clone(),
    }));
    scheduler.register_system(Box::new(EnergySimAdapter {
        energy_sim: energy_sim_service.clone(),
    }));
    scheduler.register_system(Box::new(SupplyChainAdapter));
    scheduler.register_system(Box::new(EconomyAdapter));
    scheduler.register_system(Box::new(WorldEventsAdapter));
    scheduler.register_system(Box::new(
        stg_infrastructure::ProcessedRequestsCleanupSystem::new(
            pool.clone(),
            stg_infrastructure::CleanupConfig::default(),
        ),
    ));
    scheduler.register_system(Box::new(OutboxAdapter {
        outbox_worker: Arc::new(OutboxWorker::new(pool.clone(), Arc::new(publisher.clone()))),
        batch_size: OUTBOX_BATCH_SIZE,
        metrics: metrics.clone(),
    }));

    info!("SimulationScheduler registered {} subsystems.", scheduler.system_count());

    let scheduler_metrics = metrics.clone();
    tokio::spawn(async move {
        info!("SimulationScheduler worker started.");
        if let Err(e) = scheduler.run().await {
            error!("SimulationScheduler fatal error: {:?}", e);
            scheduler_metrics.rpc_failures.fetch_add(1, Ordering::Relaxed);
        }
        info!("SimulationScheduler worker stopped.");
    });

    // All background work (session expiration, energy simulation, outbox processing,
    // cleanup, etc.) is now orchestrated exclusively via SimulationScheduler.

    // =========================================================================
    // Build THREE gRPC services on the same port
    // =========================================================================

    let world_id = std::env::var("WORLD_ID")
        .unwrap_or_else(|_| "ashland-overworld".to_string());

    // --- Create the unified backend implementation (Arc-shared across services) ---
    let unary_impl = Arc::new(StgUnaryImpl::new(
        player_service.clone(),
        economy_service.clone(),
        energy_service.clone(),
        conversion_service.clone(),
        transition_service.clone(),
        world_snapshot_service.clone(),
        session_service.clone(),
        stg_infrastructure::ResourceBundleLocalizationProvider::new().into(),
        metrics.clone(),
        server_id.clone(),
        world_id.clone(),
    ));

    // --- STGAuth service ---
    let auth_handler = StgAuthHandler::new(session_service.clone());
    let auth_svc = StgAuthServer::new(auth_handler);

    // --- STGUnary service ---
    let unary_svc = StgUnaryServer::from_arc(unary_impl.clone());

    // --- STGStreaming service ---
    let streaming_impl = StgStreamingAdapter::new(unary_impl.clone());
    let streaming_svc = StgStreamingServer::new(streaming_impl);

    info!(
        version = health::VERSION,
        uptime_secs = start_time.elapsed().as_secs(),
        server_id = %server_id,
        world_id = %world_id,
        "STG-Core gRPC server with 3 services listening at {}",
        bind_addr
    );

    // Auth interceptor applied globally via tonic::service::interceptor
    // (all three services share the same auth logic)
    let auth_interceptor = {
        let server_tokens = server_tokens.clone();
        move |req: tonic::Request<()>| {
            let metadata = req.metadata();
            let token = metadata
                .get("authorization")
                .and_then(|t| t.to_str().ok())
                .unwrap_or("");
            let token = token.strip_prefix("Bearer ").unwrap_or(token);
            let req_server_id = metadata
                .get("x-server-id")
                .and_then(|s| s.to_str().ok())
                .unwrap_or("");

            if let Some(expected_token) = server_tokens.get(req_server_id) {
                if token == expected_token {
                    return Ok(req);
                }
            }
            Err(tonic::Status::unauthenticated(
                "Invalid server identity or token",
            ))
        }
    };

    // Register all three services on the same TCP port.
    tonic::transport::Server::builder()
        .layer(tonic::service::interceptor(auth_interceptor))
        .add_service(auth_svc)
        .add_service(unary_svc)
        .add_service(streaming_svc)
        .serve(bind_addr)
        .await?;

    Ok(())
}
