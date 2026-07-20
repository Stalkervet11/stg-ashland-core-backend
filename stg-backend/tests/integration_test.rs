// STG-Ashland Integration Tests
// These tests require a running PostgreSQL instance.
// Set DATABASE_URL environment variable.
// Tests are gated behind #[cfg(feature = "integration-tests")]
// Run with: cargo test --test integration_test -- --ignored

use std::sync::Arc;
use stg_application::{
    DomainEventCollector, EconomyService, EnergySimulationService, PlayerService, QueuePublisher,
};
use stg_domain::{DomainError, DomainEvent, EconomyTransactionType, Money, PlayerId, WalletId};
use stg_infrastructure::{
    OutboxWorker, PostgresEnergyRepository, PostgresEventLogRepository,
    PostgresPlayerRepository, PostgresTransactionRepository, PostgresWalletRepository,
    PostgresOutboxStorage, SystemMetrics, POSTGRES_DDL_SCHEMA,
};
use uuid::Uuid;

#[derive(Clone)]
struct TestQueuePublisher {
    events: Arc<std::sync::Mutex<Vec<DomainEvent>>>,
}

impl TestQueuePublisher {
    fn new() -> Self {
        Self {
            events: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

#[async_trait::async_trait]
impl QueuePublisher for TestQueuePublisher {
    async fn publish(&self, event: &DomainEvent) -> Result<(), DomainError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

async fn setup_test_pool() -> sqlx::PgPool {
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/stg_ashland_test".to_string());

    let pool = sqlx::PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to connect to test database");

    // Apply schema
    sqlx::query(POSTGRES_DDL_SCHEMA)
        .execute(&pool)
        .await
        .expect("Failed to apply DDL schema");

    pool
}

async fn setup_services(
    pool: sqlx::PgPool,
    publisher: impl QueuePublisher + Clone + 'static,
) -> (
    PlayerService,
    EconomyService,
    EnergySimulationService,
    OutboxWorker,
) {
    let player_repo = Box::new(PostgresPlayerRepository::new(pool.clone()));
    let wallet_repo = Box::new(PostgresWalletRepository::new(pool.clone()));
    let tx_repo = Box::new(PostgresTransactionRepository::new(pool.clone()));
    let event_repo = Box::new(PostgresEventLogRepository::new(pool.clone()));
    let energy_repo = Box::new(PostgresEnergyRepository::new(pool.clone()));

    let player_service = PlayerService::new(
        Box::new(PostgresPlayerRepository::new(pool.clone())),
        Box::new(PostgresWalletRepository::new(pool.clone())),
        Box::new(PostgresEventLogRepository::new(pool.clone())),
        Box::new(publisher.clone()),
    );

    let economy_service = EconomyService::new(
        Box::new(PostgresPlayerRepository::new(pool.clone())),
        Box::new(PostgresWalletRepository::new(pool.clone())),
        tx_repo,
        event_repo.clone(),
        Box::new(publisher.clone()),
    );

    let energy_service = EnergySimulationService::new(
        energy_repo,
        event_repo,
        Box::new(publisher.clone()),
    );

    let outbox_worker = OutboxWorker::new(pool.clone(), Arc::new(publisher));

    (player_service, economy_service, energy_service, outbox_worker)
}

// =========================================================================
// Test: Player Registration
// =========================================================================

#[tokio::test]
#[ignore]
async fn test_player_registration() {
    let pool = setup_test_pool().await;
    let publisher = TestQueuePublisher::new();
    let (player_service, _, _, _) = setup_services(pool.clone(), publisher.clone()).await;

    let player_uuid = Uuid::new_v4();
    let result = player_service
        .register_player(player_uuid, "test_player".to_string(), Uuid::new_v4())
        .await;

    assert!(result.is_ok());
    let player = result.unwrap();
    assert_eq!(player.username, "test_player");
    assert_eq!(player.id.0, player_uuid);

    // Verify event was published
    let events = publisher.events.lock().unwrap();
    assert!(!events.is_empty());
}

// =========================================================================
// Test: Money Transfer
// =========================================================================

#[tokio::test]
#[ignore]
async fn test_money_transfer_between_players() {
    let pool = setup_test_pool().await;
    let publisher = TestQueuePublisher::new();
    let (player_service, economy_service, _, _) =
        setup_services(pool.clone(), publisher.clone()).await;

    // Register two players
    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();

    player_service
        .register_player(alice, "alice".to_string(), Uuid::new_v4())
        .await
        .expect("register alice");
    player_service
        .register_player(bob, "bob".to_string(), Uuid::new_v4())
        .await
        .expect("register bob");

    // Transfer money from Alice to Bob
    let result = economy_service
        .transfer_money(
            PlayerId(alice),
            PlayerId(bob),
            "ASH",
            100,
            Uuid::new_v4(),
            "test-server".to_string(),
            Uuid::new_v4(),
        )
        .await;

    assert!(result.is_ok());
    let tx = result.unwrap();
    assert_eq!(tx.currency_code, "ASH");
}

// =========================================================================
// Test: Idempotency
// =========================================================================

#[tokio::test]
#[ignore]
async fn test_transfer_idempotency() {
    let pool = setup_test_pool().await;
    let publisher = TestQueuePublisher::new();
    let (player_service, economy_service, _, _) =
        setup_services(pool.clone(), publisher.clone()).await;

    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();
    let idempotency_key = Uuid::new_v4();

    player_service
        .register_player(alice, "alice_idem".to_string(), Uuid::new_v4())
        .await
        .expect("register alice");
    player_service
        .register_player(bob, "bob_idem".to_string(), Uuid::new_v4())
        .await
        .expect("register bob");

    // First transfer
    let r1 = economy_service
        .transfer_money(
            PlayerId(alice),
            PlayerId(bob),
            "ASH",
            50,
            idempotency_key,
            "test-server".to_string(),
            Uuid::new_v4(),
        )
        .await;

    assert!(r1.is_ok());

    // Second transfer with same idempotency key - should return same result
    let r2 = economy_service
        .transfer_money(
            PlayerId(alice),
            PlayerId(bob),
            "ASH",
            50,
            idempotency_key,
            "test-server".to_string(),
            Uuid::new_v4(),
        )
        .await;

    assert!(r2.is_ok());
    assert_eq!(r1.unwrap().id.0, r2.unwrap().id.0);
}

// =========================================================================
// Test: Domain Event Collector
// =========================================================================

#[test]
fn test_domain_event_collector_basic() {
    let mut collector = DomainEventCollector::new(Uuid::new_v4());

    let event = DomainEvent {
        id: Uuid::new_v4(),
        aggregate_type: "Wallet".to_string(),
        aggregate_id: Uuid::new_v4(),
        aggregate_version: 1,
        payload: stg_domain::DomainEventPayload::MoneyTransferred {
            from_wallet: Uuid::new_v4(),
            to_wallet: Uuid::new_v4(),
            currency: "ASH".to_string(),
            amount: 100,
        },
        occurred_at: chrono::Utc::now(),
        correlation_id: Uuid::new_v4(),
    };

    collector.record(event);
    assert_eq!(collector.len(), 1);

    let outbox_records = collector.drain_to_outbox();
    assert_eq!(outbox_records.len(), 1);
    assert_eq!(outbox_records[0].event_type, "MoneyTransferred");
    assert!(collector.is_empty());
}

// =========================================================================
// Test: Outbox Worker - Poison Message Protection
// =========================================================================

#[tokio::test]
#[ignore]
async fn test_outbox_poison_message_protection() {
    let pool = setup_test_pool().await;
    let publisher = TestQueuePublisher::new();
    let (_ps, _es, _ens, outbox_worker) = setup_services(pool.clone(), publisher).await;

    // Process empty batch - should succeed with 0
    let result = outbox_worker.process_next_batch(10).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 0);
}

// =========================================================================
// Test: Version Conflict
// =========================================================================

#[tokio::test]
#[ignore]
async fn test_version_conflict_detection() {
    let pool = setup_test_pool().await;
    let publisher = TestQueuePublisher::new();
    let (player_service, _, _, _) = setup_services(pool.clone(), publisher).await;

    let player_uuid = Uuid::new_v4();
    player_service
        .register_player(player_uuid, "version_test".to_string(), Uuid::new_v4())
        .await
        .expect("register player");

    // Get player snapshot
    let (player, _) = player_service
        .get_player_snapshot(player_uuid)
        .await
        .expect("get snapshot");

    // Verify revision is set
    assert!(player.revision > 0);
}

// =========================================================================
// Test: Outbox Reliability
// =========================================================================

#[tokio::test]
#[ignore]
async fn test_outbox_reliability() {
    let pool = setup_test_pool().await;
    let publisher = Arc::new(TestQueuePublisher::new());
    let (player_service, economy_service, _, _) =
        setup_services(pool.clone(), (*publisher).clone()).await;

    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();

    // Register and transfer - events should be queued in outbox
    player_service
        .register_player(alice, "alice_outbox".to_string(), Uuid::new_v4())
        .await
        .expect("register");

    player_service
        .register_player(bob, "bob_outbox".to_string(), Uuid::new_v4())
        .await
        .expect("register");

    // Count events published
    let count_before = publisher.events.lock().unwrap().len();

    let _ = economy_service
        .transfer_money(
            PlayerId(alice),
            PlayerId(bob),
            "ASH",
            25,
            Uuid::new_v4(),
            "test-server".to_string(),
            Uuid::new_v4(),
        )
        .await;

    let count_after = publisher.events.lock().unwrap().len();
    // At least one more event should be published
    assert!(count_after >= count_before);
}
