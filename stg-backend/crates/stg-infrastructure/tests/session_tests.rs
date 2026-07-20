use stg_application::{SessionConfig, SessionRepository, SessionService};
use stg_domain::{DomainError, PlayerId, SessionId, SessionState};
use stg_infrastructure::{
    PostgresPlayerRepository, PostgresSessionRepository, PostgresTransactionManager,
    POSTGRES_DDL_SCHEMA,
};
use uuid::Uuid;

async fn setup_test_pool() -> sqlx::PgPool {
    let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5432/stg_ashland_test".to_string()
    });

    let pool = sqlx::pool::PoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to connect to test database");

    sqlx::query(POSTGRES_DDL_SCHEMA)
        .execute(&pool)
        .await
        .expect("Failed to apply DDL schema");

    let _ = sqlx::query("DELETE FROM player_transitions")
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM sessions").execute(&pool).await;
    let _ = sqlx::query("DELETE FROM economy_transaction_entries")
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM economy_transactions")
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM wallets").execute(&pool).await;
    let _ = sqlx::query("DELETE FROM players").execute(&pool).await;

    pool
}

async fn register_test_player(pool: &sqlx::PgPool, player_uuid: Uuid, username: &str) {
    sqlx::query(
        "INSERT INTO players (id, username, status, created_at, last_seen_at, revision) VALUES ($1, $2, 'ACTIVE', NOW(), NOW(), 0) ON CONFLICT (id) DO NOTHING"
    )
    .bind(player_uuid)
    .bind(username)
    .execute(pool)
    .await
    .expect("Failed to insert test player");
}

fn make_session_service(pool: sqlx::PgPool) -> SessionService {
    SessionService::new(
        Box::new(PostgresSessionRepository::new(pool.clone())),
        Box::new(PostgresPlayerRepository::new(pool.clone())),
        Box::new(PostgresTransactionManager::new(pool.clone())),
        SessionConfig {
            heartbeat_timeout_secs: 10,
            reconnect_grace_secs: 2,
        },
    )
}

#[tokio::test]
#[ignore]
async fn test_create_session() {
    let pool = setup_test_pool().await;
    let uid = Uuid::new_v4();
    register_test_player(&pool, uid, "t1").await;
    let svc = make_session_service(pool.clone());
    let s = svc
        .create_session(PlayerId(uid), "srv-1".into())
        .await
        .unwrap();
    assert_eq!(s.player_id.0, uid);
    assert_eq!(s.state, SessionState::Active);
}

#[tokio::test]
#[ignore]
async fn test_double_login_prevented() {
    let pool = setup_test_pool().await;
    let uid = Uuid::new_v4();
    register_test_player(&pool, uid, "t2").await;
    let svc = make_session_service(pool.clone());
    svc.create_session(PlayerId(uid), "srv-1".into())
        .await
        .unwrap();
    let r = svc.create_session(PlayerId(uid), "srv-2".into()).await;
    assert!(matches!(r.unwrap_err(), DomainError::DuplicateSession(_)));
}

#[tokio::test]
#[ignore]
async fn test_reconnect_before_expiry() {
    let pool = setup_test_pool().await;
    let uid = Uuid::new_v4();
    register_test_player(&pool, uid, "t3").await;
    let svc = make_session_service(pool.clone());
    let orig = svc
        .create_session(PlayerId(uid), "srv-1".into())
        .await
        .unwrap();
    let rec = svc.reconnect(PlayerId(uid), "srv-1".into()).await.unwrap();
    assert_eq!(rec.session_id.0, orig.session_id.0);
    assert_eq!(rec.state, SessionState::Reconnected);
}

#[tokio::test]
#[ignore]
async fn test_heartbeat_extends_expiry() {
    let pool = setup_test_pool().await;
    let uid = Uuid::new_v4();
    register_test_player(&pool, uid, "t4").await;
    let svc = make_session_service(pool.clone());
    let s = svc
        .create_session(PlayerId(uid), "srv-1".into())
        .await
        .unwrap();
    let orig = s.expires_at;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let u = svc.heartbeat(s.session_id).await.unwrap();
    assert!(u.expires_at > orig);
    assert!(u.revision > s.revision);
}

#[tokio::test]
#[ignore]
async fn test_session_termination() {
    let pool = setup_test_pool().await;
    let uid = Uuid::new_v4();
    register_test_player(&pool, uid, "t5").await;
    let svc = make_session_service(pool.clone());
    let s = svc
        .create_session(PlayerId(uid), "srv-1".into())
        .await
        .unwrap();
    let t = svc
        .terminate_session(s.session_id, "bye".into())
        .await
        .unwrap();
    assert_eq!(t.state, SessionState::Terminated);
    assert!(svc
        .terminate_session(s.session_id, "x".into())
        .await
        .is_err());
}

#[tokio::test]
#[ignore]
async fn test_session_not_found() {
    let pool = setup_test_pool().await;
    let svc = make_session_service(pool.clone());
    let r = svc.heartbeat(SessionId(Uuid::new_v4())).await;
    assert!(matches!(r.unwrap_err(), DomainError::SessionNotFound(_)));
}

#[tokio::test]
#[ignore]
async fn test_player_not_found() {
    let pool = setup_test_pool().await;
    let svc = make_session_service(pool.clone());
    let r = svc
        .create_session(PlayerId(Uuid::new_v4()), "srv-1".into())
        .await;
    assert!(matches!(r.unwrap_err(), DomainError::PlayerNotFound(_)));
}

#[tokio::test]
#[ignore]
async fn test_begin_transition() {
    let pool = setup_test_pool().await;
    let uid = Uuid::new_v4();
    register_test_player(&pool, uid, "t6").await;
    let svc = SessionService::new(
        Box::new(PostgresSessionRepository::new(pool.clone())),
        Box::new(PostgresPlayerRepository::new(pool.clone())),
        Box::new(PostgresTransactionManager::new(pool.clone())),
        SessionConfig::default(),
    );
    svc.create_session(PlayerId(uid), "srv-1".into())
        .await
        .unwrap();
    let tr = svc
        .begin_transition(
            PlayerId(uid),
            "srv-1".into(),
            "srv-2".into(),
            "ticket".into(),
        )
        .await
        .unwrap();
    assert_eq!(tr.state, SessionState::Transitioning);
    assert_eq!(tr.server_id, "srv-2");
}

#[tokio::test]
#[ignore]
async fn test_revision_conflict() {
    let pool = setup_test_pool().await;
    let uid = Uuid::new_v4();
    register_test_player(&pool, uid, "t7").await;
    let svc = make_session_service(pool.clone());
    let s = svc
        .create_session(PlayerId(uid), "srv-1".into())
        .await
        .unwrap();
    let repo = PostgresSessionRepository::new(pool.clone());
    let mut stale = s.clone();
    stale.revision = 0;
    stale.server_id = "hijacked".into();
    assert!(matches!(
        repo.update(&stale).await.unwrap_err(),
        DomainError::RevisionConflict(_, _, _)
    ));
}
