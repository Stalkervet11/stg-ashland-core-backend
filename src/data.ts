export interface RustFile {
  name: string;
  path: string;
  description: string;
  code: string;
  highlights: string[];
}

export const rustWorkspaceFiles: RustFile[] = [
  {
    name: "Cargo.toml",
    path: "/stg-backend/Cargo.toml",
    description: "Root workspace configuration file specifying the members of the modular monolith. It coordinates dependency sharing and assures proper isolated compilation of crates.",
    code: `[workspace]
resolver = "2"
members = [
    "crates/stg-domain",
    "crates/stg-application",
    "crates/stg-infrastructure",
    "crates/stg-proto",
    "crates/stg-server",
]`,
    highlights: [
      "Defines a Cargo Workspace with resolver version '2' for precise dependency resolution.",
      "Crates are strictly decoupled into domain, application, infrastructure, proto, and server."
    ]
  },
  {
    name: "stg-domain/src/lib.rs",
    path: "crates/stg-domain/src/lib.rs",
    description: "Pure core domain model crate containing domain-driven structs, strong type assertions, the double-entry accounting ledger rules, and the global energy simulation math. Zero external database or tonic dependencies to avoid cyclic imports.",
    code: `use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use thiserror::Error;
use uuid::Uuid;

// =========================================================================
// DOMAIN ERRORS (NO UNWRAP IN PRODUCTION)
// =========================================================================
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum DomainError {
    #[error("Player not found: {0}")]
    PlayerNotFound(Uuid),

    #[error("Insufficient funds in currency {0}. Required: {1}, available: {2}")]
    InsufficientFunds(String, i64, i64),

    #[error("Negative wallet balances are forbidden for currency {0}")]
    NegativeBalanceForbidden(String),

    #[error("Ledger imbalance detected. The sum of transaction entries must be exactly zero. Current sum: {0}")]
    LedgerImbalance(i64),
}

// =========================================================================
// STRONGLY TYPED IDENTIFIERS (NEWTYPES)
// =========================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlayerId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EnergyMode {
    Normal = 1,
    Surplus = 2,
    Deficit = 3,
    Critical = 4,
    Collapse = 5,
}

// =========================================================================
// MONEY (STRICTLY MINOR UNITS, I64)
// =========================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Money(pub i64);

// =========================================================================
// DOUBLE-ENTRY LEDGER SYSTEM
// =========================================================================
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub wallet_id: WalletId,
    pub amount_delta: i64, // Positive for credit, negative for debit
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EconomyTransaction {
    pub id: TransactionId,
    pub tx_type: EconomyTransactionType,
    pub status: EconomyTransactionStatus,
    pub entries: Vec<LedgerEntry>,
    pub idempotency_key: Uuid,
}

impl EconomyTransaction {
    pub fn create_and_validate(
        id: TransactionId,
        tx_type: EconomyTransactionType,
        currency_code: String,
        entries: Vec<LedgerEntry>,
        idempotency_key: Uuid,
    ) -> Result<Self, DomainError> {
        // Double-entry validation: sum must be exactly zero!
        let sum: i64 = entries.iter().map(|e| e.amount_delta).sum();
        if sum != 0 {
            return Err(DomainError::LedgerImbalance(sum));
        }
        Ok(Self { id, tx_type, status: EconomyTransactionStatus::Pending, entries, idempotency_key })
    }
}`,
    highlights: [
      "Approach 'domain-oriented architecture': Contains pure business models, validations and errors.",
      "No unwrap() in production-code: Uses structured DomainError returned via Result.",
      "Money represented as i64 minor units (100 = 1.00 credit) to avoid floating-point drift."
    ]
  },
  {
    name: "stg-application/src/lib.rs",
    path: "crates/stg-application/src/lib.rs",
    description: "Application services layer declaring the input/output Ports (Repository interfaces) and orchestrating transactional use cases. No transport-specific or database-specific libraries exist here.",
    code: `use async_trait::async_trait;
use stg_domain::{Player, Wallet, EconomyTransaction, PlayerId, DomainError};
use uuid::Uuid;

#[async_trait]
pub trait WalletRepository: Send + Sync {
    async fn find_by_player_and_currency(&self, id: PlayerId, code: &str) -> Result<Option<Wallet>, DomainError>;
    async fn save(&self, wallet: &Wallet) -> Result<(), DomainError>;
}

#[async_trait]
pub trait TransactionRepository: Send + Sync {
    async fn commit_transaction_with_balances(&self, tx: &EconomyTransaction, wallets: &[Wallet]) -> Result<(), DomainError>;
    async fn is_idempotent_processed(&self, key: Uuid) -> Result<bool, DomainError>;
}

pub struct EconomyService {
    wallet_repo: Box<dyn WalletRepository>,
    tx_repo: Box<dyn TransactionRepository>,
}

impl EconomyService {
    pub async fn transfer_money(
        &self,
        from_player: PlayerId,
        to_player: PlayerId,
        currency: &str,
        amount_minor: i64,
        idempotency_key: Uuid,
    ) -> Result<EconomyTransaction, DomainError> {
        // Idempotency check, lock balances, calculate ledgers, and commit atomically...
        Ok(tx)
    }
}`,
    highlights: [
      "Repository ports are declared using abstract Rust traits (decoupled from SQLx).",
      "Injects QueuePublisher interface to process background events without direct dependencies."
    ]
  },
  {
    name: "stg-infrastructure/src/lib.rs",
    path: "crates/stg-infrastructure/src/lib.rs",
    description: "Technical integration adapters including standard SQLx repository mappings and Queue publishers. All raw database statements, transactions, and tables are locked and executed strictly without any hidden ORMs.",
    code: `use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Transaction};
use stg_application::{WalletRepository, TransactionRepository};
use stg_domain::{Wallet, EconomyTransaction, DomainError};

pub struct PostgresWalletRepository {
    pool: PgPool,
}

#[async_trait]
impl WalletRepository for PostgresWalletRepository {
    async fn find_by_player_and_currency(&self, id: PlayerId, code: &str) -> Result<Option<Wallet>, DomainError> {
        let row = sqlx::query!("SELECT id, balance FROM wallets WHERE player_id = $1", id.0)
            .fetch_optional(&self.pool).await?;
        Ok(row.map(|r| Wallet { balance: Money(r.balance) }))
    }
}

#[async_trait]
impl TransactionRepository for PostgresTransactionRepository {
    async fn commit_transaction_with_balances(&self, tx: &EconomyTransaction, wallets: &[Wallet]) -> Result<(), DomainError> {
        let mut db_tx = self.pool.begin().await?;
        
        // 1. Insert Transaction
        sqlx::query!("INSERT INTO economy_transactions ...").execute(&mut *db_tx).await?;
        
        // 2. Lock rows FOR UPDATE to prevent race conditions
        for wallet in wallets {
            sqlx::query!("SELECT id FROM wallets WHERE id = $1 FOR UPDATE", wallet.id).fetch_one(&mut *db_tx).await?;
            sqlx::query!("UPDATE wallets SET balance = $1 WHERE id = $2", wallet.balance, wallet.id).execute(&mut *db_tx).await?;
        }
        
        db_tx.commit().await?;
        Ok(())
    }
}`,
    highlights: [
      "SQL-запросы только через sqlx без использования ORM: Complete transparency over raw query execution.",
      "Row locks via 'FOR UPDATE' prevent currency duplication during concurrent transaction retries.",
      "Check constraints and foreign keys declared in static PostgreSQL DDL scripts enforce absolute truth."
    ]
  },
  {
    name: "stg_core.proto",
    path: "crates/stg-proto/proto/stg_core.proto",
    description: "The authoritative gRPC contract for Ashland. Specifies unary transaction endpoints, bidirectional connection streams, registration nodes, and transition lifecycle tickets.",
    code: `syntax = "proto3";
package stg.v1;

service STGBackend {
  rpc Connect(stream ServerMessage) returns (stream BackendMessage);
  rpc RegisterPlayer(RegisterPlayerRequest) returns (RegisterPlayerResponse);
  rpc TransferMoney(TransferMoneyRequest) returns (TransactionResult);
  rpc ReportEnergyObservation(ReportEnergyObservationRequest) returns (ReportEnergyObservationResponse);
}

message ServerMessage {
  oneof payload {
    ServerHello hello = 1;
    ServerHeartbeat heartbeat = 2;
    EnergyNodeObservation energy_observation = 3;
  }
}

message BackendMessage {
  string event_id = 1;
  uint64 revision = 2;
  oneof payload {
    WorldSnapshot world_snapshot = 10;
    EnergyState energy_update = 11;
    EconomyState economy_update = 12;
  }
}`,
    highlights: [
      "Bidirectional connection stream allows high-frequency observations without opening new connections.",
      "Contains error packages with structured machine-readable error codes (INSUFFICIENT_FUNDS, QUOTE_EXPIRED)."
    ]
  },
  {
    name: "stg-server/src/main.rs",
    path: "crates/stg-server/src/main.rs",
    description: "Executable application bootstrapper. Instantiates the database pool, executes schema migrations, registers gRPC services, and boots the background async simulation ticker.",
    code: `use sqlx::postgres::PgPoolOptions;
use stg_application::{EconomyService, EnergySimulationService};
use stg_proto::v1::stg_backend_server::StgBackendServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new().connect(&db_url).await?;
    
    // Boot up the database schema
    sqlx::query(POSTGRES_DDL_SCHEMA).execute(&pool).await?;

    // Start background simulation tickers (runs every 5 seconds)
    tokio::spawn(async move {
        let mut sim_interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            sim_interval.tick().await;
            simulation_service.execute_tick(Uuid::new_v4()).await;
        }
    });

    // Run high-load Tonic server
    tonic::transport::Server::builder()
        .add_service(StgBackendServer::new(backend_server))
        .serve(bind_addr).await?;
        
    Ok(())
}`,
    highlights: [
      "Simulation ticks are isolated from Minecraft TPS and execute asynchronously inside a Toki o spawned task.",
      "Integrates dotenv to securely load environment secrets without leaking API keys to the client."
    ]
  }
];

export interface DBTable {
  name: string;
  columns: { name: string; type: string; constraints?: string }[];
  description: string;
}

export const dbSchemaTables: DBTable[] = [
  {
    name: "players",
    description: "Authoritative global registry of Minecraft players connected to Ashland. Uses immutable UUIDs.",
    columns: [
      { name: "id", type: "UUID", constraints: "PRIMARY KEY" },
      { name: "username", type: "VARCHAR(64)", constraints: "NOT NULL" },
      { name: "status", type: "VARCHAR(32)", constraints: "NOT NULL ('ACTIVE', 'LOCKED', 'TRANSITIONING')" },
      { name: "created_at", type: "TIMESTAMP WITH TZ", constraints: "NOT NULL" },
      { name: "last_seen_at", type: "TIMESTAMP WITH TZ", constraints: "NOT NULL" },
      { name: "revision", type: "BIGINT", constraints: "NOT NULL DEFAULT 0" }
    ]
  },
  {
    name: "wallets",
    description: "Holds transactional currency balances. A composite unique constraint ensures exactly one wallet per player/currency.",
    columns: [
      { name: "id", type: "UUID", constraints: "PRIMARY KEY" },
      { name: "player_id", type: "UUID", constraints: "FOREIGN KEY REFERENCES players(id)" },
      { name: "currency_code", type: "VARCHAR(16)", constraints: "NOT NULL" },
      { name: "balance", type: "BIGINT", constraints: "NOT NULL DEFAULT 0, CHECK (balance >= 0)" },
      { name: "revision", type: "BIGINT", constraints: "NOT NULL DEFAULT 0" }
    ]
  },
  {
    name: "economy_transactions",
    description: "The double-entry accounting ledger transaction boundary header. Ensures idempotency via unique request_id.",
    columns: [
      { name: "id", type: "UUID", constraints: "PRIMARY KEY" },
      { name: "tx_type", type: "VARCHAR(64)", constraints: "NOT NULL" },
      { name: "status", type: "VARCHAR(32)", constraints: "NOT NULL ('PENDING', 'COMMITTED')" },
      { name: "currency_code", type: "VARCHAR(16)", constraints: "NOT NULL" },
      { name: "initiating_server_id", type: "VARCHAR(64)", constraints: "NOT NULL" },
      { name: "request_id", type: "UUID", constraints: "NOT NULL UNIQUE (Idempotency Key)" },
      { name: "created_at", type: "TIMESTAMP WITH TZ", constraints: "NOT NULL" },
      { name: "committed_at", type: "TIMESTAMP WITH TZ" }
    ]
  },
  {
    name: "economy_transaction_entries",
    description: "Individual ledger debit/credit details. The sum of entries within any transaction must equal exactly zero.",
    columns: [
      { name: "id", type: "UUID", constraints: "PRIMARY KEY" },
      { name: "transaction_id", type: "UUID", constraints: "FOREIGN KEY REFERENCES economy_transactions(id)" },
      { name: "wallet_id", type: "UUID", constraints: "FOREIGN KEY REFERENCES wallets(id)" },
      { name: "amount_delta", type: "BIGINT", constraints: "NOT NULL, CHECK (amount_delta <> 0)" }
    ]
  },
  {
    name: "energy_nodes",
    description: "Tracks physical infrastructure machinery status reported from modded Minecraft servers.",
    columns: [
      { name: "id", type: "UUID", constraints: "PRIMARY KEY" },
      { name: "node_type", type: "VARCHAR(32)", constraints: "NOT NULL ('PRODUCER', 'CONSUMER', 'STORAGE')" },
      { name: "server_id", type: "VARCHAR(64)", constraints: "NOT NULL" },
      { name: "region_id", type: "VARCHAR(64)", constraints: "NOT NULL" },
      { name: "display_name", type: "VARCHAR(128)", constraints: "NOT NULL" },
      { name: "enabled", type: "BOOLEAN", constraints: "NOT NULL DEFAULT TRUE" },
      { name: "capacity_watts", type: "BIGINT", constraints: "NOT NULL, CHECK (capacity_watts >= 0)" },
      { name: "production_watts", type: "BIGINT", constraints: "NOT NULL, CHECK (production_watts >= 0)" },
      { name: "consumption_watts", type: "BIGINT", constraints: "NOT NULL, CHECK (consumption_watts >= 0)" },
      { name: "stored_wh", type: "BIGINT", constraints: "NOT NULL" },
      { name: "max_stored_wh", type: "BIGINT", constraints: "NOT NULL" },
      { name: "efficiency", type: "DOUBLE PRECISION", constraints: "NOT NULL DEFAULT 1.0" },
      { name: "health", type: "DOUBLE PRECISION", constraints: "NOT NULL DEFAULT 1.0" },
      { name: "revision", type: "BIGINT", constraints: "NOT NULL DEFAULT 0" },
      { name: "last_reported_at", type: "TIMESTAMP WITH TZ", constraints: "NOT NULL" }
    ]
  },
  {
    name: "domain_events",
    description: "Audit trail record of critical world events (ledger mutations, energy crises, transitions). Perfect for future queue consumption.",
    columns: [
      { name: "id", type: "UUID", constraints: "PRIMARY KEY" },
      { name: "aggregate_type", type: "VARCHAR(64)", constraints: "NOT NULL" },
      { name: "aggregate_id", type: "UUID", constraints: "NOT NULL" },
      { name: "aggregate_version", type: "BIGINT", constraints: "NOT NULL" },
      { name: "payload_json", type: "JSONB", constraints: "NOT NULL" },
      { name: "occurred_at", type: "TIMESTAMP WITH TZ", constraints: "NOT NULL" },
      { name: "correlation_id", type: "UUID", constraints: "NOT NULL" }
    ]
  }
];
