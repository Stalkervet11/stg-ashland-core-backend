# STG-Ashland Core Backend Architecture

## Overview

STG-Ashland is a **Production-Grade Authoritative Game Server** for a persistent multiplayer sandbox. Built as a **Domain-Oriented Architecture (Modular Monolith)** in Rust.

## Layer Responsibilities

```
┌─────────────────────────────────────┐
│  stg-server (Transport / Entry)     │  gRPC, startup, DI wiring
├─────────────────────────────────────┤
│  stg-api (gRPC Adapters)            │  Request parsing, auth, error mapping
├─────────────────────────────────────┤
│  stg-application (Use Cases)        │  Services, repositories (ports), domain events
├─────────────────────────────────────┤
│  stg-domain (Core Domain)           │  Entities, value objects, domain errors, events
├─────────────────────────────────────┤
│  stg-infrastructure (Adapters)      │  PostgreSQL, outbox, tracing, metrics
├─────────────────────────────────────┤
│  stg-proto (Protobuf)               │  Generated tonic/prost code
└─────────────────────────────────────┘
```

### Dependency Rules
- **stg-domain** has ZERO external dependencies (pure Rust, no database, no network)
- **stg-application** depends only on stg-domain + async_trait + sha2 + uuid
- **stg-infrastructure** depends on stg-domain + stg-application + sqlx
- **stg-api** depends on stg-proto + stg-application
- **stg-server** depends on everything; wires DI graph at startup

## Transaction Flow

```
RPC Request → Application Service
                  │
                  ├→ Repository::begin_transaction()
                  ├→ Check idempotency (processed_requests)
                  ├→ Lock rows (SELECT ... FOR UPDATE, deterministic order)
                  ├→ Perform domain calculations (checked arithmetic)
                  ├→ UPDATE entities (with version check)
                  ├→ INSERT transaction + ledger entries
                  ├→ INSERT domain_events
                  ├→ INSERT outbox_events (SAME TX!)
                  ├→ INSERT processed_requests
                  └→ COMMIT (all or nothing)
                       │
                       └→ Outbox Worker (async, separate process)
                            ├→ SELECT pending records (SKIP LOCKED)
                            ├→ Publish via QueuePublisher
                            └→ Mark PUBLISHED or retry with backoff
```

## Money Flow

- All amounts in **i64 minor units** (e.g., cents, micro-ASH)
- `Money` value object enforces non-negative invariant
- `EconomyTransaction` validates ledger balance (sum of entries == 0)
- All arithmetic uses `checked_add`/`checked_sub` — no overflow possible
- Wallet balances updated atomically within the same PostgreSQL transaction

## Energy Flow

- Energy nodes report production/consumption/storage
- `EnergyState` represents global grid mode (Normal/Surplus/Deficit/Critical/Collapse)
- `calculate_tick()` is a pure domain function — no I/O
- Global energy tick runs on a timer with **PostgreSQL advisory lock** (key=42)
- Only ONE server instance executes the tick; others skip

## Event Flow

```
Domain Layer: emits DomainEventPayload (pure data, no transport concern)
     │
Application Layer: DomainEventCollector records events
     │
Infrastructure Layer (during commit):
     ├→ domain_events table (event log, immutable)
     └→ outbox_events table (pending publish queue)
          │
Outbox Worker:
     ├→ Polls outbox_events (status=PENDING/FAILED, available_at <= NOW)
     ├→ FOR UPDATE SKIP LOCKED (multiple workers safe)
     ├→ Publishes via QueuePublisher trait
     ├→ On success: status=PUBLISHED
     └→ On failure: exponential backoff (2^n seconds, capped at 3600s)
```

## Concurrency Model

| Mechanism | Use Case |
|-----------|----------|
| `SELECT ... FOR UPDATE` | Wallet updates during transfers |
| Deterministic lock ordering | Deadlock prevention (lock wallets by UUID asc) |
| `FOR UPDATE SKIP LOCKED` | Outbox worker polling |
| `pg_try_advisory_lock(42)` | Global energy tick singleton |
| Optimistic locking (revision) | Player/wallet/energy node updates |
| Idempotency (fingerprint) | Duplicate request detection |

## Repository Model

- Application layer defines **trait** (port) — e.g., `WalletRepository`
- Infrastructure implements using **sqlx** (adapter) — e.g., `PostgresWalletRepository`
- Every `UPDATE` includes `WHERE revision = $n` and sets `revision = $n + 1`
- Version mismatch returns `DomainError::RevisionConflict`
- No ORM, no query builder — **explicit SQL** for full control

## Versioning Model

```
players.revision          BIGINT (optimistic lock)
wallets.revision          BIGINT (optimistic lock)
energy_nodes.revision     BIGINT (optimistic lock)
conversion_rules.revision BIGINT (optimistic lock)
```

Every mutation:
```sql
UPDATE wallets
SET balance = $1, revision = $2
WHERE id = $3 AND revision = $4
```

If `rows_affected() == 0` → `RevisionConflict`

## Recovery Model

| Scenario | Recovery |
|----------|----------|
| Crash during transfer | PostgreSQL transaction rolls back automatically |
| Outbox publish failure | Retry with exponential backoff (2^n, max 3600s) |
| Poison message (>10 retries) | Marked with long delay (24h), metrics counter incremented |
| Database restart | Connection pool auto-reconnects (sqlx) |
| Server restart | Outbox worker resumes from last committed position |
| Instance duplication | Advisory locks prevent duplicate energy ticks |

## Directory Responsibilities

```
stg-backend/
├── crates/
│   ├── stg-domain/         Domain entities, errors, events
│   ├── stg-application/     Use cases, repository ports, event collector
│   ├── stg-infrastructure/  PostgreSQL repos, outbox, metrics, tracing
│   ├── stg-proto/           Protobuf definitions + generated code
│   ├── stg-api/             gRPC service implementations
│   └── stg-server/          Main binary, DI wiring, health, startup
├── migrations/              SQL migration files (001-007)
├── tests/                   Integration tests
└── ARCHITECTURE.md          This file
```

## Developer Rules

1. **No unwrap()** in production code — use `Result` and `?`
2. **Money is always i64** minor units — never float
3. **SQLx only** — no ORM, no Diesel, no SeaORM
4. **No global mutable state** — all state through database
5. **No business logic in transport layer** — gRPC handlers are thin adapters
6. **No business logic in repositories** — repositories execute SQL only
7. **No cyclic dependencies** — dependency arrow always points down
8. **Transactional outbox is mandatory** — events published in same SQL transaction
9. **Version check on every UPDATE** — optimistic locking for mutable entities
10. **Checked arithmetic** — `checked_add`/`checked_sub` for all money operations
11. **Compile-time guarantees preferred** — type system over runtime checks
