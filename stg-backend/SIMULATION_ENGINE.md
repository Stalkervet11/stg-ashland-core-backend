# STG-Ashland Simulation Engine

## Overview

The Simulation Engine is the central orchestrator for all periodic world-state computations. It runs on a configurable interval (default 5 seconds), executing each subsystem in a deterministic pipeline. The scheduler is agnostic to subsystem implementation details, relying solely on the `SimulationSystem` trait.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                 SimulationScheduler                 │
│                                                     │
│  ┌─────────┐  ┌──────────┐  ┌───────────┐          │
│  │ Session │  │  Energy  │  │  Supply   │  ...     │
│  │Expirati │─▶│Simulation│─▶│  Chain    │─▶        │
│  │   on    │  │          │  │           │          │
│  └─────────┘  └──────────┘  └───────────┘          │
│       │              │              │               │
│       ▼              ▼              ▼               │
│  SubsystemTick  SubsystemTick  SubsystemTick        │
│    Outcome        Outcome        Outcome            │
│                                                     │
│              ┌──────────────┐                       │
│              │ TickMetrics  │──▶ PostgreSQL         │
│              └──────────────┘                       │
└─────────────────────────────────────────────────────┘
```

## Tick Lifecycle

```
  ┌──────────┐
  │  IDLE    │ ◀── timer fires (every tick_interval_ms)
  └────┬─────┘
       │
       ▼
  ┌──────────────┐
  │ ACQUIRE LOCK │ ── pg_try_advisory_xact_lock(tick_number)
  └────┬─────────┘
       │ true           │ false
       ▼                ▼
  ┌──────────┐    ┌──────────┐
  │ EXECUTE  │    │  SKIP    │──▶ record Failed tick
  │ PIPELINE │    └──────────┘
  └────┬─────┘
       │
       ▼
  For each subsystem (deterministic order):
  ┌──────────────────────────────┐
  │ system.tick(&ctx)            │
  │   ├─ Ok(outcome) → aggregate │
  │   └─ Err(e)     → record     │
  │       failure, continue      │
  └──────────────────────────────┘
       │
       ▼
  ┌──────────────────┐
  │ FINALIZE METRICS │
  │ Persist to DB    │
  │ Update dashboard │
  └──────────────────┘
```

## Pipeline Execution Order

Subsystems execute in **registration order** (deterministic):

| # | Subsystem | Status | Description |
|---|-----------|--------|-------------|
| 1 | `SessionExpiration` | Active | Expires stale player sessions |
| 2 | `EnergySimulation` | Active | Computes global energy state |
| 3 | `SupplyChain` | Placeholder | Resource production/distribution |
| 4 | `EconomyUpdate` | Placeholder | Periodic economic settlement |
| 5 | `WorldEvents` | Placeholder | Random/scripted world events |

Registration order cannot change at runtime. Adding a new subsystem appends it to the end.

## SimulationSystem Trait

Every subsystem implements:

```rust
#[async_trait]
pub trait SimulationSystem: Send + Sync {
    fn name(&self) -> &str;
    async fn tick(&self, ctx: &SimulationContext) -> Result<SubsystemTickOutcome, DomainError>;
}
```

The scheduler:
- Does **not** know subsystem internals
- Does **not** inspect outcomes beyond status codes
- Does **not** share state between subsystems (context is read-only)

## SimulationContext

Passed to every `tick()` call:

| Field | Type | Description |
|-------|------|-------------|
| `tick_number` | `u64` | Monotonically increasing |
| `now` | `DateTime<Utc>` | Tick start timestamp |
| `tick_duration_ms` | `i64` | Configured interval |
| `request_id` | `Uuid` | Idempotency key for this tick |
| `correlation_id` | `Uuid` | Links tick to its trigger |

## Failure Handling

- **Subsystem failure**: Error is recorded in `SubsystemTickOutcome`, tick continues with next subsystem
- **Tick marked `PartialFailure`**: One or more subsystems failed but tick completed
- **`max_failures_per_tick`**: If exceeded, remaining subsystems are skipped, tick marked `Failed`
- **Scheduler crash**: On restart, reads `MAX(tick_number)` from DB, resumes from next tick
- **Advisory lock prevents double execution**: `pg_try_advisory_xact_lock(tick_number)` ensures only one instance runs

## Tick Persistence

Stored in `simulation_ticks` table:

```sql
CREATE TABLE simulation_ticks (
    tick_id UUID PRIMARY KEY,
    tick_number BIGINT NOT NULL UNIQUE,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'IN_PROGRESS',
    total_events BIGINT NOT NULL DEFAULT 0,
    total_entities_processed BIGINT NOT NULL DEFAULT 0,
    subsystem_details JSONB
);
```

The `UNIQUE(tick_number)` constraint + advisory lock guarantee exactly-once execution.

## Graceful Shutdown

- `shutdown()` sends signal via `tokio::sync::watch`
- Current tick finishes before loop exits
- Dashboard shows `running: false`
- `SIGTERM` → call `scheduler.shutdown()` → wait for `run()` to return

## Metrics & Dashboard

The `SchedulerDashboard` snapshot provides:

| Metric | Description |
|--------|-------------|
| `current_tick` | Last tick number executed |
| `last_tick_duration_ms` | Duration of last tick |
| `total_ticks_completed` | Cumulative completed + partial |
| `total_ticks_failed` | Cumulative fully failed |
| `subsystem_statuses` | Per-subsystem: duration, status, failures |
| `uptime_secs` | Scheduler wall-clock uptime |

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tick_interval_ms` | 5000 | Milliseconds between tick starts |
| `auto_start` | true | Begin ticking on launch |
| `max_failures_per_tick` | MAX | Subsystem failures before aborting tick |

## Future Extensions

1. **Health endpoint**: Expose `SchedulerDashboard` via gRPC/HTTP
2. **Dynamic tick rate**: Adjust interval based on load metrics
3. **Subsystem dependency graph**: DAG-based execution instead of linear
4. **Distributed ticks**: Each subsystem on dedicated workers
5. **Tick replay**: Rerun historical ticks for debugging
