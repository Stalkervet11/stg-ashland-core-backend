# STG-Ashland Session Architecture

## Overview

The Player Session System provides authoritative, production-grade session management with guarantees against duplicate sessions, race conditions, and stale state. Every session is tracked in PostgreSQL with optimistic locking and advisory locks for critical paths.

## Session Lifecycle

```
  ┌──────────┐    create     ┌──────────┐   heartbeat    ┌──────────┐
  │  (none)  │──────────────▶│  ACTIVE  │───────────────▶│  ACTIVE  │ (extended)
  └──────────┘               └────┬─────┘                └──────────┘
                                  │
                    ┌─────────────┼─────────────┐
                    │             │             │
                    ▼             ▼             ▼
              ┌──────────┐ ┌──────────┐  ┌──────────────┐
              │ EXPIRED  │ │TERMINATED│  │TRANSITIONING │
              └────┬─────┘ └──────────┘  └──────┬───────┘
                   │                             │
              reconnect                  commit / abort
                   │                             │
                   ▼                             ▼
              ┌──────────┐               ┌──────────┐
              │  ACTIVE  │               │  ACTIVE  │
              │(new sess)│               │(new srv) │
              └──────────┘               └──────────┘
```

### States

| State | Code | Description |
|-------|------|-------------|
| `Active` | 0 | Player is connected and session is valid |
| `Expired` | 1 | Session heartbeat timeout exceeded |
| `Terminated` | 2 | Explicit logout or admin termination |
| `Reconnected` | 3 | Session restored after network disruption |
| `Transitioning` | 4 | Session ownership moving between servers |

### State Machine Rules

1. `Active` → `Expired`: Automatic when `expires_at < NOW()`
2. `Active` → `Terminated`: Explicit termination
3. `Active` → `Reconnected`: Reconnect before expiry
4. `Active` → `Transitioning`: BeginTransition handoff
5. `Reconnected` → `Active`: Heartbeat restores to Active
6. `Reconnected` → `Expired`: Automatic when timeout exceeded
7. `Transitioning` → `Active`: Commit transition
8. `Transitioning` → `Active`: Abort transition (restore to original server)
9. `Expired` + reconnect → New `Active` session

## Reconnect Flow

```
Client disconnects
        │
        ▼
  ┌─────────────────┐
  │ Can reconnect?  │
  │ state=Active/   │
  │ Reconnected AND │
  │ expires_at>NOW  │
  └────┬───────┬────┘
       │ YES   │ NO
       ▼       ▼
  ┌────────┐ ┌──────────────┐
  │Restore │ │ Expire old   │
  │session │ │ + Create new │
  └────────┘ └──────────────┘
```

- Reconnect before `expires_at`: session state changes to `Reconnected`, heartbeat extended
- Reconnect after `expires_at`: old session marked `Expired`, new session created
- Reconnect with no prior session: new session created

## Transition Flow

```
Server A (source)                 Backend                  Server B (target)
      │                              │                           │
      │  BeginTransition             │                           │
      │  (player, from, to, ticket)  │                           │
      │─────────────────────────────▶│                           │
      │                              │  pg_advisory_xact_lock    │
      │                              │  SELECT FOR UPDATE        │
      │                              │  Validate ownership       │
      │                              │  Mark TRANSITIONING       │
      │                              │  Insert transition record │
      │                              │                           │
      │◀───── TransitionResponse ────│                           │
      │                              │                           │
      │                              │  ClaimTransition          │
      │                              │◀──────────────────────────│
      │                              │                           │
      │                              │  CommitTransition         │
      │                              │◀──────────────────────────│
      │                              │  Mark ACTIVE on new srv   │
      │                              │                           │
```

### Concurrency Guarantees

1. **Advisory lock** (`pg_advisory_xact_lock`): Serializes transitions per player UUID
2. **SELECT FOR UPDATE**: Locks the session row within the transaction
3. **Optimistic locking**: `revision` column prevents stale writes
4. **Unique partial index**: `WHERE state IN ('ACTIVE', 'RECONNECTED')` prevents duplicate active sessions at the database level

## Database Schema

### `sessions` Table

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | UUID PK | Unique session identifier |
| `player_uuid` | UUID FK | Player reference |
| `server_id` | TEXT | Owning game server |
| `state` | VARCHAR(32) | Session state enum |
| `created_at` | TIMESTAMPTZ | Creation timestamp |
| `updated_at` | TIMESTAMPTZ | Last mutation timestamp |
| `last_heartbeat` | TIMESTAMPTZ | Most recent heartbeat |
| `expires_at` | TIMESTAMPTZ | Auto-expiry deadline |
| `revision` | BIGINT | Optimistic lock version |

### `player_transitions` Table

| Column | Type | Description |
|--------|------|-------------|
| `transition_id` | UUID PK | Unique transition identifier |
| `player_uuid` | UUID FK | Player reference |
| `ticket` | VARCHAR(128) | Transition authorization ticket |
| `from_server_id` | VARCHAR(64) | Source server |
| `to_server_id` | VARCHAR(64) | Target server |
| `status` | VARCHAR(32) | PENDING/COMPLETED/FAILED |
| `created_at` | TIMESTAMPTZ | Creation timestamp |
| `completed_at` | TIMESTAMPTZ | Completion timestamp |

### Key Indexes

- `idx_sessions_active_player` (UNIQUE, partial): Enforces at most one active/reconnected session per player
- `idx_sessions_expires`: Fast stale session cleanup
- `idx_sessions_server`: Server-scoped session queries
- `idx_transitions_player`: Player-scoped transition queries
- `idx_transitions_ticket`: Ticket-based transition lookup

## Failure Recovery

### Heartbeat Miss
- Background worker calls `expire_stale_sessions()` periodically
- Sessions past `expires_at` with state `ACTIVE`/`RECONNECTED` are marked `EXPIRED`
- Idempotent: can run multiple times safely

### Server Crash
- Sessions owned by crashed server eventually expire
- Players reconnect to any available server
- New session replaces expired one

### Network Partition
- During partition, heartbeat may fail
- Session expires on backend
- On reconnect after partition heals, player gets new session
- Transitional state data should be preserved separately

### Transition Failure
- If target server doesn't claim: source server can abort transition
- `restore_from_transition()` reverts to `ACTIVE` on original server
- Advisory lock prevents concurrent transition attempts

## Concurrency Model

### Optimistic Locking
Every session mutation increments `revision`. Updates use `WHERE revision = $expected` to detect concurrent modifications. On conflict, `RevisionConflict` error is returned.

### Advisory Locks (Transaction-level)
`pg_advisory_xact_lock(player_uuid_hash)` is acquired during `begin_transition`. This serializes all transitions for a given player. The lock is automatically released on commit/rollback.

### Unique Partial Index
The database enforces `player_uuid` uniqueness for rows where `state IN ('ACTIVE', 'RECONNECTED')`. This is the last line of defense against duplicate active sessions.

## API Endpoints

| gRPC Method | Session Operation |
|-------------|-------------------|
| `CreateSession` | `create_session()` |
| `ReconnectSession` | `reconnect()` |
| `HeartbeatSession` | `heartbeat()` |
| `TerminateSession` | `terminate_session()` |
| `BeginTransition` | `begin_transition()` |
| `CommitTransition` | (commit transition) |
| `AbortTransition` | (abort transition) |
