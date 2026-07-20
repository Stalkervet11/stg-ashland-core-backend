# STG-Ashland Backend

Backend server for the STG-Core cross-server economy and simulation engine.
Built with Rust, tonic (gRPC), SQLx (PostgreSQL), and a simulation scheduler.

## Quick Start

**Prerequisites:** Rust 1.97+, PostgreSQL 16+

1. Copy and configure environment:
   ```bash
   cp .env.example .env
   # Edit .env with your production values
   ```

2. Run the server:
   ```bash
   DATABASE_URL=postgres://postgres:postgres@localhost:5432/stg_ashland \
   SERVER_ID=ashland-server-01 \
   SERVER_TOKEN=your-secret-token \
   WORLD_ID=ashland-overworld \
   cargo run
   ```

3. The gRPC server starts on `0.0.0.0:50051` by default, serving three services:
   - **STGAuth** — server authentication handshake
   - **STGUnary** — request-response RPCs (players, economy, energy, transitions)
   - **STGStreaming** — bidirectional realtime channel

## Configuration

See `.env.example` for all environment variables:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5432/stg_ashland` | PostgreSQL connection string |
| `GRPC_BIND_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `SERVER_ID` | *(required)* | Unique server identifier |
| `SERVER_TOKEN` | *(required)* | Server authentication token |
| `WORLD_ID` | `ashland-overworld` | World identifier |
| `LOG_LEVEL` | `info` | Tracing log level |

## Architecture

```
crates/
├── stg-domain        # Domain models, traits, error types
├── stg-application   # Services, scheduler, business logic
├── stg-infrastructure # PostgreSQL repos, outbox, metrics
├── stg-proto         # Protobuf definitions & generated code
├── stg-api           # gRPC handlers (auth, unary, streaming)
└── stg-server        # Bootstrap, wiring, main entry point
```

## License

Proprietary — all rights reserved.
