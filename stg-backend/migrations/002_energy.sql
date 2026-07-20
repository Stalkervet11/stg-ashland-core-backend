-- Migration 002: Energy nodes and state

CREATE TABLE IF NOT EXISTS energy_nodes (
    id UUID PRIMARY KEY,
    node_type VARCHAR(32) NOT NULL,
    server_id VARCHAR(64) NOT NULL,
    region_id VARCHAR(64) NOT NULL,
    display_name VARCHAR(128) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    capacity_watts BIGINT NOT NULL,
    production_watts BIGINT NOT NULL,
    consumption_watts BIGINT NOT NULL,
    stored_wh BIGINT NOT NULL,
    max_stored_wh BIGINT NOT NULL,
    efficiency DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    health DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    revision BIGINT NOT NULL DEFAULT 0,
    last_reported_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT energy_non_negative CHECK (capacity_watts >= 0 AND production_watts >= 0 AND consumption_watts >= 0)
);

CREATE TABLE IF NOT EXISTS energy_state (
    id INTEGER PRIMARY KEY DEFAULT 1,
    mode VARCHAR(32) NOT NULL DEFAULT 'NORMAL',
    simulation_tick BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT energy_state_single_row CHECK (id = 1)
);
