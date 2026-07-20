-- Migration 009: Simulation Engine
-- Implements tick persistence and advisory locking for the central simulation scheduler.

CREATE TABLE IF NOT EXISTS simulation_ticks (
    tick_id UUID PRIMARY KEY,
    tick_number BIGINT NOT NULL UNIQUE,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_ms BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'IN_PROGRESS',
    total_events BIGINT NOT NULL DEFAULT 0,
    total_entities_processed BIGINT NOT NULL DEFAULT 0,
    subsystem_details JSONB
);

-- Fast lookup by tick number
CREATE INDEX IF NOT EXISTS idx_tick_number ON simulation_ticks(tick_number);

-- Filter by status for dashboard queries
CREATE INDEX IF NOT EXISTS idx_tick_status ON simulation_ticks(status);
