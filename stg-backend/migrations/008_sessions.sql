-- Migration 008: Player Sessions
-- Implements authoritative player session management
-- Supports: create, resume, terminate, heartbeat, reconnect, expiration, transition

-- Sessions table: tracks authoritative player sessions
CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID PRIMARY KEY,
    player_uuid UUID NOT NULL REFERENCES players(id),
    server_id TEXT NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0
);

-- Player transitions: tracks session handoff between servers
CREATE TABLE IF NOT EXISTS player_transitions (
    transition_id UUID PRIMARY KEY,
    player_uuid UUID NOT NULL REFERENCES players(id),
    ticket VARCHAR(128) NOT NULL,
    from_server_id VARCHAR(64) NOT NULL,
    to_server_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Unique partial index: at most ONE active/reconnected session per player
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_active_player
    ON sessions(player_uuid)
    WHERE state IN ('ACTIVE', 'RECONNECTED');

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(state, expires_at);
CREATE INDEX IF NOT EXISTS idx_sessions_server ON sessions(server_id);
CREATE INDEX IF NOT EXISTS idx_transitions_player ON player_transitions(player_uuid);
CREATE INDEX IF NOT EXISTS idx_transitions_ticket ON player_transitions(ticket);

-- Ensure revision column has default
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'sessions' AND column_name = 'revision'
    ) THEN
        ALTER TABLE sessions ADD COLUMN revision BIGINT NOT NULL DEFAULT 0;
    END IF;
END $$;
