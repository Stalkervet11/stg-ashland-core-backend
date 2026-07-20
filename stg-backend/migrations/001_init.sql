-- STG-Ashland Core Relational Schema
-- Migration 001: Core tables

CREATE TABLE IF NOT EXISTS players (
    id UUID PRIMARY KEY,
    username VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS wallets (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL REFERENCES players(id),
    currency_code VARCHAR(16) NOT NULL,
    balance BIGINT NOT NULL DEFAULT 0,
    revision BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT wallets_balance_check CHECK (balance >= 0),
    CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency_code)
);

CREATE TABLE IF NOT EXISTS economy_transactions (
    id UUID PRIMARY KEY,
    tx_type VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    currency_code VARCHAR(16) NOT NULL,
    initiating_server_id VARCHAR(64) NOT NULL,
    request_id UUID NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    committed_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS economy_transaction_entries (
    id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES economy_transactions(id),
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    amount_delta BIGINT NOT NULL,
    CONSTRAINT entry_non_zero CHECK (amount_delta <> 0)
);

CREATE TABLE IF NOT EXISTS processed_requests (
    request_id UUID PRIMARY KEY,
    operation_type VARCHAR(64) NOT NULL DEFAULT 'GENERIC',
    request_fingerprint VARCHAR(128) NOT NULL DEFAULT '',
    response_payload TEXT NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL
);
