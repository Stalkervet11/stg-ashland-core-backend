-- Migration 004: Resource conversion tables

CREATE TABLE IF NOT EXISTS conversion_rules (
    id UUID PRIMARY KEY,
    direction VARCHAR(32) NOT NULL,
    resource_namespace VARCHAR(64) NOT NULL,
    resource_path VARCHAR(64) NOT NULL,
    currency_code VARCHAR(16) NOT NULL,
    unit_price_minor BIGINT NOT NULL,
    min_amount BIGINT NOT NULL DEFAULT 1,
    max_amount BIGINT NOT NULL DEFAULT 1000000,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    pricing_revision BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS resource_conversion_reservations (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL REFERENCES players(id),
    status VARCHAR(32) NOT NULL,
    direction VARCHAR(32) NOT NULL,
    resource_namespace VARCHAR(64) NOT NULL,
    resource_path VARCHAR(64) NOT NULL,
    resource_amount BIGINT NOT NULL,
    currency_code VARCHAR(16) NOT NULL,
    unit_price_minor BIGINT NOT NULL,
    total_price_minor BIGINT NOT NULL,
    pricing_revision BIGINT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
