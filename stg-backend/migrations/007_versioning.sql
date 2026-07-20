-- Migration 007: Optimistic locking versioning

-- Ensure all mutable entities have revision columns
-- players.revision already exists from 001
-- wallets.revision already exists from 001
-- energy_nodes.revision already exists from 002

-- Add versioning to any tables that might be missing it
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'conversion_rules' AND column_name = 'revision'
    ) THEN
        ALTER TABLE conversion_rules ADD COLUMN revision BIGINT NOT NULL DEFAULT 0;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'resource_conversion_reservations' AND column_name = 'revision'
    ) THEN
        ALTER TABLE resource_conversion_reservations ADD COLUMN revision BIGINT NOT NULL DEFAULT 0;
    END IF;
END $$;
