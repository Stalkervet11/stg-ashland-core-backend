-- Migration 006: Constraints and data integrity

-- Ensure energy_state has exactly one row
INSERT INTO energy_state (id, mode, simulation_tick)
VALUES (1, 'NORMAL', 0)
ON CONFLICT (id) DO NOTHING;

-- Ensure wallets have unique player+currency
-- (already in 001_init but reinforced here)
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'wallets_player_currency_unique'
    ) THEN
        ALTER TABLE wallets ADD CONSTRAINT wallets_player_currency_unique UNIQUE (player_id, currency_code);
    END IF;
END $$;
