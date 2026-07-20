-- Migration 005: Performance indexes

CREATE INDEX IF NOT EXISTS idx_wallets_player ON wallets(player_id);
CREATE INDEX IF NOT EXISTS idx_wallets_player_currency ON wallets(player_id, currency_code);
CREATE INDEX IF NOT EXISTS idx_entries_tx ON economy_transaction_entries(transaction_id);
CREATE INDEX IF NOT EXISTS idx_events_aggregate ON domain_events(aggregate_type, aggregate_id);
CREATE INDEX IF NOT EXISTS idx_events_correlation ON domain_events(correlation_id);
CREATE INDEX IF NOT EXISTS idx_outbox_status_available ON outbox_events(status, available_at);
CREATE INDEX IF NOT EXISTS idx_outbox_created ON outbox_events(created_at);
CREATE INDEX IF NOT EXISTS idx_processed_requests_fingerprint ON processed_requests(request_id, request_fingerprint);
CREATE INDEX IF NOT EXISTS idx_energy_nodes_server ON energy_nodes(server_id, enabled);
CREATE INDEX IF NOT EXISTS idx_reservations_player ON resource_conversion_reservations(player_id, status);
