use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{error, info, Span};
use uuid::Uuid;

use stg_domain::Locale;

/// RequestContext carries tracing metadata through the entire call chain.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub request_id: Uuid,
    pub correlation_id: Uuid,
    pub player_id: Option<Uuid>,
    pub server_id: String,
    pub locale: Locale,
    pub started_at: Instant,
}

impl RequestContext {
    pub fn new(server_id: String) -> Self {
        Self {
            request_id: Uuid::new_v4(),
            correlation_id: Uuid::new_v4(),
            player_id: None,
            server_id,
            locale: Locale::default(),
            started_at: Instant::now(),
        }
    }

    pub fn with_player(mut self, player_id: Uuid) -> Self {
        self.player_id = Some(player_id);
        self
    }

    pub fn with_correlation(mut self, correlation_id: Uuid) -> Self {
        self.correlation_id = correlation_id;
        self
    }

    pub fn with_locale(mut self, locale: Locale) -> Self {
        self.locale = locale;
        self
    }

    pub fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    /// Create a tracing span from this context.
    pub fn to_span(&self, operation: &str) {
        let span = Span::current();
        span.record("request_id", self.request_id.to_string());
        span.record("correlation_id", self.correlation_id.to_string());
        if let Some(pid) = self.player_id {
            span.record("player_id", pid.to_string());
        }
        span.record("server_id", self.server_id.as_str());
        span.record("operation", operation);
    }
}

/// Metrics counters for the outbox worker and system health.
#[derive(Debug, Default)]
pub struct SystemMetrics {
    pub outbox_published: AtomicU64,
    pub outbox_failed: AtomicU64,
    pub outbox_poison: AtomicU64,
    pub outbox_retried: AtomicU64,
    pub transactions_processed: AtomicU64,
    pub transactions_failed: AtomicU64,
    pub energy_ticks: AtomicU64,
    pub rpc_requests: AtomicU64,
    pub rpc_failures: AtomicU64,
    /// Streaming-specific metrics (TASK 7: observability)
    pub streaming_connections: AtomicU64,
    pub streaming_disconnects: AtomicU64,
    pub streaming_heartbeats: AtomicU64,
    /// Sequence lag tracking
    pub sequence_high_watermark: AtomicU64,
    pub reconnect_count: AtomicU64,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            outbox_published: self.outbox_published.load(Ordering::Relaxed),
            outbox_failed: self.outbox_failed.load(Ordering::Relaxed),
            outbox_poison: self.outbox_poison.load(Ordering::Relaxed),
            outbox_retried: self.outbox_retried.load(Ordering::Relaxed),
            transactions_processed: self.transactions_processed.load(Ordering::Relaxed),
            transactions_failed: self.transactions_failed.load(Ordering::Relaxed),
            energy_ticks: self.energy_ticks.load(Ordering::Relaxed),
            rpc_requests: self.rpc_requests.load(Ordering::Relaxed),
            rpc_failures: self.rpc_failures.load(Ordering::Relaxed),
            streaming_connections: self.streaming_connections.load(Ordering::Relaxed),
            streaming_disconnects: self.streaming_disconnects.load(Ordering::Relaxed),
            streaming_heartbeats: self.streaming_heartbeats.load(Ordering::Relaxed),
            sequence_high_watermark: self.sequence_high_watermark.load(Ordering::Relaxed),
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub outbox_published: u64,
    pub outbox_failed: u64,
    pub outbox_poison: u64,
    pub outbox_retried: u64,
    pub transactions_processed: u64,
    pub transactions_failed: u64,
    pub energy_ticks: u64,
    pub rpc_requests: u64,
    pub rpc_failures: u64,
    pub streaming_connections: u64,
    pub streaming_disconnects: u64,
    pub streaming_heartbeats: u64,
    pub sequence_high_watermark: u64,
    pub reconnect_count: u64,
}

pub fn trace_transaction(
    ctx: &RequestContext,
    operation: &str,
    result: &Result<impl std::fmt::Debug, impl std::fmt::Debug>,
) {
    let duration = ctx.elapsed_ms();
    match result {
        Ok(_) => {
            info!(
                request_id = %ctx.request_id,
                correlation_id = %ctx.correlation_id,
                operation = operation,
                duration_ms = duration,
                status = "ok",
            );
        }
        Err(e) => {
            error!(
                request_id = %ctx.request_id,
                correlation_id = %ctx.correlation_id,
                operation = operation,
                duration_ms = duration,
                status = "error",
                error = ?e,
            );
        }
    }
}
