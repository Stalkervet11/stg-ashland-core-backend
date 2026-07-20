#![allow(dead_code)]

use serde::Serialize;
use stg_application::{LocalizationProvider, MessageKey};
use stg_domain::Locale;
use stg_infrastructure::{ResourceBundleLocalizationProvider, SystemMetrics};

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: &'static str,
    pub uptime_seconds: u64,
}

#[derive(Debug, Serialize)]
pub struct ReadyResponse {
    pub ready: bool,
    pub database: bool,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct LiveResponse {
    pub alive: bool,
}

#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub metrics: stg_infrastructure::MetricsSnapshot,
    pub version: &'static str,
}

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn uptime_seconds(start_time: std::time::Instant) -> u64 {
    start_time.elapsed().as_secs()
}

pub async fn check_database(pool: &sqlx::PgPool) -> bool {
    sqlx::query("SELECT 1").execute(pool).await.is_ok()
}

pub fn health_check(start_time: std::time::Instant, locale: Locale) -> HealthResponse {
    let l10n = ResourceBundleLocalizationProvider::new();
    HealthResponse {
        status: l10n.localize(&MessageKey::new("health.status.ok"), locale),
        version: VERSION,
        uptime_seconds: uptime_seconds(start_time),
    }
}

pub async fn readiness_check(pool: &sqlx::PgPool, locale: Locale) -> ReadyResponse {
    let l10n = ResourceBundleLocalizationProvider::new();
    let db_ok = check_database(pool).await;
    ReadyResponse {
        ready: db_ok,
        database: db_ok,
        message: if db_ok {
            l10n.localize(&MessageKey::new("health.ready.service_ready"), locale)
        } else {
            l10n.localize(&MessageKey::new("health.ready.database_failed"), locale)
        },
    }
}

pub fn liveness_check() -> LiveResponse {
    LiveResponse { alive: true }
}

pub fn metrics_snapshot(metrics: &SystemMetrics) -> MetricsResponse {
    MetricsResponse {
        metrics: metrics.snapshot(),
        version: VERSION,
    }
}

#[derive(Debug, Serialize)]
pub struct FullMetricsResponse {
    pub metrics: stg_infrastructure::MetricsSnapshot,
    pub version: &'static str,
    pub uptime_seconds: u64,
    pub scheduler_ticks: u64,
    pub energy_ticks: u64,
}

pub fn metrics_snapshot_with_system(
    metrics: &SystemMetrics,
    start_time: std::time::Instant,
    _locale: Locale,
) -> FullMetricsResponse {
    FullMetricsResponse {
        metrics: metrics.snapshot(),
        version: VERSION,
        uptime_seconds: uptime_seconds(start_time),
        scheduler_ticks: metrics
            .energy_ticks
            .load(std::sync::atomic::Ordering::Relaxed),
        energy_ticks: metrics
            .energy_ticks
            .load(std::sync::atomic::Ordering::Relaxed),
    }
}
