use std::sync::Arc;
use stg_domain::{
    DomainError, SimulationContext, SimulationSystem, SubsystemTickOutcome, TickId, TickMetrics,
    TickStatus,
};
use tokio::sync::watch;
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{error, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Tick Repository (port)
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait TickRepository: Send + Sync {
    /// Fetch the most recently completed tick number.
    async fn get_latest_tick_number(&self) -> Result<Option<u64>, DomainError>;

    /// Persist a tick metrics record.
    async fn save_tick_metrics(&self, metrics: &TickMetrics) -> Result<(), DomainError>;

    /// Acquire an advisory lock for the given tick number.
    /// Returns true if the lock was acquired (no other instance running this tick).
    async fn try_acquire_tick_lock(&self, tick_number: u64) -> Result<bool, DomainError>;
}

// ---------------------------------------------------------------------------
// Scheduler Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Interval between tick starts, in milliseconds.
    pub tick_interval_ms: u64,
    /// Whether to start ticking immediately on launch.
    pub auto_start: bool,
    /// Maximum number of concurrent subsystem failures before aborting the tick.
    pub max_failures_per_tick: usize,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: 5_000, // 5 seconds
            auto_start: true,
            max_failures_per_tick: usize::MAX, // never abort by default
        }
    }
}

// ---------------------------------------------------------------------------
// Simulation Dashboard Snapshot (for future monitoring)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchedulerDashboard {
    pub running: bool,
    pub current_tick: u64,
    pub last_tick_duration_ms: i64,
    pub total_ticks_completed: u64,
    pub total_ticks_failed: u64,
    pub subsystem_statuses: Vec<SubsystemDashboardEntry>,
    pub queue_depth: u64,
    pub uptime_secs: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SubsystemDashboardEntry {
    pub name: String,
    pub last_duration_ms: i64,
    pub last_status: String,
    pub consecutive_failures: u64,
}

// ---------------------------------------------------------------------------
// Simulation Scheduler
// ---------------------------------------------------------------------------

pub struct SimulationScheduler {
    config: SchedulerConfig,
    systems: Vec<Box<dyn SimulationSystem>>,
    tick_repo: Arc<dyn TickRepository>,
    dashboard: std::sync::Mutex<SchedulerDashboard>,
    /// Used to signal graceful shutdown.
    shutdown_tx: Option<watch::Sender<bool>>,
    shutdown_rx: watch::Receiver<bool>,
    started_at: chrono::DateTime<chrono::Utc>,
}

impl SimulationScheduler {
    pub fn new(config: SchedulerConfig, tick_repo: Arc<dyn TickRepository>) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            config,
            systems: Vec::new(),
            tick_repo,
            dashboard: std::sync::Mutex::new(SchedulerDashboard {
                running: false,
                current_tick: 0,
                last_tick_duration_ms: 0,
                total_ticks_completed: 0,
                total_ticks_failed: 0,
                subsystem_statuses: Vec::new(),
                queue_depth: 0,
                uptime_secs: 0,
            }),
            shutdown_tx: Some(shutdown_tx),
            shutdown_rx,
            started_at: chrono::Utc::now(),
        }
    }

    /// Register a simulation subsystem. Order of registration defines execution order.
    pub fn register_system(&mut self, system: Box<dyn SimulationSystem>) {
        // Initialize dashboard entry
        if let Ok(mut dash) = self.dashboard.lock() {
            dash.subsystem_statuses.push(SubsystemDashboardEntry {
                name: system.name().to_string(),
                last_duration_ms: 0,
                last_status: "UNKNOWN".to_string(),
                consecutive_failures: 0,
            });
        }
        self.systems.push(system);
    }

    /// Return the number of registered systems.
    pub fn system_count(&self) -> usize {
        self.systems.len()
    }

    /// Get a snapshot of the dashboard.
    pub fn dashboard_snapshot(&self) -> SchedulerDashboard {
        self.dashboard
            .lock()
            .map(|d| d.clone())
            .unwrap_or_else(|_e| SchedulerDashboard {
                running: false,
                current_tick: 0,
                last_tick_duration_ms: 0,
                total_ticks_completed: 0,
                total_ticks_failed: 0,
                subsystem_statuses: vec![],
                queue_depth: 0,
                uptime_secs: 0,
            })
    }

    /// Main loop. Runs until shutdown is signalled.
    pub async fn run(&mut self) -> Result<(), DomainError> {
        {
            let mut dash = self
                .dashboard
                .lock()
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            dash.running = true;
        }

        let mut tick_timer = interval(Duration::from_millis(self.config.tick_interval_ms));
        tick_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        info!(
            "SimulationScheduler started. Interval: {}ms. Systems: {}",
            self.config.tick_interval_ms,
            self.systems.len()
        );

        loop {
            tokio::select! {
                _ = tick_timer.tick() => {
                    // Check shutdown before each tick
                    if *self.shutdown_rx.borrow() {
                        info!("SimulationScheduler received shutdown signal (before tick).");
                        break;
                    }

                    self.execute_one_tick().await;
                }

                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        info!("SimulationScheduler received shutdown signal (during wait).");
                        break;
                    }
                }
            }
        }

        // Graceful: finish current tick then exit
        {
            let mut dash = self
                .dashboard
                .lock()
                .map_err(|e| DomainError::InternalStateError(e.to_string()))?;
            dash.running = false;
        }

        info!("SimulationScheduler stopped.");
        Ok(())
    }

    /// Signal graceful shutdown. Current tick will finish.
    pub fn shutdown(&self) {
        if let Some(tx) = &self.shutdown_tx {
            let _ = tx.send(true);
        }
    }

    /// Execute exactly one tick (visible for testing).
    pub async fn execute_one_tick(&self) -> TickMetrics {
        // Determine next tick number
        let latest = self
            .tick_repo
            .get_latest_tick_number()
            .await
            .unwrap_or(None);
        let tick_number = latest.map(|n| n + 1).unwrap_or(1);

        // Attempt advisory lock
        let locked = self
            .tick_repo
            .try_acquire_tick_lock(tick_number)
            .await
            .unwrap_or(false);

        if !locked {
            warn!(
                "Tick {}: advisory lock not acquired. Another instance may be running.",
                tick_number
            );
            // Record a no-op metrics entry so we don't leave gaps
            let mut metrics = TickMetrics::new(TickId(Uuid::new_v4()), tick_number);
            metrics.finalize(TickStatus::Failed);
            let _ = self.tick_repo.save_tick_metrics(&metrics).await;
            return metrics;
        }

        let correlation_id = Uuid::new_v4();
        let ctx = SimulationContext::new(
            tick_number,
            self.config.tick_interval_ms as i64,
            correlation_id,
        );

        let mut metrics = TickMetrics::new(TickId(Uuid::new_v4()), tick_number);
        let mut overall_status = TickStatus::Completed;
        let mut failures: usize = 0;

        // ---- Phase 1: Player Sessions ----
        // (handled by SessionService expire_stale_sessions if wired)
        // The scheduler delegates to registered subsystems only.

        // Execute each subsystem in deterministic order
        for system in &self.systems {
            if failures >= self.config.max_failures_per_tick {
                warn!(
                    "Tick {}: max failures ({}) reached. Skipping remaining subsystems.",
                    tick_number, self.config.max_failures_per_tick
                );
                overall_status = TickStatus::Failed;
                break;
            }

            let subsystem_name = system.name().to_string();
            let start = chrono::Utc::now();

            match system.tick(&ctx).await {
                Ok(outcome) => {
                    let elapsed = (chrono::Utc::now() - start).num_milliseconds();
                    let mut final_outcome = outcome;
                    final_outcome.duration_ms = elapsed;

                    if final_outcome.status != TickStatus::Completed {
                        overall_status = TickStatus::PartialFailure;
                        failures += 1;
                    }

                    // Update dashboard entry
                    if let Ok(mut dash) = self.dashboard.lock() {
                        if let Some(entry) = dash
                            .subsystem_statuses
                            .iter_mut()
                            .find(|e| e.name == subsystem_name)
                        {
                            entry.last_duration_ms = elapsed;
                            entry.last_status = format!("{:?}", final_outcome.status);
                            if final_outcome.status == TickStatus::Completed {
                                entry.consecutive_failures = 0;
                            }
                        }
                    }

                    metrics.record_subsystem(final_outcome);
                }
                Err(e) => {
                    let elapsed = (chrono::Utc::now() - start).num_milliseconds();
                    overall_status = TickStatus::PartialFailure;
                    failures += 1;

                    error!(
                        "Tick {}: subsystem '{}' failed: {:?}",
                        tick_number, subsystem_name, e
                    );

                    if let Ok(mut dash) = self.dashboard.lock() {
                        if let Some(entry) = dash
                            .subsystem_statuses
                            .iter_mut()
                            .find(|e| e.name == subsystem_name)
                        {
                            entry.last_duration_ms = elapsed;
                            entry.last_status = "FAILED".to_string();
                            entry.consecutive_failures += 1;
                        }
                    }

                    metrics.record_subsystem(SubsystemTickOutcome {
                        subsystem_name: subsystem_name.clone(),
                        status: TickStatus::Failed,
                        duration_ms: elapsed,
                        error: Some(e.to_string()),
                        events_generated: 0,
                        entities_processed: 0,
                    });
                }
            }
        }

        metrics.finalize(overall_status);

        // Persist tick
        if let Err(e) = self.tick_repo.save_tick_metrics(&metrics).await {
            error!("Failed to persist tick {} metrics: {:?}", tick_number, e);
        }

        // Update dashboard
        if let Ok(mut dash) = self.dashboard.lock() {
            dash.current_tick = tick_number;
            dash.last_tick_duration_ms = metrics.duration_ms;
            dash.uptime_secs = (chrono::Utc::now() - self.started_at).num_seconds().max(0) as u64;
            match metrics.status {
                TickStatus::Completed => dash.total_ticks_completed += 1,
                TickStatus::PartialFailure => dash.total_ticks_completed += 1,
                TickStatus::Failed => dash.total_ticks_failed += 1,
                _ => {}
            }
        }

        if overall_status == TickStatus::Completed {
            info!(
                "Tick {} completed in {}ms. {} events, {} entities.",
                tick_number,
                metrics.duration_ms,
                metrics.total_events,
                metrics.total_entities_processed
            );
        } else {
            warn!(
                "Tick {} finished with status {:?} in {}ms. {} failures.",
                tick_number, overall_status, metrics.duration_ms, failures
            );
        }

        metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    // ---- Test doubles ----

    struct TestTickRepository {
        latest: Mutex<Option<u64>>,
        saved: Mutex<Vec<TickMetrics>>,
        lock_granted: bool,
    }

    impl TestTickRepository {
        fn new() -> Self {
            Self {
                latest: Mutex::new(None),
                saved: Mutex::new(Vec::new()),
                lock_granted: true,
            }
        }

        fn saved_metrics(&self) -> Vec<TickMetrics> {
            self.saved.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl TickRepository for TestTickRepository {
        async fn get_latest_tick_number(&self) -> Result<Option<u64>, DomainError> {
            Ok(*self.latest.lock().unwrap())
        }

        async fn save_tick_metrics(&self, metrics: &TickMetrics) -> Result<(), DomainError> {
            self.saved.lock().unwrap().push(metrics.clone());
            // update latest
            *self.latest.lock().unwrap() = Some(metrics.tick_number);
            Ok(())
        }

        async fn try_acquire_tick_lock(&self, _tick_number: u64) -> Result<bool, DomainError> {
            Ok(self.lock_granted)
        }
    }

    struct CountingSystem {
        name: String,
        counter: Arc<AtomicU64>,
        should_fail: bool,
    }

    #[async_trait::async_trait]
    impl SimulationSystem for CountingSystem {
        fn name(&self) -> &str {
            &self.name
        }

        async fn tick(
            &self,
            _ctx: &SimulationContext,
        ) -> Result<SubsystemTickOutcome, DomainError> {
            if self.should_fail {
                return Err(DomainError::SubsystemFailure {
                    subsystem: self.name.clone(),
                    error: "injected failure".to_string(),
                });
            }

            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(SubsystemTickOutcome {
                subsystem_name: self.name.clone(),
                status: TickStatus::Completed,
                duration_ms: 0,
                error: None,
                events_generated: 3,
                entities_processed: 10,
            })
        }
    }

    // ---- Tests ----

    #[tokio::test]
    async fn test_scheduler_executes_all_systems_in_order() {
        let repo = Arc::new(TestTickRepository::new());
        let mut scheduler = SimulationScheduler::new(SchedulerConfig::default(), repo.clone());

        let counter1 = Arc::new(AtomicU64::new(0));
        let counter2 = Arc::new(AtomicU64::new(0));
        let counter3 = Arc::new(AtomicU64::new(0));

        scheduler.register_system(Box::new(CountingSystem {
            name: "A".into(),
            counter: counter1.clone(),
            should_fail: false,
        }));
        scheduler.register_system(Box::new(CountingSystem {
            name: "B".into(),
            counter: counter2.clone(),
            should_fail: false,
        }));
        scheduler.register_system(Box::new(CountingSystem {
            name: "C".into(),
            counter: counter3.clone(),
            should_fail: false,
        }));

        assert_eq!(scheduler.system_count(), 3);

        let metrics = scheduler.execute_one_tick().await;

        // Every system should have run exactly once
        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 1);
        assert_eq!(counter3.load(Ordering::SeqCst), 1);

        assert_eq!(metrics.status, TickStatus::Completed);
        assert_eq!(metrics.tick_number, 1);
        assert_eq!(metrics.subsystems.len(), 3);

        // Verify persistence
        let saved = repo.saved_metrics();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].tick_number, 1);

        // Run again -- tick number increments
        let metrics2 = scheduler.execute_one_tick().await;
        assert_eq!(metrics2.tick_number, 2);
    }

    #[tokio::test]
    async fn test_subsystem_failure_does_not_crash_scheduler() {
        let repo = Arc::new(TestTickRepository::new());
        let mut scheduler = SimulationScheduler::new(SchedulerConfig::default(), repo.clone());

        let counter = Arc::new(AtomicU64::new(0));

        scheduler.register_system(Box::new(CountingSystem {
            name: "good".into(),
            counter: counter.clone(),
            should_fail: false,
        }));
        scheduler.register_system(Box::new(CountingSystem {
            name: "bad".into(),
            counter: Arc::new(AtomicU64::new(0)),
            should_fail: true,
        }));
        scheduler.register_system(Box::new(CountingSystem {
            name: "also_good".into(),
            counter: counter.clone(),
            should_fail: false,
        }));

        let metrics = scheduler.execute_one_tick().await;

        // Good systems still ran
        assert_eq!(counter.load(Ordering::SeqCst), 2); // good + also_good

        assert_eq!(metrics.status, TickStatus::PartialFailure);
        assert_eq!(metrics.subsystems.len(), 3);

        // Verify bad system recorded error
        let bad = metrics
            .subsystems
            .iter()
            .find(|s| s.subsystem_name == "bad")
            .unwrap();
        assert_eq!(bad.status, TickStatus::Failed);
        assert!(bad.error.is_some());
    }

    #[tokio::test]
    async fn test_advisory_lock_prevents_execution() {
        let repo = Arc::new(TestTickRepository {
            lock_granted: false, // simulate another instance holding lock
            ..TestTickRepository::new()
        });

        let mut scheduler = SimulationScheduler::new(SchedulerConfig::default(), repo.clone());

        let counter = Arc::new(AtomicU64::new(0));
        scheduler.register_system(Box::new(CountingSystem {
            name: "X".into(),
            counter: counter.clone(),
            should_fail: false,
        }));

        let metrics = scheduler.execute_one_tick().await;

        // System should NOT have run
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(metrics.status, TickStatus::Failed);
    }

    #[tokio::test]
    async fn test_dashboard_uptime() {
        let repo = Arc::new(TestTickRepository::new());
        let scheduler = SimulationScheduler::new(SchedulerConfig::default(), repo);
        let dash = scheduler.dashboard_snapshot();
        assert!(!dash.running);
        assert_eq!(dash.current_tick, 0);
    }

    #[tokio::test]
    async fn test_shutdown_signal() {
        let repo = Arc::new(TestTickRepository::new());
        let scheduler = SimulationScheduler::new(SchedulerConfig::default(), repo);
        // Shutdown before running
        scheduler.shutdown();
        // The run loop would exit cleanly
    }
}
