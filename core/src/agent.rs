#[cfg(test)]
use crate::models::ScanTask;
use crate::models::{AgentConfig, AgentError, ScanResult};
use crate::traits::{AgentHooks, NoOpHooks, ResultSink, ScanEngine, TaskQueue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Instant, MissedTickBehavior};
use tracing::{debug, error, info, warn};

struct TaskOutcome {
    result: ScanResult,
}

/// Main agent orchestrator.
///
/// Generic over three trait parameters representing the three pluggable concerns:
/// - `E`: How tasks are executed ([`ScanEngine`])
/// - `Q`: Where tasks come from ([`TaskQueue`])
/// - `R`: Where results go ([`ResultSink`])
/// - `H`: Optional lifecycle hooks ([`AgentHooks`], defaults to [`NoOpHooks`])
///
/// # Example
/// ```ignore
/// let config = AgentConfig::new("agent-1".into());
/// let mut agent = Agent::new(MyEngine, MyQueue, MySink, config);
/// agent.run().await?;
/// ```
///
/// With hooks:
/// ```ignore
/// let mut agent = Agent::with_hooks(MyEngine, MyQueue, MySink, config, MyMetricsHook);
/// agent.run().await?;
/// ```
pub struct Agent<E, Q, R, H = NoOpHooks>
where
    E: ScanEngine,
    Q: TaskQueue,
    R: ResultSink,
    H: AgentHooks,
{
    engine: Arc<E>,
    queue: Q,
    sink: R,
    config: AgentConfig,
    hooks: Arc<H>,
}

impl<E, Q, R> Agent<E, Q, R>
where
    E: ScanEngine,
    Q: TaskQueue,
    R: ResultSink,
{
    /// Create an agent with no-op observability hooks.
    pub fn new(engine: E, queue: Q, sink: R, config: AgentConfig) -> Self {
        Self {
            engine: Arc::new(engine),
            queue,
            sink,
            config,
            hooks: Arc::new(NoOpHooks),
        }
    }
}

impl<E, Q, R, H> Agent<E, Q, R, H>
where
    E: ScanEngine + 'static,
    Q: TaskQueue,
    R: ResultSink,
    H: AgentHooks + 'static,
{
    /// Create an agent with custom lifecycle hooks.
    pub fn with_hooks(engine: E, queue: Q, sink: R, config: AgentConfig, hooks: H) -> Self {
        Self {
            engine: Arc::new(engine),
            queue,
            sink,
            config,
            hooks: Arc::new(hooks),
        }
    }

    /// Run the agent main loop.
    ///
    /// This method blocks until a SIGINT/Ctrl-C signal is received or an
    /// unrecoverable error occurs. It:
    ///
    /// 1. Validates configuration
    /// 2. Runs health checks on all three components
    /// 3. Enters the main loop: fetch → execute (concurrently) → publish results
    /// 4. On shutdown signal: stops accepting new tasks, drains in-flight work
    ///
    /// # Errors
    /// Returns `Err` only for unrecoverable startup failures (invalid config,
    /// failed health checks). Transient errors during operation are retried.
    pub async fn run(&mut self) -> Result<(), AgentError> {
        self.config.validate()?;

        info!(agent_id = %self.config.agent_id, "Agent starting");

        if let Err(e) = self.engine.health_check().await {
            error!(error = %e, "Engine health check failed");
            return Err(e);
        }
        if let Err(e) = self.queue.health_check().await {
            error!(error = %e, "Queue health check failed");
            return Err(e);
        }
        if let Err(e) = self.sink.health_check().await {
            error!(error = %e, "Sink health check failed");
            return Err(e);
        }

        info!(
            agent_id = %self.config.agent_id,
            max_concurrent = self.config.max_concurrent,
            "Agent initialized, entering main loop"
        );

        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent));
        let (result_tx, mut result_rx) = mpsc::unbounded_channel::<TaskOutcome>();
        let in_flight = Arc::new(AtomicUsize::new(0));

        let mut heartbeat_timer =
            interval(Duration::from_secs(self.config.heartbeat_interval_secs));
        heartbeat_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // Consume the immediate first tick so the first heartbeat fires after
        // the configured interval, not at time zero.
        heartbeat_timer.tick().await;

        let mut shutdown = false;
        let mut fetch_backoff_secs: u64 = 1;

        loop {
            // Exit once shutdown is requested and all in-flight tasks have reported back.
            if shutdown && in_flight.load(Ordering::SeqCst) == 0 {
                // Drain any results that arrived after the last iteration.
                while let Ok(outcome) = result_rx.try_recv() {
                    self.publish_with_retry(outcome.result).await;
                }
                break;
            }

            tokio::select! {
                biased; // Process results before fetching new work.

                // ── Completed task result ────────────────────────────────────
                Some(outcome) = result_rx.recv() => {
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    self.publish_with_retry(outcome.result).await;
                }

                // ── Periodic heartbeat ───────────────────────────────────────
                _ = heartbeat_timer.tick() => {
                    match self.sink.publish_heartbeat(&self.config.agent_id).await {
                        Ok(()) => {
                            debug!(agent_id = %self.config.agent_id, "Heartbeat sent");
                            self.hooks.on_heartbeat(&self.config.agent_id).await;
                        }
                        Err(e) => {
                            warn!(agent_id = %self.config.agent_id, error = %e, "Heartbeat failed");
                        }
                    }
                }

                // ── Fetch next task (disabled during shutdown) ───────────────
                task_result = self.queue.get_next_task(), if !shutdown => {
                    match task_result {
                        Ok(task) => {
                            fetch_backoff_secs = 1; // Reset backoff on successful fetch.

                            let permit = semaphore
                                .clone()
                                .acquire_owned()
                                .await
                                .map_err(|_| AgentError::ShutdownRequested)?;

                            in_flight.fetch_add(1, Ordering::SeqCst);

                            let engine  = Arc::clone(&self.engine);
                            let hooks   = Arc::clone(&self.hooks);
                            let tx      = result_tx.clone();
                            let agent_id = self.config.agent_id.clone();
                            let timeout_secs = task.timeout_secs;

                            tokio::spawn(async move {
                                let _permit = permit; // Released when this task drops.

                                hooks.on_task_start(&task).await;

                                let start   = Instant::now();
                                let timeout = Duration::from_secs(timeout_secs);

                                let result = match tokio::time::timeout(
                                    timeout,
                                    engine.execute(&task),
                                )
                                .await
                                {
                                    Ok(Ok(mut r)) => {
                                        r.duration_ms = start.elapsed().as_millis() as u64;
                                        r.agent_id    = agent_id;
                                        r
                                    }
                                    Ok(Err(e)) => {
                                        error!(task_id = %task.id, error = %e, "Task execution failed");
                                        hooks.on_task_error(&task.id, &e).await;
                                        ScanResult::failed(task.id.clone(), agent_id, e.to_string())
                                            .with_duration(start.elapsed().as_millis() as u64)
                                    }
                                    Err(_elapsed) => {
                                        warn!(task_id = %task.id, timeout_secs, "Task timed out");
                                        ScanResult::timeout(
                                            task.id.clone(),
                                            agent_id,
                                            start.elapsed().as_millis() as u64,
                                        )
                                    }
                                };

                                hooks.on_task_complete(&result).await;

                                // Ignore send errors — main loop may have exited.
                                let _ = tx.send(TaskOutcome { result });
                            });
                        }

                        Err(e) => {
                            error!(error = %e, backoff_secs = fetch_backoff_secs, "Task fetch failed");
                            sleep(Duration::from_secs(fetch_backoff_secs)).await;
                            fetch_backoff_secs = (fetch_backoff_secs * 2).min(60);
                        }
                    }
                }

                // ── Graceful shutdown on Ctrl-C / SIGINT ─────────────────────
                _ = tokio::signal::ctrl_c() => {
                    info!(agent_id = %self.config.agent_id, "Shutdown signal received, draining tasks");
                    self.hooks.on_shutdown().await;
                    shutdown = true;
                }
            }
        }

        info!(agent_id = %self.config.agent_id, "Agent shutdown complete");
        Ok(())
    }

    /// Publish a result with exponential-backoff retries.
    ///
    /// On exhausting retries the task is nack'd; the error is logged but the
    /// agent continues processing other tasks.
    async fn publish_with_retry(&mut self, result: ScanResult) {
        let mut attempt: u32 = 0;

        loop {
            match self.sink.publish_result(result.clone()).await {
                Ok(()) => {
                    debug!(task_id = %result.task_id, "Result published");
                    let _ = self.queue.acknowledge_task(result.task_id.clone()).await;
                    return;
                }
                Err(e) if attempt < self.config.max_retries => {
                    attempt += 1;
                    let backoff = Duration::from_secs(2_u64.pow(attempt));
                    warn!(
                        task_id = %result.task_id,
                        attempt,
                        error = %e,
                        "Result publish failed, retrying"
                    );
                    sleep(backoff).await;
                }
                Err(e) => {
                    error!(
                        task_id = %result.task_id,
                        error = %e,
                        "Result publish failed after max retries"
                    );
                    let _ = self
                        .queue
                        .nack_task(result.task_id.clone(), e.to_string())
                        .await;
                    return;
                }
            }
        }
    }
}

// ── Test helpers ─────────────────────────────────────────────────────────────

#[cfg(test)]
pub mod testing {
    use super::*;
    use async_trait::async_trait;

    /// Mock engine that succeeds or fails on demand.
    pub struct MockEngine {
        pub should_fail: bool,
    }

    #[async_trait]
    impl ScanEngine for MockEngine {
        async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
            if self.should_fail {
                Err(AgentError::ScanEngineError("mock failure".into()))
            } else {
                Ok(ScanResult::success(task.id.clone(), "mock-agent".into()))
            }
        }
    }

    /// Mock queue that drains a pre-loaded list of tasks.
    pub struct MockQueue {
        pub tasks: Vec<ScanTask>,
        pub index: usize,
    }

    impl MockQueue {
        pub fn with_tasks(tasks: Vec<ScanTask>) -> Self {
            Self { tasks, index: 0 }
        }
    }

    #[async_trait]
    impl TaskQueue for MockQueue {
        async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
            if self.index < self.tasks.len() {
                let task = self.tasks[self.index].clone();
                self.index += 1;
                Ok(task)
            } else {
                // Block indefinitely so the agent idles (tests cancel via timeout).
                std::future::pending::<()>().await;
                unreachable!()
            }
        }
    }

    /// Mock sink that collects published results for assertions.
    pub struct MockSink {
        pub results: Arc<tokio::sync::Mutex<Vec<ScanResult>>>,
        pub heartbeats: Arc<tokio::sync::Mutex<Vec<String>>>,
    }

    impl MockSink {
        pub fn new() -> Self {
            Self {
                results: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                heartbeats: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            }
        }

        pub fn results_handle(&self) -> Arc<tokio::sync::Mutex<Vec<ScanResult>>> {
            Arc::clone(&self.results)
        }
    }

    impl Default for MockSink {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl ResultSink for MockSink {
        async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
            self.results.lock().await.push(result);
            Ok(())
        }

        async fn publish_heartbeat(&mut self, agent_id: &str) -> Result<(), AgentError> {
            self.heartbeats.lock().await.push(agent_id.to_string());
            Ok(())
        }
    }

    /// Mock sink that always fails on publish_result.
    pub struct FailingSink {
        pub fail_count: Arc<tokio::sync::Mutex<u32>>,
    }

    impl FailingSink {
        pub fn new() -> Self {
            Self {
                fail_count: Arc::new(tokio::sync::Mutex::new(0)),
            }
        }
    }

    impl Default for FailingSink {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl ResultSink for FailingSink {
        async fn publish_result(&mut self, _result: ScanResult) -> Result<(), AgentError> {
            let mut count = self.fail_count.lock().await;
            *count += 1;
            Err(AgentError::ResultSinkError("mock sink failure".into()))
        }

        async fn publish_heartbeat(&mut self, _agent_id: &str) -> Result<(), AgentError> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use crate::agent::Agent;
    use crate::models::{AgentConfig, ScanTask};

    fn make_tasks(n: usize) -> Vec<ScanTask> {
        (0..n)
            .map(|i| ScanTask::new(format!("t{}", i), "target".into(), "check".into()))
            .collect()
    }

    #[tokio::test]
    async fn config_validation_rejects_empty_id() {
        let config = AgentConfig::new("".into());
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn config_validation_accepts_valid_config() {
        let config = AgentConfig::new("agent-01".into());
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn agent_processes_tasks_and_collects_results() {
        let tasks = make_tasks(3);
        let sink = MockSink::new();
        let results_handle = sink.results_handle();

        let config = {
            let mut c = AgentConfig::new("test-agent".into());
            c.heartbeat_interval_secs = 300; // Long interval so heartbeat doesn't fire.
            c.max_retries = 0;
            c
        };

        let mut agent = Agent::new(
            MockEngine { should_fail: false },
            MockQueue::with_tasks(tasks),
            sink,
            config,
        );

        // Run the agent with a timeout so it doesn't block the test suite.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), agent.run()).await;

        let results = results_handle.lock().await;
        assert_eq!(
            results.len(),
            3,
            "expected 3 results, got {}",
            results.len()
        );
        for r in results.iter() {
            assert_eq!(r.status, crate::models::ResultStatus::Success);
        }
    }

    #[tokio::test]
    async fn failing_engine_produces_failed_results() {
        let tasks = make_tasks(2);
        let sink = MockSink::new();
        let results_handle = sink.results_handle();

        let config = {
            let mut c = AgentConfig::new("test-agent".into());
            c.heartbeat_interval_secs = 300;
            c.max_retries = 0;
            c
        };

        let mut agent = Agent::new(
            MockEngine { should_fail: true },
            MockQueue::with_tasks(tasks),
            sink,
            config,
        );

        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), agent.run()).await;

        let results = results_handle.lock().await;
        assert_eq!(results.len(), 2);
        for r in results.iter() {
            assert_eq!(r.status, crate::models::ResultStatus::Failed);
        }
    }

    #[tokio::test]
    async fn task_timeout_produces_timeout_result() {
        // Task timeout of 0 seconds → immediate timeout.
        let mut task = ScanTask::new("t1".into(), "target".into(), "slow-check".into());
        task.timeout_secs = 1;

        // Engine that sleeps longer than the task timeout.
        struct SlowEngine;
        use crate::models::{AgentError, ScanResult};
        use crate::traits::ScanEngine;
        use async_trait::async_trait;

        #[async_trait]
        impl ScanEngine for SlowEngine {
            async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                Ok(ScanResult::success(task.id.clone(), "agent".into()))
            }
        }

        let sink = MockSink::new();
        let results_handle = sink.results_handle();

        let config = {
            let mut c = AgentConfig::new("test-agent".into());
            c.heartbeat_interval_secs = 300;
            c.max_retries = 0;
            c
        };

        let mut agent = Agent::new(SlowEngine, MockQueue::with_tasks(vec![task]), sink, config);

        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), agent.run()).await;

        let results = results_handle.lock().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, crate::models::ResultStatus::Timeout);
    }
}
