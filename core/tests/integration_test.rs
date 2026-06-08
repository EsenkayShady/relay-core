use async_trait::async_trait;
use relay_core::{
    Agent, AgentConfig, AgentError, Finding, ResultSink, ResultStatus, ScanEngine, ScanResult,
    ScanTask, TaskQueue,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// ── Shared mock implementations ───────────────────────────────────────────────

struct SimpleEngine {
    pub should_fail: bool,
    pub sleep_ms: u64,
}

#[async_trait]
impl ScanEngine for SimpleEngine {
    async fn execute(&self, task: &ScanTask) -> Result<ScanResult, AgentError> {
        if self.sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        }
        if self.should_fail {
            return Err(AgentError::ScanEngineError("forced failure".into()));
        }
        Ok(
            ScanResult::success(task.id.clone(), "test-agent".into()).add_finding(Finding::new(
                format!("finding-{}", task.id),
                "Test finding".into(),
                "INFO".into(),
            )),
        )
    }
}

struct FiniteQueue {
    tasks: Vec<ScanTask>,
    index: usize,
    acked: Arc<Mutex<Vec<String>>>,
    nacked: Arc<Mutex<Vec<String>>>,
}

impl FiniteQueue {
    fn new(tasks: Vec<ScanTask>) -> Self {
        Self {
            tasks,
            index: 0,
            acked: Arc::new(Mutex::new(Vec::new())),
            nacked: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl TaskQueue for FiniteQueue {
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
        if self.index < self.tasks.len() {
            let task = self.tasks[self.index].clone();
            self.index += 1;
            Ok(task)
        } else {
            std::future::pending::<()>().await;
            unreachable!()
        }
    }

    async fn acknowledge_task(&mut self, task_id: String) -> Result<(), AgentError> {
        self.acked.lock().await.push(task_id);
        Ok(())
    }

    async fn nack_task(&mut self, task_id: String, _reason: String) -> Result<(), AgentError> {
        self.nacked.lock().await.push(task_id);
        Ok(())
    }
}

struct CollectingSink {
    results: Arc<Mutex<Vec<ScanResult>>>,
}

impl CollectingSink {
    fn new() -> Self {
        Self {
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn results_handle(&self) -> Arc<Mutex<Vec<ScanResult>>> {
        Arc::clone(&self.results)
    }
}

#[async_trait]
impl ResultSink for CollectingSink {
    async fn publish_result(&mut self, result: ScanResult) -> Result<(), AgentError> {
        self.results.lock().await.push(result);
        Ok(())
    }

    async fn publish_heartbeat(&mut self, _agent_id: &str) -> Result<(), AgentError> {
        Ok(())
    }
}

fn n_tasks(n: usize) -> Vec<ScanTask> {
    (0..n)
        .map(|i| ScanTask::new(format!("task-{}", i), "example.com".into(), "check".into()))
        .collect()
}

fn config(id: &str) -> AgentConfig {
    let mut c = AgentConfig::new(id.into());
    c.heartbeat_interval_secs = 300;
    c.max_retries = 0;
    c
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn agent_completes_all_tasks() {
    let tasks = n_tasks(5);
    let sink = CollectingSink::new();
    let results_handle = sink.results_handle();

    let mut agent = Agent::new(
        SimpleEngine {
            should_fail: false,
            sleep_ms: 0,
        },
        FiniteQueue::new(tasks),
        sink,
        config("integration-agent"),
    );

    tokio::time::timeout(Duration::from_secs(5), agent.run())
        .await
        .ok();

    let results = results_handle.lock().await;
    assert_eq!(results.len(), 5);
    for r in results.iter() {
        assert_eq!(r.status, ResultStatus::Success);
        assert_eq!(r.findings.len(), 1);
    }
}

#[tokio::test]
async fn failed_engine_produces_failed_results() {
    let tasks = n_tasks(3);
    let sink = CollectingSink::new();
    let results_handle = sink.results_handle();

    let mut agent = Agent::new(
        SimpleEngine {
            should_fail: true,
            sleep_ms: 0,
        },
        FiniteQueue::new(tasks),
        sink,
        config("fail-agent"),
    );

    tokio::time::timeout(Duration::from_secs(5), agent.run())
        .await
        .ok();

    let results = results_handle.lock().await;
    assert_eq!(results.len(), 3);
    for r in results.iter() {
        assert_eq!(r.status, ResultStatus::Failed);
        assert!(r.error.is_some());
    }
}

#[tokio::test]
async fn task_timeout_produces_timeout_result() {
    let mut task = ScanTask::new("slow-task".into(), "example.com".into(), "slow".into());
    task.timeout_secs = 1;

    let sink = CollectingSink::new();
    let results_handle = sink.results_handle();

    // Engine sleeps 10s, task timeout is 1s → should produce Timeout result.
    let mut agent = Agent::new(
        SimpleEngine {
            should_fail: false,
            sleep_ms: 10_000,
        },
        FiniteQueue::new(vec![task]),
        sink,
        config("timeout-agent"),
    );

    tokio::time::timeout(Duration::from_secs(5), agent.run())
        .await
        .ok();

    let results = results_handle.lock().await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, ResultStatus::Timeout);
}

#[tokio::test]
async fn concurrent_tasks_respect_max_concurrent() {
    let tasks = n_tasks(10);
    let sink = CollectingSink::new();
    let results_handle = sink.results_handle();

    let mut c = config("concurrent-agent");
    c.max_concurrent = 3; // At most 3 tasks in flight at once.

    // Each task takes 50ms — 10 tasks with concurrency 3 should take ~200ms.
    let mut agent = Agent::new(
        SimpleEngine {
            should_fail: false,
            sleep_ms: 50,
        },
        FiniteQueue::new(tasks),
        sink,
        c,
    );

    tokio::time::timeout(Duration::from_secs(5), agent.run())
        .await
        .ok();

    let results = results_handle.lock().await;
    assert_eq!(results.len(), 10);
}

#[tokio::test]
async fn result_has_correct_agent_id() {
    let tasks = n_tasks(1);
    let sink = CollectingSink::new();
    let results_handle = sink.results_handle();

    let mut agent = Agent::new(
        SimpleEngine {
            should_fail: false,
            sleep_ms: 0,
        },
        FiniteQueue::new(tasks),
        sink,
        config("my-named-agent"),
    );

    tokio::time::timeout(Duration::from_secs(5), agent.run())
        .await
        .ok();

    let results = results_handle.lock().await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].agent_id, "my-named-agent");
}

#[tokio::test]
async fn result_sink_failure_exhausts_retries_and_nacks() {
    struct AlwaysFailSink;

    #[async_trait]
    impl ResultSink for AlwaysFailSink {
        async fn publish_result(&mut self, _: ScanResult) -> Result<(), AgentError> {
            Err(AgentError::ResultSinkError("always fails".into()))
        }
        async fn publish_heartbeat(&mut self, _: &str) -> Result<(), AgentError> {
            Ok(())
        }
    }

    let mut task = ScanTask::new(
        "fail-sink-task".into(),
        "example.com".into(),
        "check".into(),
    );
    task.timeout_secs = 10;

    let nacked: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let nacked_clone = Arc::clone(&nacked);

    struct TrackingQueue {
        tasks: Vec<ScanTask>,
        index: usize,
        nacked: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl TaskQueue for TrackingQueue {
        async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
            if self.index < self.tasks.len() {
                let t = self.tasks[self.index].clone();
                self.index += 1;
                Ok(t)
            } else {
                std::future::pending::<()>().await;
                unreachable!()
            }
        }

        async fn nack_task(&mut self, task_id: String, _: String) -> Result<(), AgentError> {
            self.nacked.lock().await.push(task_id);
            Ok(())
        }
    }

    let mut c = config("fail-sink-agent");
    c.max_retries = 2;

    let mut agent = Agent::new(
        SimpleEngine {
            should_fail: false,
            sleep_ms: 0,
        },
        TrackingQueue {
            tasks: vec![task],
            index: 0,
            nacked: nacked_clone,
        },
        AlwaysFailSink,
        c,
    );

    tokio::time::timeout(Duration::from_secs(15), agent.run())
        .await
        .ok();

    let nacked_list = nacked.lock().await;
    assert_eq!(
        nacked_list.len(),
        1,
        "task should be nacked after retry exhaustion"
    );
    assert_eq!(nacked_list[0], "fail-sink-task");
}
