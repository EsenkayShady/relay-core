mod engine;
mod queue;
mod sink;

use engine::HttpEngine;
use queue::HttpTaskQueue;
use relay_core::{Agent, AgentConfig};
use reqwest::Client;
use sink::HttpResultSink;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let api_key = env::var("RELAY_API_KEY").expect("RELAY_API_KEY must be set");
    let base_url = env::var("RELAY_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());

    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let mut config = AgentConfig::new(format!("http-agent-{}", &api_key[..8.min(api_key.len())]));
    config.timeout_secs = env::var("SCAN_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    config.max_concurrent = env::var("MAX_CONCURRENT_TASKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    config.max_retries = env::var("MAX_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    config.heartbeat_interval_secs = env::var("HEARTBEAT_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    config.metadata.insert("transport".into(), "http".into());
    config.metadata.insert("base_url".into(), base_url.clone());

    let engine = HttpEngine::new();
    let queue = HttpTaskQueue::new(http_client.clone(), base_url.clone(), api_key.clone());
    let sink = HttpResultSink::new(http_client, base_url, api_key);

    let mut agent = Agent::new(engine, queue, sink, config);
    agent.run().await?;
    Ok(())
}
