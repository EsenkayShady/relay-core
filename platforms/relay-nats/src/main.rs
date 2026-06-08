mod engine;
mod queue;
mod sink;

use engine::NatsEngine;
use queue::NatsTaskQueue;
use relay_core::{Agent, AgentConfig};
use sink::NatsResultSink;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let api_key = env::var("RELAY_API_KEY").expect("RELAY_API_KEY must be set");
    let nats_url = env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());

    let client = async_nats::connect(&nats_url).await?;

    let mut config = AgentConfig::new(format!("nats-agent-{}", &api_key[..8.min(api_key.len())]));
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
    config.metadata.insert("transport".into(), "nats".into());
    config.metadata.insert("nats_url".into(), nats_url.clone());

    let engine = NatsEngine::new();
    let queue = NatsTaskQueue::new(client.clone(), api_key.clone()).await?;
    let sink = NatsResultSink::new(client, api_key);

    let mut agent = Agent::new(engine, queue, sink, config);
    agent.run().await?;
    Ok(())
}
