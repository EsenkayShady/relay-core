mod convert;
mod engine;
mod queue;
mod sink;

pub mod relay {
    tonic::include_proto!("relay");
}

use engine::GrpcEngine;
use queue::GrpcTaskQueue;
use relay::relay_agent_client::RelayAgentClient;
use relay_core::{Agent, AgentConfig};
use sink::GrpcResultSink;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let api_key = env::var("RELAY_API_KEY").expect("RELAY_API_KEY must be set");
    let grpc_url = env::var("RELAY_GRPC_URL").unwrap_or_else(|_| "http://localhost:50051".into());

    let agent_id = format!("grpc-agent-{}", &api_key[..8.min(api_key.len())]);

    let channel = tonic::transport::Channel::from_shared(grpc_url.clone())?
        .connect()
        .await?;
    let client_q = RelayAgentClient::new(channel.clone());
    let client_s = RelayAgentClient::new(channel);

    let mut config = AgentConfig::new(agent_id.clone());
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
    config.metadata.insert("transport".into(), "grpc".into());
    config.metadata.insert("grpc_url".into(), grpc_url);

    let engine = GrpcEngine::new();
    let queue = GrpcTaskQueue::new(client_q, api_key.clone(), agent_id);
    let sink = GrpcResultSink::new(client_s, api_key);

    let mut agent = Agent::new(engine, queue, sink, config);
    agent.run().await?;
    Ok(())
}
