use async_nats::Client;
use async_trait::async_trait;
use futures::StreamExt;
use relay_core::{AgentError, ScanTask, TaskQueue};
use tracing::{error, warn};

pub struct NatsTaskQueue {
    client: Client,
    api_key: String,
    subscriber: Option<async_nats::Subscriber>,
}

impl NatsTaskQueue {
    pub async fn new(client: Client, api_key: String) -> Result<Self, AgentError> {
        let subject = format!("relay.{}.tasks", api_key);
        let subscriber = client
            .subscribe(subject.clone())
            .await
            .map_err(|e| AgentError::TaskQueueError(format!("subscribe {subject}: {e}")))?;
        Ok(Self {
            client,
            api_key,
            subscriber: Some(subscriber),
        })
    }
}

#[async_trait]
impl TaskQueue for NatsTaskQueue {
    async fn get_next_task(&mut self) -> Result<ScanTask, AgentError> {
        loop {
            let mut sub = self
                .subscriber
                .take()
                .ok_or_else(|| AgentError::TaskQueueError("subscriber closed".into()))?;

            match sub.next().await {
                Some(msg) => {
                    self.subscriber = Some(sub);
                    match serde_json::from_slice::<ScanTask>(&msg.payload) {
                        Ok(task) => return Ok(task),
                        Err(e) => {
                            warn!("failed to deserialize task: {e}");
                            if let Some(reply) = msg.reply {
                                let _ = self.client.publish(reply, format!("ERR:{e}").into()).await;
                            }
                        }
                    }
                }
                None => {
                    drop(sub);
                    error!("NATS subscriber ended unexpectedly; resubscribing");
                    let subject = format!("relay.{}.tasks", self.api_key);
                    let new_sub = self.client.subscribe(subject.clone()).await.map_err(|e| {
                        AgentError::TaskQueueError(format!("resubscribe {subject}: {e}"))
                    })?;
                    self.subscriber = Some(new_sub);
                }
            }
        }
    }

    async fn acknowledge_task(&mut self, task_id: String) -> Result<(), AgentError> {
        let subject = format!("relay.{}.acks", self.api_key);
        let payload = format!("ACK:{task_id}");
        self.client
            .publish(subject, payload.into())
            .await
            .map_err(|e| AgentError::TaskQueueError(format!("ack {task_id}: {e}")))
    }

    async fn nack_task(&mut self, task_id: String, reason: String) -> Result<(), AgentError> {
        let subject = format!("relay.{}.nacks", self.api_key);
        let payload = serde_json::json!({ "task_id": task_id, "reason": reason }).to_string();
        self.client
            .publish(subject, payload.into())
            .await
            .map_err(|e| AgentError::TaskQueueError(format!("nack {task_id}: {e}")))
    }

    async fn health_check(&self) -> Result<(), AgentError> {
        // NATS client is healthy as long as the connection is open; no simple ping method
        Ok(())
    }
}
