use crate::config::NodeConfig;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tonic::transport::Channel;
use tonic::Status;

pub struct ManagedChannel {
    pub node_id: String,
    endpoint: tonic::transport::Endpoint,
    channel: ArcSwap<Option<Channel>>,
    connecting: AtomicBool,
    reconnect_notify: Notify,
}

impl ManagedChannel {
    pub fn new(node_id: String, url: &str) -> Result<Self, String> {
        let endpoint = Channel::from_shared(url.to_string())
            .map_err(|e| format!("Invalid gRPC URL for {}: {}", node_id, e))?
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(5));

        Ok(Self {
            node_id,
            endpoint,
            channel: ArcSwap::from_pointee(None),
            connecting: AtomicBool::new(false),
            reconnect_notify: Notify::new(),
        })
    }

    pub async fn get(&self) -> Result<Channel, Status> {
        let guard = self.channel.load();
        if let Some(ref ch) = **guard {
            return Ok(ch.clone());
        }
        drop(guard);
        self.reconnect().await
    }

    pub async fn reconnect(&self) -> Result<Channel, Status> {
        if self
            .connecting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            // Another task is already reconnecting — wait for it to finish (up to 10s)
            match tokio::time::timeout(
                Duration::from_secs(10),
                self.reconnect_notify.notified(),
            )
            .await
            {
                Ok(()) => {
                    let guard = self.channel.load();
                    if let Some(ref ch) = **guard {
                        return Ok(ch.clone());
                    }
                    return Err(Status::unavailable(format!(
                        "gRPC reconnect failed for {}",
                        self.node_id
                    )));
                }
                Err(_timeout) => {
                    return Err(Status::unavailable(format!(
                        "gRPC reconnect timed out for {}",
                        self.node_id
                    )));
                }
            }
        }

        let result = self.endpoint.connect().await;
        match result {
            Ok(ch) => {
                self.channel.store(Arc::new(Some(ch.clone())));
                self.connecting.store(false, Ordering::Release);
                self.reconnect_notify.notify_waiters();
                tracing::info!("gRPC channel connected to node {}", self.node_id);
                Ok(ch)
            }
            Err(e) => {
                self.connecting.store(false, Ordering::Release);
                self.reconnect_notify.notify_waiters();
                tracing::warn!(
                    "gRPC reconnect failed for node {}: {}",
                    self.node_id,
                    e
                );
                Err(Status::unavailable(format!(
                    "gRPC connect failed for {}: {}",
                    self.node_id, e
                )))
            }
        }
    }

    pub fn invalidate(&self) {
        self.channel.store(Arc::new(None));
        tracing::debug!("gRPC channel invalidated for node {}", self.node_id);
    }
}

pub struct GrpcPool {
    channels: HashMap<String, Arc<ManagedChannel>>,
}

impl GrpcPool {
    pub fn new(nodes: &[NodeConfig], eligible_ids: &[String]) -> Self {
        let mut channels = HashMap::new();

        for node in nodes {
            if eligible_ids.contains(&node.id) {
                match ManagedChannel::new(node.id.clone(), &node.grpc_url) {
                    Ok(ch) => {
                        channels.insert(node.id.clone(), Arc::new(ch));
                    }
                    Err(e) => {
                        tracing::error!("Failed to create gRPC channel for {}: {}", node.id, e);
                    }
                }
            }
        }

        Self { channels }
    }

    pub fn get_channel(&self, node_id: &str) -> Option<Arc<ManagedChannel>> {
        self.channels.get(node_id).cloned()
    }

    pub fn invalidate(&self, node_id: &str) {
        if let Some(ch) = self.channels.get(node_id) {
            ch.invalidate();
        }
    }
}
