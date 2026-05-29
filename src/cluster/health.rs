use crate::config::HealthConfig;
use crate::cluster::NodeEntry;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Healthy = 0,
    Degraded = 1,
    Down = 2,
}

impl NodeStatus {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => NodeStatus::Healthy,
            1 => NodeStatus::Degraded,
            _ => NodeStatus::Down,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            NodeStatus::Healthy => "healthy",
            NodeStatus::Degraded => "degraded",
            NodeStatus::Down => "down",
        }
    }
}

pub struct NodeHealth {
    pub node_id: String,
    pub status: AtomicU8,
    pub latency_ms: AtomicU64,
    pub last_slot: AtomicU64,
    pub last_seen_ms: AtomicU64,
    pub consecutive_failures: AtomicU32,
    pub consecutive_successes: AtomicU32,
    pub total_checks: AtomicU64,
    pub total_successes: AtomicU64,
    pub iris_healthy: AtomicBool,
    pub grpc_healthy: AtomicBool,
    /// True when the node responds but returns -32002 Unauthorized (RPC blocked, connection OK)
    pub rpc_blocked: AtomicBool,
    /// Multi-region mux: messages where this region delivered first (raced and won)
    pub wins: AtomicU64,
    /// Multi-region mux: messages this region delivered late (already seen)
    pub dupes: AtomicU64,
    /// Wall-clock ms (UNIX epoch) of last gRPC stream message from this region
    pub last_msg_ms: AtomicU64,
}

impl NodeHealth {
    pub fn new(node_id: String) -> Self {
        Self {
            node_id,
            status: AtomicU8::new(NodeStatus::Healthy as u8),
            latency_ms: AtomicU64::new(0),
            last_slot: AtomicU64::new(0),
            last_seen_ms: AtomicU64::new(0),
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            total_checks: AtomicU64::new(0),
            total_successes: AtomicU64::new(0),
            iris_healthy: AtomicBool::new(false),
            grpc_healthy: AtomicBool::new(true),
            rpc_blocked: AtomicBool::new(false),
            wins: AtomicU64::new(0),
            dupes: AtomicU64::new(0),
            last_msg_ms: AtomicU64::new(0),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.get_status() != NodeStatus::Down
    }

    pub fn get_status(&self) -> NodeStatus {
        NodeStatus::from_u8(self.status.load(Ordering::Relaxed))
    }

    pub fn uptime_pct(&self) -> f64 {
        let total = self.total_checks.load(Ordering::Relaxed);
        if total == 0 {
            return 100.0;
        }
        let success = self.total_successes.load(Ordering::Relaxed);
        (success as f64 / total as f64) * 100.0
    }

    fn record_success(&self, latency_ms: u64, slot: u64) {
        self.latency_ms.store(latency_ms, Ordering::Relaxed);
        self.last_slot.store(slot, Ordering::Relaxed);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_seen_ms.store(now_ms, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.consecutive_successes.fetch_add(1, Ordering::Relaxed);
        self.total_checks.fetch_add(1, Ordering::Relaxed);
        self.total_successes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.consecutive_successes.store(0, Ordering::Relaxed);
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        self.total_checks.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct HealthChecker {
    nodes: Vec<Arc<NodeEntry>>,
    config: HealthConfig,
}

impl HealthChecker {
    pub fn new(nodes: Vec<Arc<NodeEntry>>, config: HealthConfig) -> Self {
        Self { nodes, config }
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        let interval_secs = self.config.check_interval_secs.max(1);
        let timeout_ms = self.config.timeout_ms;
        let unhealthy_threshold = self.config.unhealthy_threshold;
        let healthy_threshold = self.config.healthy_threshold;
        let nodes = self.nodes;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

            loop {
                interval.tick().await;

                let mut max_slot: u64 = 0;

                for node in &nodes {
                    let health = &node.health;
                    let timeout = Duration::from_millis(timeout_ms);

                    // Check RPC health + getSlot
                    let outcome = tokio::time::timeout(
                        timeout,
                        check_node_rpc(&node.rpc_client, &node.config.rpc_url),
                    )
                    .await
                    .unwrap_or(RpcOutcome::Failed);

                    match outcome {
                        RpcOutcome::Healthy(latency_ms, slot) => {
                            health.rpc_blocked.store(false, Ordering::Relaxed);
                            health.record_success(latency_ms, slot);
                            if slot > max_slot {
                                max_slot = slot;
                            }

                            let successes = health.consecutive_successes.load(Ordering::Relaxed);
                            if health.get_status() == NodeStatus::Down
                                && successes >= healthy_threshold
                            {
                                health.status.store(NodeStatus::Healthy as u8, Ordering::Relaxed);
                                tracing::info!("Node {} is now healthy", health.node_id);
                            } else if health.get_status() != NodeStatus::Healthy
                                && successes >= healthy_threshold
                            {
                                health.status.store(NodeStatus::Healthy as u8, Ordering::Relaxed);
                            }

                            // High latency = degraded
                            if latency_ms > 500 && health.get_status() == NodeStatus::Healthy {
                                health.status.store(NodeStatus::Degraded as u8, Ordering::Relaxed);
                            }
                        }
                        RpcOutcome::Blocked => {
                            health.rpc_blocked.store(true, Ordering::Relaxed);
                            health.record_failure();
                            let failures = health.consecutive_failures.load(Ordering::Relaxed);
                            if failures >= unhealthy_threshold {
                                if health.get_status() != NodeStatus::Down {
                                    tracing::warn!("Node {} RPC BLOCKED (auth error, {} consecutive)", health.node_id, failures);
                                }
                                health.status.store(NodeStatus::Down as u8, Ordering::Relaxed);
                            }
                        }
                        RpcOutcome::Failed => {
                            health.rpc_blocked.store(false, Ordering::Relaxed);
                            health.record_failure();
                            let failures = health.consecutive_failures.load(Ordering::Relaxed);
                            if failures >= unhealthy_threshold {
                                if health.get_status() != NodeStatus::Down {
                                    tracing::warn!("Node {} is DOWN ({} consecutive failures)", health.node_id, failures);
                                }
                                health.status.store(NodeStatus::Down as u8, Ordering::Relaxed);
                            }
                        }
                    }

                    // Check iris health (if configured)
                    if let Some(ref iris_url) = node.config.iris_url {
                        let iris_ok = tokio::time::timeout(
                            timeout,
                            check_iris_health(&node.rpc_client, iris_url),
                        )
                        .await
                        .unwrap_or(Ok(false))
                        .unwrap_or(false);
                        health.iris_healthy.store(iris_ok, Ordering::Relaxed);
                    }

                    // Check gRPC health: Ping succeeds OR mux received data within 30s
                    if !node.config.grpc_url.is_empty() {
                        let ping_ok = tokio::time::timeout(
                            timeout,
                            check_grpc_health(&node.config.grpc_url, &node.config.grpc_x_token),
                        )
                        .await
                        .map(|r| r.is_ok())
                        .unwrap_or(false);

                        let last_msg_ms = health.last_msg_ms.load(Ordering::Relaxed);
                        let stream_ok = if last_msg_ms > 0 {
                            let now_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64;
                            now_ms.saturating_sub(last_msg_ms) < 30_000
                        } else {
                            false
                        };

                        let grpc_ok = ping_ok || stream_ok;
                        health.grpc_healthy.store(grpc_ok, Ordering::Relaxed);
                        if !grpc_ok {
                            tracing::debug!("Node {} gRPC health check failed (ping={} stream={})", health.node_id, ping_ok, stream_ok);
                        }
                    }
                }

                // Slot drift detection
                if max_slot > 0 {
                    for node in &nodes {
                        let node_slot = node.health.last_slot.load(Ordering::Relaxed);
                        if node_slot > 0
                            && max_slot > node_slot + 5
                            && node.health.get_status() == NodeStatus::Healthy
                        {
                            node.health.status.store(NodeStatus::Degraded as u8, Ordering::Relaxed);
                            tracing::debug!(
                                "Node {} degraded: slot {} is {} behind max {}",
                                node.health.node_id, node_slot, max_slot - node_slot, max_slot
                            );
                        }
                    }
                }
            }
        })
    }
}

enum RpcOutcome {
    Healthy(u64, u64), // (latency_ms, slot)
    Blocked,           // -32002 Unauthorized — node reachable but RPC auth blocked
    Failed,            // connection error, timeout, or unexpected response
}

async fn check_node_rpc(
    client: &Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
    rpc_url: &str,
) -> RpcOutcome {
    let start = std::time::Instant::now();

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot"
    });

    let uri: hyper::Uri = match rpc_url.parse() {
        Ok(u) => u,
        Err(_) => return RpcOutcome::Failed,
    };
    let req = match hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
    {
        Ok(r) => r,
        Err(_) => return RpcOutcome::Failed,
    };

    let resp = match client.request(req).await {
        Ok(r) => r,
        Err(_) => return RpcOutcome::Failed,
    };
    let resp_body = match resp.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return RpcOutcome::Failed,
    };
    let latency_ms = start.elapsed().as_millis() as u64;

    let json: serde_json::Value = match serde_json::from_slice(&resp_body) {
        Ok(j) => j,
        Err(_) => return RpcOutcome::Failed,
    };

    if let Some(slot) = json["result"].as_u64() {
        return RpcOutcome::Healthy(latency_ms, slot);
    }

    // Check for -32002 Unauthorized specifically
    if json["error"]["code"].as_i64() == Some(-32002) {
        return RpcOutcome::Blocked;
    }

    RpcOutcome::Failed
}

async fn check_iris_health(
    client: &Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
    iris_url: &str,
) -> anyhow::Result<bool> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "health"
    });

    let uri: hyper::Uri = iris_url.parse()?;
    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))?;

    let resp = client.request(req).await?;
    Ok(resp.status().is_success())
}

async fn check_grpc_health(grpc_url: &str, x_token: &str) -> anyhow::Result<()> {
    let endpoint = tonic::transport::Channel::from_shared(grpc_url.to_string())?
        .tcp_nodelay(true)
        .connect_timeout(std::time::Duration::from_secs(3));
    let channel = endpoint.connect().await?;

    use crate::proxy::grpc_yellowstone::geyser_proto::geyser_client::GeyserClient;
    use crate::proxy::grpc_yellowstone::geyser_proto::PingRequest;

    let mut client = GeyserClient::new(channel);
    let mut request = tonic::Request::new(PingRequest { count: 1 });
    if !x_token.is_empty() {
        if let Ok(val) = x_token.parse() {
            request.metadata_mut().insert("x-token", val);
        }
    }
    client.ping(request).await?;
    Ok(())
}

use hyper_util::client::legacy::Client;
