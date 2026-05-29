use crate::cluster::NodeEntry;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, Response};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct Router {
    nodes: Vec<Arc<NodeEntry>>,
    send_tx_node_ids: Vec<String>,
    grpc_node_ids: Vec<String>,
    heavy_rpc_counter: std::sync::atomic::AtomicUsize,
}

impl Router {
    pub fn new(
        nodes: Vec<Arc<NodeEntry>>,
        send_tx_node_ids: Vec<String>,
        grpc_node_ids: Vec<String>,
    ) -> Self {
        Self {
            nodes,
            send_tx_node_ids,
            grpc_node_ids,
            heavy_rpc_counter: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Pick the best healthy node for read RPCs (lowest priority)
    pub fn pick_read_node(&self) -> Option<Arc<NodeEntry>> {
        self.nodes
            .iter()
            .filter(|n| n.health.is_healthy())
            .min_by_key(|n| n.config.priority)
            .cloned()
    }

    /// Pick the best healthy node for WebSocket (lowest priority)
    pub fn pick_ws_node(&self) -> Option<Arc<NodeEntry>> {
        self.pick_read_node()
    }

    /// Pick the best healthy node for gRPC from eligible regions
    pub fn pick_grpc_node(&self) -> Option<Arc<NodeEntry>> {
        self.nodes
            .iter()
            .filter(|n| self.grpc_node_ids.contains(&n.config.id))
            .filter(|n| n.health.is_healthy())
            .filter(|n| n.health.grpc_healthy.load(std::sync::atomic::Ordering::Relaxed))
            .min_by_key(|n| n.config.priority)
            .cloned()
    }

    /// Get all gRPC-eligible nodes (no health filter — multi-region mux manages
    /// its own per-region reconnection)
    pub fn get_grpc_eligible_nodes(&self) -> Vec<Arc<NodeEntry>> {
        self.nodes
            .iter()
            .filter(|n| self.grpc_node_ids.contains(&n.config.id))
            .cloned()
            .collect()
    }

    /// Fan out sendTransaction to all healthy send-tx-eligible nodes
    /// Returns first successful response
    pub async fn fan_out_send_tx(
        &self,
        body: Bytes,
    ) -> Result<Response<Full<Bytes>>, String> {
        let targets: Vec<Arc<NodeEntry>> = self
            .nodes
            .iter()
            .filter(|n| self.send_tx_node_ids.contains(&n.config.id))
            .filter(|n| n.health.is_healthy())
            .cloned()
            .collect();

        if targets.is_empty() {
            return Err("No healthy nodes for sendTransaction".into());
        }

        let (tx, mut rx) = mpsc::channel::<Result<Response<Full<Bytes>>, String>>(targets.len());

        for node in &targets {
            let tx = tx.clone();
            let body = body.clone();
            let node = node.clone();

            tokio::spawn(async move {
                // Prefer iris_url if available and healthy
                let url = if node.config.iris_url.is_some()
                    && node.health.iris_healthy.load(std::sync::atomic::Ordering::Relaxed)
                {
                    node.config.iris_url.as_ref().unwrap().clone()
                } else {
                    node.config.rpc_url.clone()
                };

                let result = forward_rpc_to(&node, &url, body).await;
                let _ = tx.send(result).await;
            });
        }
        drop(tx);

        let mut last_err = None;
        while let Some(result) = rx.recv().await {
            match result {
                Ok(resp) => return Ok(resp),
                Err(e) => last_err = Some(e),
            }
        }

        Err(last_err.unwrap_or_else(|| "All upstreams failed".into()))
    }

    /// Forward a heavy read RPC (GPA, getTokenAccountsByOwner, etc.) to a healthy
    /// node via round-robin so expensive calls are distributed across the cluster.
    /// Falls back through remaining healthy nodes if the chosen one fails.
    pub async fn forward_heavy_rpc(
        &self,
        body: Bytes,
    ) -> Result<Response<Full<Bytes>>, String> {
        let healthy: Vec<&Arc<NodeEntry>> = self
            .nodes
            .iter()
            .filter(|n| n.health.is_healthy())
            .collect();

        if healthy.is_empty() {
            return Err("No healthy nodes for heavy RPC".to_string());
        }

        let start = self
            .heavy_rpc_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % healthy.len();

        let mut last_err = String::new();
        for i in 0..healthy.len() {
            let node = healthy[(start + i) % healthy.len()];
            match forward_rpc_to(node, &node.config.rpc_url, body.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    tracing::warn!("Heavy RPC node {} failed, trying next: {}", node.config.id, e);
                    last_err = e;
                }
            }
        }

        Err(last_err)
    }

    /// Forward a read RPC — tries nodes in priority order, falls back on failure
    pub async fn forward_read_rpc(
        &self,
        body: Bytes,
    ) -> Result<Response<Full<Bytes>>, String> {
        let mut healthy: Vec<&Arc<NodeEntry>> = self
            .nodes
            .iter()
            .filter(|n| n.health.is_healthy())
            .collect();

        if healthy.is_empty() {
            return Err("No healthy nodes".to_string());
        }

        healthy.sort_by_key(|n| n.config.priority);

        let mut last_err = String::new();
        for node in healthy {
            match forward_rpc_to(node, &node.config.rpc_url, body.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    tracing::warn!("Read RPC node {} failed, trying next: {}", node.config.id, e);
                    last_err = e;
                }
            }
        }

        Err(last_err)
    }

    pub fn get_nodes(&self) -> &[Arc<NodeEntry>] {
        &self.nodes
    }
}

const UPSTREAM_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);

async fn forward_rpc_to(
    node: &NodeEntry,
    url: &str,
    body: Bytes,
) -> Result<Response<Full<Bytes>>, String> {
    let uri: hyper::Uri = url.parse().map_err(|e| format!("Invalid URL: {}", e))?;

    let req = Request::builder()
        .method(hyper::Method::POST)
        .uri(&uri)
        .header("content-type", "application/json")
        .body(Full::new(body))
        .map_err(|e| format!("Request build error: {}", e))?;

    let resp = tokio::time::timeout(UPSTREAM_TIMEOUT, node.rpc_client.request(req))
        .await
        .map_err(|_| format!("Upstream {} timeout after 8s", node.config.id))?
        .map_err(|e| format!("Upstream {} error: {}", node.config.id, e))?;

    let status = resp.status();
    let resp_body = resp
        .into_body()
        .collect()
        .await
        .map_err(|e| format!("Body read error: {}", e))?
        .to_bytes();

    Ok(Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("access-control-allow-origin", "*")
        .body(Full::new(resp_body))
        .unwrap())
}
