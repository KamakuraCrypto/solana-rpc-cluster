pub mod health;
pub mod router;
pub mod grpc_pool;

use crate::config::{Config, NodeConfig};
use crate::cluster::grpc_pool::GrpcPool;
use crate::cluster::health::{HealthChecker, NodeHealth};
use crate::cluster::router::Router;
use bytes::Bytes;
use http_body_util::Full;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::sync::Arc;

pub struct NodeEntry {
    pub config: NodeConfig,
    pub health: Arc<NodeHealth>,
    pub rpc_client: Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>,
}

pub struct ClusterState {
    pub nodes: Vec<Arc<NodeEntry>>,
    pub router: Arc<Router>,
    pub grpc_pool: Arc<GrpcPool>,
    pub health_checker: HealthChecker,
}

impl ClusterState {
    pub fn new(config: &Config) -> Self {
        let mut nodes = Vec::new();

        for node_config in &config.nodes {
            let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
            connector.set_nodelay(true);
            connector.set_keepalive(Some(std::time::Duration::from_secs(60)));

            let client = Client::builder(TokioExecutor::new())
                .pool_idle_timeout(std::time::Duration::from_secs(
                    node_config.rpc_pool_idle_timeout_secs,
                ))
                .pool_max_idle_per_host(node_config.rpc_pool_max_idle)
                .build(connector);

            let health = Arc::new(NodeHealth::new(node_config.id.clone()));

            nodes.push(Arc::new(NodeEntry {
                config: node_config.clone(),
                health,
                rpc_client: client,
            }));
        }

        let grpc_eligible = config.get_grpc_eligible_node_ids();
        let grpc_pool = Arc::new(GrpcPool::new(&config.nodes, &grpc_eligible));

        let router = Arc::new(Router::new(
            nodes.clone(),
            config.get_send_tx_node_ids(),
            grpc_eligible,
        ));

        let health_checker = HealthChecker::new(
            nodes.clone(),
            config.health.clone(),
        );

        ClusterState {
            nodes,
            router,
            grpc_pool,
            health_checker,
        }
    }
}
