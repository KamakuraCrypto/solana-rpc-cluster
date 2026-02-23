use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::IpAddr;
use std::path::Path;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub general: GeneralConfig,
    #[serde(default)]
    pub nodes: Vec<NodeConfig>,
    pub rate_limits: RateLimitConfig,
    #[serde(default)]
    pub cpu: CpuConfig,
    #[serde(default)]
    pub firewall: FirewallConfig,
    #[serde(default)]
    pub health: HealthConfig,
    #[serde(default)]
    pub grpc: GrpcConfig,
    #[serde(default)]
    pub routing: RoutingConfig,
    #[serde(default)]
    pub whitelist: Vec<WhitelistEntry>,
    #[serde(default)]
    pub api_keys: Vec<ApiKeyEntry>,
    // Legacy single-upstream support
    #[serde(default)]
    pub upstream: Option<LegacyUpstreamConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GeneralConfig {
    #[serde(default = "default_rpc_bind")]
    pub rpc_bind: String,
    #[serde(default = "default_ws_bind")]
    pub ws_bind: String,
    #[serde(default = "default_grpc_bind")]
    pub grpc_bind: String,
    #[serde(default = "default_arpc_bind")]
    pub arpc_bind: String,
    #[serde(default = "default_dashboard_bind")]
    pub dashboard_bind: String,
    #[serde(default = "default_dashboard_user")]
    pub dashboard_user: String,
    #[serde(default = "default_dashboard_pass")]
    pub dashboard_pass: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NodeConfig {
    pub id: String,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub region: String,
    #[serde(default = "default_priority")]
    pub priority: u32,
    pub rpc_url: String,
    pub ws_url: String,
    pub grpc_url: String,
    pub arpc_url: String,
    #[serde(default)]
    pub grpc_x_token: String,
    #[serde(default)]
    pub iris_url: Option<String>,
    #[serde(default = "default_pool_max_idle")]
    pub rpc_pool_max_idle: usize,
    #[serde(default = "default_pool_idle_timeout")]
    pub rpc_pool_idle_timeout_secs: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HealthConfig {
    #[serde(default = "default_check_interval")]
    pub check_interval_secs: u64,
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
    #[serde(default = "default_healthy_threshold")]
    pub healthy_threshold: u32,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval_secs: default_check_interval(),
            unhealthy_threshold: default_unhealthy_threshold(),
            healthy_threshold: default_healthy_threshold(),
            timeout_ms: default_timeout_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RoutingConfig {
    #[serde(default = "default_send_tx_targets")]
    pub send_tx_targets: String,
    #[serde(default)]
    pub grpc_regions: Vec<String>,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            send_tx_targets: default_send_tx_targets(),
            grpc_regions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrpcConfig {
    /// HTTP/2 initial connection window size in bytes
    #[serde(default = "default_grpc_conn_window")]
    pub connection_window_bytes: u32,
    /// HTTP/2 initial stream window size in bytes
    #[serde(default = "default_grpc_stream_window")]
    pub stream_window_bytes: u32,
    /// Enable HTTP/2 adaptive flow control
    #[serde(default = "default_grpc_adaptive_window")]
    pub adaptive_window: bool,
    /// Max concurrent streams per connection
    #[serde(default = "default_grpc_concurrency_limit")]
    pub concurrency_limit: usize,
    /// Broadcast channel capacity for mux
    #[serde(default = "default_grpc_broadcast_capacity")]
    pub broadcast_capacity: usize,
    /// Per-client channel capacity
    #[serde(default = "default_grpc_client_channel_capacity")]
    pub client_channel_capacity: usize,
    /// Max lag events before disconnecting a slow consumer
    #[serde(default = "default_grpc_max_lag_events")]
    pub max_lag_events: u64,
    /// HTTP/2 keepalive interval in seconds (server-side)
    #[serde(default = "default_grpc_keepalive_interval")]
    pub keepalive_interval_secs: u64,
    /// HTTP/2 keepalive timeout in seconds (server-side)
    #[serde(default = "default_grpc_keepalive_timeout")]
    pub keepalive_timeout_secs: u64,
    /// Stream from all gRPC-eligible regions in parallel with dedup
    #[serde(default = "default_grpc_multi_region_enabled")]
    pub multi_region_enabled: bool,
    /// How long to remember messages for dedup (seconds)
    #[serde(default = "default_grpc_dedup_window_secs")]
    pub dedup_window_secs: u64,
    /// How often to evict expired dedup entries (seconds)
    #[serde(default = "default_grpc_dedup_eviction_interval_secs")]
    pub dedup_eviction_interval_secs: u64,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            connection_window_bytes: default_grpc_conn_window(),
            stream_window_bytes: default_grpc_stream_window(),
            adaptive_window: default_grpc_adaptive_window(),
            concurrency_limit: default_grpc_concurrency_limit(),
            broadcast_capacity: default_grpc_broadcast_capacity(),
            client_channel_capacity: default_grpc_client_channel_capacity(),
            max_lag_events: default_grpc_max_lag_events(),
            keepalive_interval_secs: default_grpc_keepalive_interval(),
            keepalive_timeout_secs: default_grpc_keepalive_timeout(),
            multi_region_enabled: default_grpc_multi_region_enabled(),
            dedup_window_secs: default_grpc_dedup_window_secs(),
            dedup_eviction_interval_secs: default_grpc_dedup_eviction_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimitConfig {
    #[serde(default = "default_global_rps")]
    pub global_rps: u32,
    #[serde(default = "default_global_tps")]
    pub global_tps: u32,
    #[serde(default = "default_per_ip_rps")]
    pub default_rps: u32,
    #[serde(default = "default_per_ip_tps")]
    pub default_tps: u32,
    #[serde(default = "default_burst_rps")]
    pub default_burst_rps: u32,
    #[serde(default = "default_burst_tps")]
    pub default_burst_tps: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct CpuConfig {
    #[serde(default)]
    pub cores: Vec<usize>,
    #[serde(default)]
    pub worker_threads: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct FirewallConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_fw_backend")]
    pub backend: String,
    #[serde(default = "default_chain_name")]
    pub chain_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WhitelistEntry {
    pub ip: IpAddr,
    #[serde(default)]
    pub label: String,
    pub rps: Option<u32>,
    pub tps: Option<u32>,
    pub burst_rps: Option<u32>,
    pub burst_tps: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiKeyEntry {
    pub key: String,
    #[serde(default)]
    pub label: String,
    pub rps: Option<u32>,
    pub tps: Option<u32>,
    pub burst_rps: Option<u32>,
    pub burst_tps: Option<u32>,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub revoked: bool,
}

/// Legacy single-upstream config (auto-converted to nodes[0])
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LegacyUpstreamConfig {
    pub rpc_url: String,
    pub ws_url: String,
    pub grpc_url: String,
    pub arpc_url: String,
    #[serde(default)]
    pub grpc_x_token: String,
    #[serde(default = "default_pool_max_idle")]
    pub rpc_pool_max_idle: usize,
    #[serde(default = "default_pool_idle_timeout")]
    pub rpc_pool_idle_timeout_secs: u64,
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut config: Config = toml::from_str(&content)?;

        // Legacy fallback: convert [upstream] to [[nodes]] if no nodes defined
        if config.nodes.is_empty() {
            if let Some(upstream) = config.upstream.take() {
                config.nodes.push(NodeConfig {
                    id: "primary".into(),
                    label: "Primary".into(),
                    region: "default".into(),
                    priority: 1,
                    rpc_url: upstream.rpc_url,
                    ws_url: upstream.ws_url,
                    grpc_url: upstream.grpc_url,
                    arpc_url: upstream.arpc_url,
                    grpc_x_token: upstream.grpc_x_token,
                    iris_url: None,
                    rpc_pool_max_idle: upstream.rpc_pool_max_idle,
                    rpc_pool_idle_timeout_secs: upstream.rpc_pool_idle_timeout_secs,
                });
                tracing::info!("Converted legacy [upstream] config to [[nodes]] format");
            }
        }
        // Clear legacy field after conversion
        config.upstream = None;

        config.validate()?;
        Ok(config)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let toml_string = toml::to_string_pretty(self)?;
        let tmp = path.with_extension("toml.tmp");
        std::fs::write(&tmp, &toml_string)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.nodes.is_empty() {
            anyhow::bail!("At least one [[nodes]] entry is required");
        }

        let total_rps: u32 = self
            .whitelist
            .iter()
            .map(|w| w.rps.unwrap_or(self.rate_limits.default_rps))
            .sum();
        let total_tps: u32 = self
            .whitelist
            .iter()
            .map(|w| w.tps.unwrap_or(self.rate_limits.default_tps))
            .sum();

        if total_rps > self.rate_limits.global_rps {
            tracing::warn!(
                "Total allocated RPS ({}) exceeds global limit ({})",
                total_rps,
                self.rate_limits.global_rps
            );
        }
        if total_tps > self.rate_limits.global_tps {
            tracing::warn!(
                "Total allocated TPS ({}) exceeds global limit ({})",
                total_tps,
                self.rate_limits.global_tps
            );
        }
        Ok(())
    }

    pub fn whitelist_set(&self) -> HashSet<IpAddr> {
        self.whitelist.iter().map(|w| w.ip).collect()
    }

    pub fn get_ip_rps(&self, ip: &IpAddr) -> u32 {
        self.whitelist
            .iter()
            .find(|w| &w.ip == ip)
            .and_then(|w| w.rps)
            .unwrap_or(self.rate_limits.default_rps)
    }

    pub fn get_ip_tps(&self, ip: &IpAddr) -> u32 {
        self.whitelist
            .iter()
            .find(|w| &w.ip == ip)
            .and_then(|w| w.tps)
            .unwrap_or(self.rate_limits.default_tps)
    }

    pub fn get_ip_burst_rps(&self, ip: &IpAddr) -> u32 {
        self.whitelist
            .iter()
            .find(|w| &w.ip == ip)
            .and_then(|w| w.burst_rps)
            .unwrap_or(self.rate_limits.default_burst_rps)
    }

    pub fn get_ip_burst_tps(&self, ip: &IpAddr) -> u32 {
        self.whitelist
            .iter()
            .find(|w| &w.ip == ip)
            .and_then(|w| w.burst_tps)
            .unwrap_or(self.rate_limits.default_burst_tps)
    }

    pub fn get_send_tx_node_ids(&self) -> Vec<String> {
        if self.routing.send_tx_targets == "all" {
            self.nodes.iter().map(|n| n.id.clone()).collect()
        } else {
            self.routing
                .send_tx_targets
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        }
    }

    pub fn get_grpc_eligible_node_ids(&self) -> Vec<String> {
        if self.routing.grpc_regions.is_empty() {
            self.nodes.iter().map(|n| n.id.clone()).collect()
        } else {
            self.routing.grpc_regions.clone()
        }
    }
}

// Defaults
fn default_rpc_bind() -> String { "0.0.0.0:8899".into() }
fn default_ws_bind() -> String { "0.0.0.0:8900".into() }
fn default_grpc_bind() -> String { "0.0.0.0:10101".into() }
fn default_arpc_bind() -> String { "0.0.0.0:20202".into() }
fn default_dashboard_bind() -> String { "127.0.0.1:9000".into() }
fn default_dashboard_user() -> String { "admin".into() }
fn default_dashboard_pass() -> String { "admin".into() }
fn default_data_dir() -> String { "/var/lib/solana-rpc-cluster".into() }
fn default_log_level() -> String { "info".into() }
fn default_pool_max_idle() -> usize { 32 }
fn default_pool_idle_timeout() -> u64 { 60 }
fn default_priority() -> u32 { 10 }
fn default_global_rps() -> u32 { 1250 }
fn default_global_tps() -> u32 { 250 }
fn default_per_ip_rps() -> u32 { 200 }
fn default_per_ip_tps() -> u32 { 50 }
fn default_burst_rps() -> u32 { 20 }
fn default_burst_tps() -> u32 { 5 }
fn default_fw_backend() -> String { "nftables".into() }
fn default_chain_name() -> String { "solana-rpc-cluster".into() }
fn default_check_interval() -> u64 { 5 }
fn default_unhealthy_threshold() -> u32 { 3 }
fn default_healthy_threshold() -> u32 { 2 }
fn default_timeout_ms() -> u64 { 3000 }
fn default_send_tx_targets() -> String { "all".into() }
fn default_grpc_conn_window() -> u32 { 16 * 1024 * 1024 }
fn default_grpc_stream_window() -> u32 { 4 * 1024 * 1024 }
fn default_grpc_adaptive_window() -> bool { true }
fn default_grpc_concurrency_limit() -> usize { 256 }
fn default_grpc_broadcast_capacity() -> usize { 65536 }
fn default_grpc_client_channel_capacity() -> usize { 4096 }
fn default_grpc_max_lag_events() -> u64 { 3 }
fn default_grpc_keepalive_interval() -> u64 { 20 }
fn default_grpc_keepalive_timeout() -> u64 { 10 }
fn default_grpc_multi_region_enabled() -> bool { true }
fn default_grpc_dedup_window_secs() -> u64 { 30 }
fn default_grpc_dedup_eviction_interval_secs() -> u64 { 5 }
