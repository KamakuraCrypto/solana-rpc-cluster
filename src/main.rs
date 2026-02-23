mod cluster;
mod config;
mod dashboard;
mod error;
mod middleware;
mod proxy;
mod shutdown;
mod stats;

use crate::cluster::ClusterState;
use crate::config::Config;
use crate::dashboard::DashboardState;
use crate::middleware::api_keys::ApiKeyStore;
use crate::middleware::ip_whitelist::IpWhitelist;
use crate::middleware::rate_limiter::RateLimiterMiddleware;
use crate::proxy::grpc_arpc::{ArpcV1Proxy, ArpcV2Proxy};
use crate::proxy::grpc_mux::GrpcMultiplexer;
use crate::proxy::grpc_yellowstone::YellowstoneProxy;
use crate::proxy::http_rpc::HttpRpcProxy;
use crate::proxy::websocket::WebSocketProxy;
use crate::stats::Stats;
use clap::Parser;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Parser)]
#[command(name = "solana-rpc-cluster", about = "Solana RPC Cluster Manager")]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "/etc/solana-rpc-cluster/config.toml")]
    config: PathBuf,
}

fn main() {
    let cli = Cli::parse();

    // Load config
    let config = Config::load(&cli.config).unwrap_or_else(|e| {
        eprintln!("Failed to load config from {:?}: {}", cli.config, e);
        std::process::exit(1);
    });

    // Build runtime with CPU pinning
    let runtime = build_runtime(&config);

    runtime.block_on(async move {
        // Initialize tracing
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| config.general.log_level.parse().unwrap_or_default()),
            )
            .init();

        tracing::info!("Solana RPC Cluster starting...");

        // Ensure data directory exists
        let data_dir = Path::new(&config.general.data_dir);
        if let Err(e) = std::fs::create_dir_all(data_dir) {
            tracing::warn!("Failed to create data dir {:?}: {}", data_dir, e);
        }

        // Shared state
        let stats = Arc::new(Stats::new());

        // Restore persisted stats
        let stats_path = data_dir.join("stats.json");
        if let Some(persisted) = Stats::load_from_file(&stats_path) {
            stats.restore(&persisted);
            tracing::info!("Restored persisted stats ({} IPs)", persisted.per_ip.len());
        }

        let whitelist = IpWhitelist::new(config.whitelist_set());
        let rate_limiter = RateLimiterMiddleware::new(&config);
        let api_key_store = ApiKeyStore::new(&config.api_keys);
        let config_shared = Arc::new(RwLock::new(config.clone()));

        // Build cluster state (nodes, router, grpc pool, health checker)
        let cluster = ClusterState::new(&config);
        let router = cluster.router.clone();
        let grpc_pool = cluster.grpc_pool.clone();

        // Spawn health checker background task
        cluster.health_checker.spawn();
        tracing::info!("Health checker started for {} nodes", cluster.nodes.len());

        // Create gRPC broadcast multiplexer (1 upstream stream → N downstream)
        let grpc_mux = Arc::new(GrpcMultiplexer::new(router.clone(), grpc_pool.clone(), &config.grpc));
        tracing::info!("gRPC multiplexer initialized");

        // Stats counter reset task (every 1 second)
        {
            let stats = stats.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    stats.reset_rolling_counters();
                }
            });
        }

        // Stats persistence task (every 60 seconds)
        {
            let stats = stats.clone();
            let stats_path = stats_path.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    interval.tick().await;
                    if let Err(e) = stats.save_to_file(&stats_path) {
                        tracing::warn!("Failed to save stats: {}", e);
                    }
                }
            });
        }

        // SIGHUP handler for hot-reload
        {
            let mut sighup_rx = shutdown::setup_sighup_handler();
            let config_path = cli.config.clone();
            let whitelist = whitelist.clone();
            let rate_limiter = rate_limiter.clone();
            let api_key_store = api_key_store.clone();
            let config_shared = config_shared.clone();

            tokio::spawn(async move {
                while sighup_rx.recv().await.is_some() {
                    match Config::load(&config_path) {
                        Ok(new_config) => {
                            whitelist.update(new_config.whitelist_set());
                            rate_limiter.reload(&new_config);
                            api_key_store.reload(&new_config.api_keys);
                            *config_shared.write().await = new_config;
                            tracing::info!("Config reloaded successfully");
                        }
                        Err(e) => {
                            tracing::error!("Failed to reload config: {}", e);
                        }
                    }
                }
            });
        }

        // Optional firewall sync
        if config.firewall.enabled {
            let ports = vec![8899, 8900, 10101, 20202];
            if let Err(e) =
                middleware::firewall::sync_firewall(&config.whitelist_set(), &config.firewall, &ports)
                    .await
            {
                tracing::warn!("Firewall sync failed: {}", e);
            }
        }

        // Parse bind addresses
        let rpc_addr = config.general.rpc_bind.parse().expect("Invalid rpc_bind");
        let ws_addr = config.general.ws_bind.parse().expect("Invalid ws_bind");
        let grpc_addr = config.general.grpc_bind.parse().expect("Invalid grpc_bind");
        let arpc_addr = config.general.arpc_bind.parse().expect("Invalid arpc_bind");
        let dashboard_addr = config
            .general
            .dashboard_bind
            .parse()
            .expect("Invalid dashboard_bind");

        // Create proxy instances
        let http_proxy = HttpRpcProxy::new(
            router.clone(),
            whitelist.clone(),
            api_key_store.clone(),
            rate_limiter.clone(),
            stats.clone(),
        );

        let ws_proxy = WebSocketProxy::new(
            router.clone(),
            whitelist.clone(),
            api_key_store.clone(),
            rate_limiter.clone(),
            stats.clone(),
        );

        let grpc_proxy = YellowstoneProxy::new(
            router.clone(),
            grpc_pool.clone(),
            grpc_mux.clone(),
            whitelist.clone(),
            api_key_store.clone(),
            rate_limiter.clone(),
            stats.clone(),
        );

        let arpc_v1 = ArpcV1Proxy::new(
            router.clone(),
            grpc_pool.clone(),
            whitelist.clone(),
            api_key_store.clone(),
            rate_limiter.clone(),
            stats.clone(),
        );

        let arpc_v2 = ArpcV2Proxy::new(
            router.clone(),
            grpc_pool.clone(),
            whitelist.clone(),
            api_key_store.clone(),
            rate_limiter.clone(),
            stats.clone(),
        );

        let dashboard_state = DashboardState {
            stats: stats.clone(),
            whitelist: whitelist.clone(),
            rate_limiter: rate_limiter.clone(),
            api_key_store: api_key_store.clone(),
            config: config_shared.clone(),
            config_path: cli.config.clone(),
            cluster_nodes: cluster.nodes.clone(),
            dashboard_user: config.general.dashboard_user.clone(),
            dashboard_pass: config.general.dashboard_pass.clone(),
        };

        tracing::info!("Starting all proxy servers...");

        // Run all servers concurrently
        tokio::select! {
            r = http_proxy.run(rpc_addr) => {
                if let Err(e) = r { tracing::error!("HTTP RPC proxy failed: {}", e); }
            }
            r = ws_proxy.run(ws_addr) => {
                if let Err(e) = r { tracing::error!("WebSocket proxy failed: {}", e); }
            }
            r = proxy::grpc_yellowstone::run_grpc_proxy(grpc_addr, grpc_proxy, &config.grpc) => {
                if let Err(e) = r { tracing::error!("gRPC proxy failed: {}", e); }
            }
            r = proxy::grpc_arpc::run_arpc_proxy(arpc_addr, arpc_v1, arpc_v2, &config.grpc) => {
                if let Err(e) = r { tracing::error!("aRPC proxy failed: {}", e); }
            }
            r = dashboard::run_dashboard(dashboard_addr, dashboard_state) => {
                if let Err(e) = r { tracing::error!("Dashboard failed: {}", e); }
            }
            _ = shutdown::shutdown_signal() => {
                tracing::info!("Shutting down gracefully...");
            }
        }

        // Save stats on shutdown
        if let Err(e) = stats.save_to_file(&stats_path) {
            tracing::warn!("Failed to save stats on shutdown: {}", e);
        } else {
            tracing::info!("Stats saved to {:?}", stats_path);
        }

        tracing::info!("Solana RPC Cluster stopped.");
    });
}

fn build_runtime(config: &Config) -> tokio::runtime::Runtime {
    let worker_threads = config
        .cpu
        .worker_threads
        .unwrap_or_else(|| {
            if config.cpu.cores.is_empty() {
                num_cpus()
            } else {
                config.cpu.cores.len().max(1)
            }
        });

    let cores = config.cpu.cores.clone();
    let thread_idx = Arc::new(AtomicUsize::new(0));

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .worker_threads(worker_threads)
        .enable_all()
        .thread_name("solana-rpc-cluster");

    if !cores.is_empty() {
        let core_ids: Vec<core_affinity::CoreId> = cores
            .iter()
            .map(|&c| core_affinity::CoreId { id: c })
            .collect();
        let core_ids = Arc::new(core_ids);

        builder.on_thread_start(move || {
            let idx = thread_idx.fetch_add(1, Ordering::SeqCst);
            if !core_ids.is_empty() {
                let core = core_ids[idx % core_ids.len()];
                if core_affinity::set_for_current(core) {
                    eprintln!("Pinned worker thread {} to core {}", idx, core.id);
                }
            }
        });
    }

    builder.build().expect("Failed to build tokio runtime")
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
