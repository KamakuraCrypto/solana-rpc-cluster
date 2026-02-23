use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Default)]
pub struct IpStats {
    pub active_rpc_conns: AtomicU64,
    pub active_ws_conns: AtomicU64,
    pub active_grpc_streams: AtomicU64,
    pub active_arpc_streams: AtomicU64,
    pub total_rpc_requests: AtomicU64,
    pub total_tx_sends: AtomicU64,
    pub total_ws_messages: AtomicU64,
    pub total_grpc_requests: AtomicU64,
    pub total_arpc_requests: AtomicU64,
    pub rate_limited_count: AtomicU64,
    pub error_count: AtomicU64,
    pub last_seen_epoch_ms: AtomicU64,
    // Rolling window counters (reset every second by background task)
    pub rps_current: AtomicU64,
    pub tps_current: AtomicU64,
}

#[derive(Serialize, Clone)]
pub struct IpStatsSnapshot {
    pub active_rpc_conns: u64,
    pub active_ws_conns: u64,
    pub active_grpc_streams: u64,
    pub active_arpc_streams: u64,
    pub total_rpc_requests: u64,
    pub total_tx_sends: u64,
    pub total_ws_messages: u64,
    pub total_grpc_requests: u64,
    pub total_arpc_requests: u64,
    pub rate_limited_count: u64,
    pub error_count: u64,
    pub rps_current: u64,
    pub tps_current: u64,
    pub last_seen_epoch_ms: u64,
}

impl IpStats {
    pub fn snapshot(&self) -> IpStatsSnapshot {
        IpStatsSnapshot {
            active_rpc_conns: self.active_rpc_conns.load(Ordering::Relaxed),
            active_ws_conns: self.active_ws_conns.load(Ordering::Relaxed),
            active_grpc_streams: self.active_grpc_streams.load(Ordering::Relaxed),
            active_arpc_streams: self.active_arpc_streams.load(Ordering::Relaxed),
            total_rpc_requests: self.total_rpc_requests.load(Ordering::Relaxed),
            total_tx_sends: self.total_tx_sends.load(Ordering::Relaxed),
            total_ws_messages: self.total_ws_messages.load(Ordering::Relaxed),
            total_grpc_requests: self.total_grpc_requests.load(Ordering::Relaxed),
            total_arpc_requests: self.total_arpc_requests.load(Ordering::Relaxed),
            rate_limited_count: self.rate_limited_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            rps_current: self.rps_current.load(Ordering::Relaxed),
            tps_current: self.tps_current.load(Ordering::Relaxed),
            last_seen_epoch_ms: self.last_seen_epoch_ms.load(Ordering::Relaxed),
        }
    }

    fn touch(&self) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_seen_epoch_ms.store(now_ms, Ordering::Relaxed);
    }
}

pub struct Stats {
    pub per_ip: DashMap<IpAddr, Arc<IpStats>>,
    pub start_time: Instant,
    // Global counters
    pub global_rps_current: AtomicU64,
    pub global_tps_current: AtomicU64,
    pub global_total_requests: AtomicU64,
}

#[derive(Serialize, Clone)]
pub struct GlobalStatsSnapshot {
    pub uptime_secs: u64,
    pub total_requests: u64,
    pub global_rps: u64,
    pub global_tps: u64,
    pub connected_ips: usize,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            per_ip: DashMap::new(),
            start_time: Instant::now(),
            global_rps_current: AtomicU64::new(0),
            global_tps_current: AtomicU64::new(0),
            global_total_requests: AtomicU64::new(0),
        }
    }

    pub fn get_or_create(&self, ip: IpAddr) -> Arc<IpStats> {
        self.per_ip
            .entry(ip)
            .or_insert_with(|| Arc::new(IpStats::default()))
            .clone()
    }

    pub fn record_rpc(&self, ip: IpAddr, is_send_tx: bool) {
        let stats = self.get_or_create(ip);
        stats.total_rpc_requests.fetch_add(1, Ordering::Relaxed);
        stats.rps_current.fetch_add(1, Ordering::Relaxed);
        stats.touch();

        self.global_total_requests.fetch_add(1, Ordering::Relaxed);
        self.global_rps_current.fetch_add(1, Ordering::Relaxed);

        if is_send_tx {
            stats.total_tx_sends.fetch_add(1, Ordering::Relaxed);
            stats.tps_current.fetch_add(1, Ordering::Relaxed);
            self.global_tps_current.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_rate_limited(&self, ip: IpAddr) {
        let stats = self.get_or_create(ip);
        stats.rate_limited_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self, ip: IpAddr) {
        let stats = self.get_or_create(ip);
        stats.error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn global_snapshot(&self) -> GlobalStatsSnapshot {
        GlobalStatsSnapshot {
            uptime_secs: self.start_time.elapsed().as_secs(),
            total_requests: self.global_total_requests.load(Ordering::Relaxed),
            global_rps: self.global_rps_current.load(Ordering::Relaxed),
            global_tps: self.global_tps_current.load(Ordering::Relaxed),
            connected_ips: self.per_ip.len(),
        }
    }

    /// Reset rolling window counters (called every second)
    pub fn reset_rolling_counters(&self) {
        self.global_rps_current.store(0, Ordering::Relaxed);
        self.global_tps_current.store(0, Ordering::Relaxed);
        for entry in self.per_ip.iter() {
            entry.value().rps_current.store(0, Ordering::Relaxed);
            entry.value().tps_current.store(0, Ordering::Relaxed);
        }
    }

    // --- Persistence ---

    pub fn save_to_file(&self, path: &Path) -> anyhow::Result<()> {
        let persisted = PersistedStats {
            global_total_requests: self.global_total_requests.load(Ordering::Relaxed),
            per_ip: self
                .per_ip
                .iter()
                .map(|entry| {
                    let ip = entry.key().to_string();
                    let s = entry.value();
                    (
                        ip,
                        PersistedIpStats {
                            total_rpc_requests: s.total_rpc_requests.load(Ordering::Relaxed),
                            total_tx_sends: s.total_tx_sends.load(Ordering::Relaxed),
                            total_ws_messages: s.total_ws_messages.load(Ordering::Relaxed),
                            total_grpc_requests: s.total_grpc_requests.load(Ordering::Relaxed),
                            total_arpc_requests: s.total_arpc_requests.load(Ordering::Relaxed),
                            rate_limited_count: s.rate_limited_count.load(Ordering::Relaxed),
                            error_count: s.error_count.load(Ordering::Relaxed),
                        },
                    )
                })
                .collect(),
        };
        let json = serde_json::to_string_pretty(&persisted)?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn load_from_file(path: &Path) -> Option<PersistedStats> {
        let content = std::fs::read_to_string(path).ok()?;
        serde_json::from_str(&content).ok()
    }

    pub fn restore(&self, persisted: &PersistedStats) {
        self.global_total_requests
            .store(persisted.global_total_requests, Ordering::Relaxed);
        for (ip_str, pstats) in &persisted.per_ip {
            if let Ok(ip) = ip_str.parse::<IpAddr>() {
                let stats = self.get_or_create(ip);
                stats
                    .total_rpc_requests
                    .store(pstats.total_rpc_requests, Ordering::Relaxed);
                stats
                    .total_tx_sends
                    .store(pstats.total_tx_sends, Ordering::Relaxed);
                stats
                    .total_ws_messages
                    .store(pstats.total_ws_messages, Ordering::Relaxed);
                stats
                    .total_grpc_requests
                    .store(pstats.total_grpc_requests, Ordering::Relaxed);
                stats
                    .total_arpc_requests
                    .store(pstats.total_arpc_requests, Ordering::Relaxed);
                stats
                    .rate_limited_count
                    .store(pstats.rate_limited_count, Ordering::Relaxed);
                stats
                    .error_count
                    .store(pstats.error_count, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct PersistedStats {
    pub global_total_requests: u64,
    pub per_ip: HashMap<String, PersistedIpStats>,
}

#[derive(Serialize, Deserialize)]
pub struct PersistedIpStats {
    pub total_rpc_requests: u64,
    pub total_tx_sends: u64,
    pub total_ws_messages: u64,
    pub total_grpc_requests: u64,
    pub total_arpc_requests: u64,
    pub rate_limited_count: u64,
    pub error_count: u64,
}
