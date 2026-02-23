use crate::config::Config;
use dashmap::DashMap;
use governor::{Quota, RateLimiter as GovRateLimiter};
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

type Limiter = GovRateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
>;

struct IpLimiters {
    rps: Arc<Limiter>,
    tps: Arc<Limiter>,
}

#[derive(Clone)]
pub struct RateLimiterMiddleware {
    ip_limiters: Arc<DashMap<IpAddr, IpLimiters>>,
    key_limiters: Arc<DashMap<String, IpLimiters>>,
    config: Arc<arc_swap::ArcSwap<Config>>,
}

impl RateLimiterMiddleware {
    pub fn new(config: &Config) -> Self {
        let ip_limiters = DashMap::new();

        for entry in &config.whitelist {
            let rps = entry.rps.unwrap_or(config.rate_limits.default_rps);
            let tps = entry.tps.unwrap_or(config.rate_limits.default_tps);
            let burst_rps = entry.burst_rps.unwrap_or(config.rate_limits.default_burst_rps);
            let burst_tps = entry.burst_tps.unwrap_or(config.rate_limits.default_burst_tps);

            ip_limiters.insert(
                entry.ip,
                IpLimiters {
                    rps: Arc::new(create_limiter(rps, burst_rps)),
                    tps: Arc::new(create_limiter(tps, burst_tps)),
                },
            );
        }

        let key_limiters = DashMap::new();
        for entry in &config.api_keys {
            if !entry.revoked {
                let rps = entry.rps.unwrap_or(config.rate_limits.default_rps);
                let tps = entry.tps.unwrap_or(config.rate_limits.default_tps);
                let burst_rps = entry.burst_rps.unwrap_or(config.rate_limits.default_burst_rps);
                let burst_tps = entry.burst_tps.unwrap_or(config.rate_limits.default_burst_tps);

                key_limiters.insert(
                    entry.key.clone(),
                    IpLimiters {
                        rps: Arc::new(create_limiter(rps, burst_rps)),
                        tps: Arc::new(create_limiter(tps, burst_tps)),
                    },
                );
            }
        }

        Self {
            ip_limiters: Arc::new(ip_limiters),
            key_limiters: Arc::new(key_limiters),
            config: Arc::new(arc_swap::ArcSwap::from_pointee(config.clone())),
        }
    }

    pub fn check(&self, ip: &IpAddr, is_send_tx: bool) -> Result<(), bool> {
        let limiters = self.get_or_create_limiters(ip);
        if limiters.rps.check().is_err() {
            return Err(false);
        }
        if is_send_tx && limiters.tps.check().is_err() {
            return Err(true);
        }
        Ok(())
    }

    pub fn check_rps(&self, ip: &IpAddr) -> Result<(), ()> {
        let limiters = self.get_or_create_limiters(ip);
        limiters.rps.check().map_err(|_| ())
    }

    pub fn check_by_key(&self, key: &str, is_send_tx: bool) -> Result<(), bool> {
        let limiters = self.get_or_create_key_limiters(key);
        if limiters.rps.check().is_err() {
            return Err(false);
        }
        if is_send_tx && limiters.tps.check().is_err() {
            return Err(true);
        }
        Ok(())
    }

    pub fn check_rps_by_key(&self, key: &str) -> Result<(), ()> {
        let limiters = self.get_or_create_key_limiters(key);
        limiters.rps.check().map_err(|_| ())
    }

    fn get_or_create_limiters(&self, ip: &IpAddr) -> IpLimiters {
        if let Some(existing) = self.ip_limiters.get(ip) {
            return IpLimiters {
                rps: existing.rps.clone(),
                tps: existing.tps.clone(),
            };
        }

        let config = self.config.load();
        let rps = config.get_ip_rps(ip);
        let tps = config.get_ip_tps(ip);
        let burst_rps = config.get_ip_burst_rps(ip);
        let burst_tps = config.get_ip_burst_tps(ip);

        let limiters = IpLimiters {
            rps: Arc::new(create_limiter(rps, burst_rps)),
            tps: Arc::new(create_limiter(tps, burst_tps)),
        };

        self.ip_limiters.insert(*ip, IpLimiters {
            rps: limiters.rps.clone(),
            tps: limiters.tps.clone(),
        });

        limiters
    }

    fn get_or_create_key_limiters(&self, key: &str) -> IpLimiters {
        if let Some(existing) = self.key_limiters.get(key) {
            return IpLimiters {
                rps: existing.rps.clone(),
                tps: existing.tps.clone(),
            };
        }

        let config = self.config.load();
        let rps = config.rate_limits.default_rps;
        let tps = config.rate_limits.default_tps;
        let burst_rps = config.rate_limits.default_burst_rps;
        let burst_tps = config.rate_limits.default_burst_tps;

        let limiters = IpLimiters {
            rps: Arc::new(create_limiter(rps, burst_rps)),
            tps: Arc::new(create_limiter(tps, burst_tps)),
        };

        self.key_limiters.insert(key.to_string(), IpLimiters {
            rps: limiters.rps.clone(),
            tps: limiters.tps.clone(),
        });

        limiters
    }

    pub fn reload(&self, config: &Config) {
        self.config.store(Arc::new(config.clone()));
        self.ip_limiters.clear();
        self.key_limiters.clear();

        for entry in &config.whitelist {
            let rps = entry.rps.unwrap_or(config.rate_limits.default_rps);
            let tps = entry.tps.unwrap_or(config.rate_limits.default_tps);
            let burst_rps = entry.burst_rps.unwrap_or(config.rate_limits.default_burst_rps);
            let burst_tps = entry.burst_tps.unwrap_or(config.rate_limits.default_burst_tps);

            self.ip_limiters.insert(
                entry.ip,
                IpLimiters {
                    rps: Arc::new(create_limiter(rps, burst_rps)),
                    tps: Arc::new(create_limiter(tps, burst_tps)),
                },
            );
        }

        for entry in &config.api_keys {
            if !entry.revoked {
                let rps = entry.rps.unwrap_or(config.rate_limits.default_rps);
                let tps = entry.tps.unwrap_or(config.rate_limits.default_tps);
                let burst_rps = entry.burst_rps.unwrap_or(config.rate_limits.default_burst_rps);
                let burst_tps = entry.burst_tps.unwrap_or(config.rate_limits.default_burst_tps);

                self.key_limiters.insert(
                    entry.key.clone(),
                    IpLimiters {
                        rps: Arc::new(create_limiter(rps, burst_rps)),
                        tps: Arc::new(create_limiter(tps, burst_tps)),
                    },
                );
            }
        }

        tracing::info!("Rate limiters reloaded");
    }
}

fn create_limiter(rate_per_sec: u32, burst: u32) -> Limiter {
    let rate = NonZeroU32::new(rate_per_sec.max(1)).unwrap();
    let burst_size = NonZeroU32::new((rate_per_sec + burst).max(1)).unwrap();
    let quota = Quota::per_second(rate).allow_burst(burst_size);
    GovRateLimiter::direct(quota)
}
