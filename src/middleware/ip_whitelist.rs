use arc_swap::ArcSwap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;

#[derive(Clone)]
pub struct IpWhitelist {
    whitelist: Arc<ArcSwap<HashSet<IpAddr>>>,
}

impl IpWhitelist {
    pub fn new(ips: HashSet<IpAddr>) -> Self {
        Self {
            whitelist: Arc::new(ArcSwap::from_pointee(ips)),
        }
    }

    pub fn is_allowed(&self, ip: &IpAddr) -> bool {
        self.whitelist.load().contains(ip)
    }

    pub fn update(&self, ips: HashSet<IpAddr>) {
        let count = ips.len();
        self.whitelist.store(Arc::new(ips));
        tracing::info!("IP whitelist updated: {} IPs", count);
    }

    pub fn get_all(&self) -> HashSet<IpAddr> {
        (**self.whitelist.load()).clone()
    }

    pub fn add_ip(&self, ip: IpAddr) {
        let mut current = (**self.whitelist.load()).clone();
        current.insert(ip);
        self.whitelist.store(Arc::new(current));
        tracing::info!("Added IP to whitelist: {}", ip);
    }

    pub fn remove_ip(&self, ip: &IpAddr) {
        let mut current = (**self.whitelist.load()).clone();
        current.remove(ip);
        self.whitelist.store(Arc::new(current));
        tracing::info!("Removed IP from whitelist: {}", ip);
    }
}
