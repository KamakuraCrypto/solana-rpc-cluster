use arc_swap::ArcSwap;
use crate::config::ApiKeyEntry;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct ApiKeyData {
    pub label: String,
    pub rps: Option<u32>,
    pub tps: Option<u32>,
    pub burst_rps: Option<u32>,
    pub burst_tps: Option<u32>,
    pub created_at: String,
}

#[derive(Clone)]
pub struct ApiKeyStore {
    keys: Arc<ArcSwap<HashMap<String, ApiKeyData>>>,
}

impl ApiKeyStore {
    pub fn new(entries: &[ApiKeyEntry]) -> Self {
        let mut map = HashMap::new();
        for entry in entries {
            if !entry.revoked {
                map.insert(
                    entry.key.clone(),
                    ApiKeyData {
                        label: entry.label.clone(),
                        rps: entry.rps,
                        tps: entry.tps,
                        burst_rps: entry.burst_rps,
                        burst_tps: entry.burst_tps,
                        created_at: entry.created_at.clone(),
                    },
                );
            }
        }
        Self {
            keys: Arc::new(ArcSwap::from_pointee(map)),
        }
    }

    pub fn validate(&self, key: &str) -> Option<ApiKeyData> {
        let store = self.keys.load();
        store.get(key).cloned()
    }

    pub fn add_key(&self, key: String, data: ApiKeyData) {
        let mut current = (**self.keys.load()).clone();
        current.insert(key, data);
        self.keys.store(Arc::new(current));
    }

    pub fn revoke_key(&self, key: &str) -> bool {
        let mut current = (**self.keys.load()).clone();
        let removed = current.remove(key).is_some();
        if removed {
            self.keys.store(Arc::new(current));
        }
        removed
    }

    pub fn list_keys(&self) -> Vec<(String, ApiKeyData)> {
        let store = self.keys.load();
        store
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn reload(&self, entries: &[ApiKeyEntry]) {
        let mut map = HashMap::new();
        for entry in entries {
            if !entry.revoked {
                map.insert(
                    entry.key.clone(),
                    ApiKeyData {
                        label: entry.label.clone(),
                        rps: entry.rps,
                        tps: entry.tps,
                        burst_rps: entry.burst_rps,
                        burst_tps: entry.burst_tps,
                        created_at: entry.created_at.clone(),
                    },
                );
            }
        }
        self.keys.store(Arc::new(map));
    }
}

pub fn generate_api_key() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 16] = rng.gen();
    let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
    format!("sk_{}", hex)
}
