use super::DashboardState;
use crate::config::{ApiKeyEntry, WhitelistEntry};
use crate::middleware::api_keys::{generate_api_key, ApiKeyData};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::Ordering;

fn auth(state: &DashboardState, headers: &HeaderMap) -> Result<(), Response> {
    super::check_basic_auth(headers, &state.dashboard_user, &state.dashboard_pass)
}

fn last_msg_age_secs(health: &crate::cluster::health::NodeHealth) -> u64 {
    let last_ms = health.last_msg_ms.load(Ordering::Relaxed);
    if last_ms == 0 {
        return u64::MAX;
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    now_ms.saturating_sub(last_ms) / 1000
}

async fn persist_config(state: &DashboardState) {
    let config = state.config.read().await;
    if let Err(e) = config.save(&state.config_path) {
        tracing::warn!("Failed to persist config: {}", e);
    }
}

// ═══════════════════════════════════════════════════════════
// Stats
// ═══════════════════════════════════════════════════════════

#[derive(Serialize)]
pub struct StatsResponse {
    pub global: crate::stats::GlobalStatsSnapshot,
    pub per_ip: HashMap<String, crate::stats::IpStatsSnapshot>,
}

pub async fn get_stats(
    State(state): State<DashboardState>,
    headers: HeaderMap,
) -> Result<Json<StatsResponse>, Response> {
    auth(&state, &headers)?;

    let global = state.stats.global_snapshot();
    let mut per_ip = HashMap::new();

    for entry in state.stats.per_ip.iter() {
        per_ip.insert(entry.key().to_string(), entry.value().snapshot());
    }

    Ok(Json(StatsResponse { global, per_ip }))
}

// ═══════════════════════════════════════════════════════════
// IP Management
// ═══════════════════════════════════════════════════════════

#[derive(Serialize)]
pub struct IpEntry {
    pub ip: String,
    pub label: String,
    pub rps: u32,
    pub tps: u32,
    pub burst_rps: u32,
    pub burst_tps: u32,
}

pub async fn list_ips(
    State(state): State<DashboardState>,
    headers: HeaderMap,
) -> Result<Json<Vec<IpEntry>>, Response> {
    auth(&state, &headers)?;

    let config = state.config.read().await;
    let entries: Vec<IpEntry> = config
        .whitelist
        .iter()
        .map(|w| IpEntry {
            ip: w.ip.to_string(),
            label: w.label.clone(),
            rps: w.rps.unwrap_or(config.rate_limits.default_rps),
            tps: w.tps.unwrap_or(config.rate_limits.default_tps),
            burst_rps: w.burst_rps.unwrap_or(config.rate_limits.default_burst_rps),
            burst_tps: w.burst_tps.unwrap_or(config.rate_limits.default_burst_tps),
        })
        .collect();

    Ok(Json(entries))
}

#[derive(Deserialize)]
pub struct AddIpRequest {
    pub ip: String,
    #[serde(default)]
    pub label: String,
    pub rps: Option<u32>,
    pub tps: Option<u32>,
    pub burst_rps: Option<u32>,
    pub burst_tps: Option<u32>,
}

pub async fn add_ip(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Json(body): Json<AddIpRequest>,
) -> Result<Json<serde_json::Value>, Response> {
    auth(&state, &headers)?;

    let ip: IpAddr = body
        .ip
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST.into_response())?;

    state.whitelist.add_ip(ip);

    {
        let mut config = state.config.write().await;
        config.whitelist.push(WhitelistEntry {
            ip,
            label: body.label,
            rps: body.rps,
            tps: body.tps,
            burst_rps: body.burst_rps,
            burst_tps: body.burst_tps,
        });
        state.rate_limiter.reload(&config);
    }

    persist_config(&state).await;

    Ok(Json(
        serde_json::json!({"status": "ok", "ip": ip.to_string()}),
    ))
}

pub async fn update_ip(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Path(ip_str): Path<String>,
    Json(body): Json<AddIpRequest>,
) -> Result<Json<serde_json::Value>, Response> {
    auth(&state, &headers)?;

    let ip: IpAddr = ip_str
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST.into_response())?;

    {
        let mut config = state.config.write().await;
        if let Some(entry) = config.whitelist.iter_mut().find(|w| w.ip == ip) {
            if !body.label.is_empty() {
                entry.label = body.label;
            }
            if body.rps.is_some() {
                entry.rps = body.rps;
            }
            if body.tps.is_some() {
                entry.tps = body.tps;
            }
            if body.burst_rps.is_some() {
                entry.burst_rps = body.burst_rps;
            }
            if body.burst_tps.is_some() {
                entry.burst_tps = body.burst_tps;
            }
            state.rate_limiter.reload(&config);
        } else {
            return Err(StatusCode::NOT_FOUND.into_response());
        }
    }

    persist_config(&state).await;

    Ok(Json(serde_json::json!({"status": "ok"})))
}

pub async fn remove_ip(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Path(ip_str): Path<String>,
) -> Result<Json<serde_json::Value>, Response> {
    auth(&state, &headers)?;

    let ip: IpAddr = ip_str
        .parse()
        .map_err(|_| StatusCode::BAD_REQUEST.into_response())?;

    state.whitelist.remove_ip(&ip);

    {
        let mut config = state.config.write().await;
        config.whitelist.retain(|w| w.ip != ip);
        state.rate_limiter.reload(&config);
    }

    persist_config(&state).await;

    Ok(Json(
        serde_json::json!({"status": "ok", "removed": ip.to_string()}),
    ))
}

// ═══════════════════════════════════════════════════════════
// Node Health
// ═══════════════════════════════════════════════════════════

#[derive(Serialize)]
pub struct NodeHealthEntry {
    pub id: String,
    pub label: String,
    pub region: String,
    pub status: String,
    pub latency_ms: u64,
    pub last_slot: u64,
    pub uptime_pct: f64,
    pub iris_healthy: bool,
    pub iris_configured: bool,
    pub wins: u64,
    pub dupes: u64,
    pub last_msg_age_secs: u64,
}

pub async fn list_nodes(
    State(state): State<DashboardState>,
    headers: HeaderMap,
) -> Result<Json<Vec<NodeHealthEntry>>, Response> {
    auth(&state, &headers)?;

    let nodes: Vec<NodeHealthEntry> = state
        .cluster_nodes
        .iter()
        .map(|n| NodeHealthEntry {
            id: n.config.id.clone(),
            label: n.config.label.clone(),
            region: n.config.region.clone(),
            status: n.health.get_status().as_str().to_string(),
            latency_ms: n.health.latency_ms.load(Ordering::Relaxed),
            last_slot: n.health.last_slot.load(Ordering::Relaxed),
            uptime_pct: n.health.uptime_pct(),
            iris_healthy: n.health.iris_healthy.load(Ordering::Relaxed),
            iris_configured: n.config.iris_url.is_some(),
            wins: n.health.wins.load(Ordering::Relaxed),
            dupes: n.health.dupes.load(Ordering::Relaxed),
            last_msg_age_secs: last_msg_age_secs(&n.health),
        })
        .collect();

    Ok(Json(nodes))
}

// ═══════════════════════════════════════════════════════════
// API Key Management
// ═══════════════════════════════════════════════════════════

#[derive(Serialize)]
pub struct ApiKeyResponse {
    pub key_prefix: String,
    pub label: String,
    pub rps: Option<u32>,
    pub tps: Option<u32>,
    pub created_at: String,
}

pub async fn list_api_keys(
    State(state): State<DashboardState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ApiKeyResponse>>, Response> {
    auth(&state, &headers)?;

    let keys: Vec<ApiKeyResponse> = state
        .api_key_store
        .list_keys()
        .into_iter()
        .map(|(k, data)| {
            let prefix = mask_key(&k);
            ApiKeyResponse {
                key_prefix: prefix,
                label: data.label,
                rps: data.rps,
                tps: data.tps,
                created_at: data.created_at,
            }
        })
        .collect();

    Ok(Json(keys))
}

#[derive(Deserialize)]
pub struct CreateKeyRequest {
    #[serde(default)]
    pub label: String,
    pub rps: Option<u32>,
    pub tps: Option<u32>,
    pub burst_rps: Option<u32>,
    pub burst_tps: Option<u32>,
}

pub async fn create_api_key(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Json(body): Json<CreateKeyRequest>,
) -> Result<Json<serde_json::Value>, Response> {
    auth(&state, &headers)?;

    let key = generate_api_key();
    let now = chrono::Utc::now().to_rfc3339();

    state.api_key_store.add_key(
        key.clone(),
        ApiKeyData {
            label: body.label.clone(),
            rps: body.rps,
            tps: body.tps,
            burst_rps: body.burst_rps,
            burst_tps: body.burst_tps,
            created_at: now.clone(),
        },
    );

    {
        let mut config = state.config.write().await;
        config.api_keys.push(ApiKeyEntry {
            key: key.clone(),
            label: body.label,
            rps: body.rps,
            tps: body.tps,
            burst_rps: body.burst_rps,
            burst_tps: body.burst_tps,
            created_at: now,
            revoked: false,
        });
        state.rate_limiter.reload(&config);
    }

    persist_config(&state).await;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "key": key,
    })))
}

pub async fn revoke_api_key(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    Path(prefix): Path<String>,
) -> Result<Json<serde_json::Value>, Response> {
    auth(&state, &headers)?;

    // Find the full key by prefix
    let full_key = {
        let keys = state.api_key_store.list_keys();
        keys.into_iter()
            .find(|(k, _)| k.starts_with(&prefix) || mask_key(k) == prefix)
            .map(|(k, _)| k)
    };

    if let Some(key) = full_key {
        state.api_key_store.revoke_key(&key);

        {
            let mut config = state.config.write().await;
            if let Some(entry) = config.api_keys.iter_mut().find(|e| e.key == key) {
                entry.revoked = true;
            }
            state.rate_limiter.reload(&config);
        }

        persist_config(&state).await;

        Ok(Json(serde_json::json!({"status": "ok"})))
    } else {
        Err(StatusCode::NOT_FOUND.into_response())
    }
}

fn mask_key(key: &str) -> String {
    if key.len() > 12 {
        format!("{}...{}", &key[..8], &key[key.len() - 4..])
    } else {
        key.to_string()
    }
}

// ═══════════════════════════════════════════════════════════
// Health Check
// ═══════════════════════════════════════════════════════════

pub async fn health_check(
    State(state): State<DashboardState>,
) -> Json<serde_json::Value> {
    let config = state.config.read().await;
    let uptime = state.stats.start_time.elapsed().as_secs();

    let nodes: Vec<serde_json::Value> = state
        .cluster_nodes
        .iter()
        .map(|n| {
            serde_json::json!({
                "id": n.config.id,
                "status": n.health.get_status().as_str(),
                "latency_ms": n.health.latency_ms.load(Ordering::Relaxed),
            })
        })
        .collect();

    Json(serde_json::json!({
        "status": "ok",
        "uptime_secs": uptime,
        "nodes": nodes,
        "whitelisted_ips": config.whitelist.len(),
        "api_keys": config.api_keys.iter().filter(|k| !k.revoked).count(),
    }))
}

// ═══════════════════════════════════════════════════════════
// WebSocket Live Stats
// ═══════════════════════════════════════════════════════════

#[derive(Serialize)]
struct WsStatsMessage {
    global: crate::stats::GlobalStatsSnapshot,
    per_ip: HashMap<String, crate::stats::IpStatsSnapshot>,
    ips: Vec<IpEntry>,
    nodes: Vec<NodeHealthEntry>,
    api_keys_count: usize,
}

pub async fn ws_stats(
    State(state): State<DashboardState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let authed = super::check_basic_auth(&headers, &state.dashboard_user, &state.dashboard_pass).is_ok();

    if !authed {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    ws.on_upgrade(move |socket| ws_stats_stream(socket, state))
}

async fn ws_stats_stream(mut socket: WebSocket, state: DashboardState) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

    loop {
        interval.tick().await;

        let global = state.stats.global_snapshot();
        let mut per_ip = HashMap::new();
        for entry in state.stats.per_ip.iter() {
            per_ip.insert(entry.key().to_string(), entry.value().snapshot());
        }

        let config = state.config.read().await;
        let ips: Vec<IpEntry> = config
            .whitelist
            .iter()
            .map(|w| IpEntry {
                ip: w.ip.to_string(),
                label: w.label.clone(),
                rps: w.rps.unwrap_or(config.rate_limits.default_rps),
                tps: w.tps.unwrap_or(config.rate_limits.default_tps),
                burst_rps: w.burst_rps.unwrap_or(config.rate_limits.default_burst_rps),
                burst_tps: w.burst_tps.unwrap_or(config.rate_limits.default_burst_tps),
            })
            .collect();
        let api_keys_count = config.api_keys.iter().filter(|k| !k.revoked).count();
        drop(config);

        let nodes: Vec<NodeHealthEntry> = state
            .cluster_nodes
            .iter()
            .map(|n| NodeHealthEntry {
                id: n.config.id.clone(),
                label: n.config.label.clone(),
                region: n.config.region.clone(),
                status: n.health.get_status().as_str().to_string(),
                latency_ms: n.health.latency_ms.load(Ordering::Relaxed),
                last_slot: n.health.last_slot.load(Ordering::Relaxed),
                uptime_pct: n.health.uptime_pct(),
                iris_healthy: n.health.iris_healthy.load(Ordering::Relaxed),
                iris_configured: n.config.iris_url.is_some(),
                wins: n.health.wins.load(Ordering::Relaxed),
                dupes: n.health.dupes.load(Ordering::Relaxed),
                last_msg_age_secs: last_msg_age_secs(&n.health),
            })
            .collect();

        let msg = WsStatsMessage {
            global,
            per_ip,
            ips,
            nodes,
            api_keys_count,
        };

        let json = match serde_json::to_string(&msg) {
            Ok(j) => j,
            Err(_) => break,
        };

        if socket.send(Message::Text(json.into())).await.is_err() {
            break;
        }
    }
}
