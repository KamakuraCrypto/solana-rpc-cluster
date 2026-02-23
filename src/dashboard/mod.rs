pub mod handlers;
pub mod static_assets;

use crate::cluster::NodeEntry;
use crate::config::Config;
use crate::middleware::api_keys::ApiKeyStore;
use crate::middleware::ip_whitelist::IpWhitelist;
use crate::middleware::rate_limiter::RateLimiterMiddleware;
use crate::stats::Stats;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, post, put};
use axum::Router;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct DashboardState {
    pub stats: Arc<Stats>,
    pub whitelist: IpWhitelist,
    pub rate_limiter: RateLimiterMiddleware,
    pub api_key_store: ApiKeyStore,
    pub config: Arc<RwLock<Config>>,
    pub config_path: PathBuf,
    pub cluster_nodes: Vec<Arc<NodeEntry>>,
    pub dashboard_user: String,
    pub dashboard_pass: String,
}

pub async fn run_dashboard(
    bind_addr: SocketAddr,
    state: DashboardState,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(static_assets::index_page))
        .route("/api/stats", get(handlers::get_stats))
        .route("/api/ips", get(handlers::list_ips))
        .route("/api/ips", post(handlers::add_ip))
        .route("/api/ips/:ip", put(handlers::update_ip))
        .route("/api/ips/:ip", delete(handlers::remove_ip))
        .route("/api/nodes", get(handlers::list_nodes))
        .route("/api/keys", get(handlers::list_api_keys))
        .route("/api/keys", post(handlers::create_api_key))
        .route("/api/keys/:prefix", delete(handlers::revoke_api_key))
        .route("/api/health", get(handlers::health_check))
        .route("/api/ws", get(handlers::ws_stats))
        .with_state(state);

    tracing::info!("Dashboard listening on {}", bind_addr);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};

pub fn check_basic_auth(
    headers: &HeaderMap,
    expected_user: &str,
    expected_pass: &str,
) -> Result<(), Response> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some(encoded) = auth.strip_prefix("Basic ") {
        if let Ok(decoded) = String::from_utf8(
            base64_decode(encoded).unwrap_or_default(),
        ) {
            if let Some((user, pass)) = decoded.split_once(':') {
                if user == expected_user && pass == expected_pass {
                    return Ok(());
                }
            }
        }
    }

    // Return 401 with WWW-Authenticate header to trigger browser login popup
    let mut resp = (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    resp.headers_mut().insert(
        "www-authenticate",
        HeaderValue::from_static("Basic realm=\"solana-rpc-cluster\""),
    );
    Err(resp)
}

fn base64_decode(input: &str) -> Option<Vec<u8>> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    for &b in input.as_bytes() {
        if b == b'=' { break; }
        let val = TABLE.iter().position(|&c| c == b)? as u32;
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
            buf &= (1 << bits) - 1;
        }
    }
    Some(out)
}
