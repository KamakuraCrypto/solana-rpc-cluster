use crate::cluster::router::Router;
use crate::middleware::api_keys::ApiKeyStore;
use crate::middleware::ip_whitelist::IpWhitelist;
use crate::middleware::rate_limiter::RateLimiterMiddleware;
use crate::stats::Stats;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

const MAX_BODY_SIZE: usize = 1024 * 1024; // 1MB

pub struct HttpRpcProxy {
    router: Arc<Router>,
    whitelist: IpWhitelist,
    api_key_store: ApiKeyStore,
    rate_limiter: RateLimiterMiddleware,
    stats: Arc<Stats>,
}

impl HttpRpcProxy {
    pub fn new(
        router: Arc<Router>,
        whitelist: IpWhitelist,
        api_key_store: ApiKeyStore,
        rate_limiter: RateLimiterMiddleware,
        stats: Arc<Stats>,
    ) -> Arc<Self> {
        Arc::new(Self {
            router,
            whitelist,
            api_key_store,
            rate_limiter,
            stats,
        })
    }

    pub async fn run(self: Arc<Self>, bind_addr: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(bind_addr).await?;
        tracing::info!("HTTP JSON-RPC proxy listening on {}", bind_addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            stream.set_nodelay(true).ok();

            let proxy = self.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let proxy = proxy.clone();
                    async move { proxy.handle_request(peer_addr, req).await }
                });

                if let Err(e) = http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await
                {
                    if !e.is_incomplete_message() {
                        tracing::debug!("HTTP connection error from {}: {}", peer_addr, e);
                    }
                }
            });
        }
    }

    async fn handle_request(
        &self,
        peer_addr: SocketAddr,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let client_ip = peer_addr.ip();

        // Auth: IP whitelist OR API key
        let api_key = req
            .headers()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let authed_by_key = api_key
            .as_ref()
            .and_then(|k| self.api_key_store.validate(k));

        if !self.whitelist.is_allowed(&client_ip) && authed_by_key.is_none() {
            return Ok(json_error_response(
                StatusCode::FORBIDDEN,
                "Unauthorized",
            ));
        }

        // Read body
        let body_bytes = match read_body(req.into_body()).await {
            Ok(b) => b,
            Err(_) => {
                return Ok(json_error_response(
                    StatusCode::BAD_REQUEST,
                    "Request body too large",
                ));
            }
        };

        // Classify request
        let method = classify_rpc(&body_bytes);
        let is_send_tx = matches!(method, RpcMethod::SendTransaction | RpcMethod::SendTransactionBatch);

        // Rate limit check (by key or by IP)
        if let Some(ref key) = api_key {
            if authed_by_key.is_some() {
                if let Err(is_tps) = self.rate_limiter.check_by_key(key, is_send_tx) {
                    let msg = if is_tps { "TPS rate limit exceeded" } else { "RPS rate limit exceeded" };
                    return Ok(json_error_response(StatusCode::TOO_MANY_REQUESTS, msg));
                }
            }
        } else if let Err(is_tps) = self.rate_limiter.check(&client_ip, is_send_tx) {
            self.stats.record_rate_limited(client_ip);
            let msg = if is_tps { "TPS rate limit exceeded" } else { "RPS rate limit exceeded" };
            return Ok(json_error_response(StatusCode::TOO_MANY_REQUESTS, msg));
        }

        // Record stats
        self.stats.record_rpc(client_ip, is_send_tx);
        let ip_stats = self.stats.get_or_create(client_ip);
        ip_stats.active_rpc_conns.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Route based on method type
        let result = match method {
            RpcMethod::SendTransaction | RpcMethod::SendTransactionBatch => {
                match self.router.fan_out_send_tx(body_bytes).await {
                    Ok(resp) => Ok(resp),
                    Err(e) => {
                        self.stats.record_error(client_ip);
                        tracing::warn!("sendTx fan-out error: {}", e);
                        Ok(json_error_response(StatusCode::BAD_GATEWAY, &e))
                    }
                }
            }
            RpcMethod::HeavyReadRpc => {
                // Distribute heavy calls (GPA etc.) across all healthy nodes via round-robin
                match self.router.forward_heavy_rpc(body_bytes).await {
                    Ok(resp) => Ok(resp),
                    Err(e) => {
                        self.stats.record_error(client_ip);
                        tracing::warn!("Heavy RPC error: {}", e);
                        Ok(json_error_response(StatusCode::BAD_GATEWAY, &e))
                    }
                }
            }
            RpcMethod::ReadRpc => {
                match self.router.forward_read_rpc(body_bytes).await {
                    Ok(resp) => Ok(resp),
                    Err(e) => {
                        self.stats.record_error(client_ip);
                        tracing::warn!("Read RPC error: {}", e);
                        Ok(json_error_response(StatusCode::BAD_GATEWAY, &e))
                    }
                }
            }
        };

        ip_stats.active_rpc_conns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        result
    }
}

enum RpcMethod {
    SendTransaction,
    SendTransactionBatch,
    HeavyReadRpc,
    ReadRpc,
}

fn classify_rpc(body: &[u8]) -> RpcMethod {
    if memchr_find(body, b"sendTransactionBatch") {
        RpcMethod::SendTransactionBatch
    } else if memchr_find(body, b"sendTransaction") {
        RpcMethod::SendTransaction
    } else if memchr_find(body, b"getProgramAccounts")
        || memchr_find(body, b"getTokenAccountsByOwner")
        || memchr_find(body, b"getTokenAccountsByDelegate")
        || memchr_find(body, b"getTokenLargestAccounts")
        || memchr_find(body, b"getLargestAccounts")
        || memchr_find(body, b"getSupply")
    {
        RpcMethod::HeavyReadRpc
    } else {
        RpcMethod::ReadRpc
    }
}

async fn read_body(body: Incoming) -> Result<Bytes, ()> {
    let collected = body.collect().await.map_err(|_| ())?;
    let bytes = collected.to_bytes();
    if bytes.len() > MAX_BODY_SIZE {
        return Err(());
    }
    Ok(bytes)
}

fn memchr_find(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn json_error_response(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "error": {
            "code": -(status.as_u16() as i64),
            "message": message
        },
        "id": null
    });

    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("access-control-allow-origin", "*")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}
