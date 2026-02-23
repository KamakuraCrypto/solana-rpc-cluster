use crate::cluster::router::Router;
use crate::middleware::api_keys::ApiKeyStore;
use crate::middleware::ip_whitelist::IpWhitelist;
use crate::middleware::rate_limiter::RateLimiterMiddleware;
use crate::stats::Stats;
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

pub struct WebSocketProxy {
    router: Arc<Router>,
    whitelist: IpWhitelist,
    api_key_store: ApiKeyStore,
    rate_limiter: RateLimiterMiddleware,
    stats: Arc<Stats>,
}

impl WebSocketProxy {
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
        tracing::info!("WebSocket proxy listening on {}", bind_addr);

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            stream.set_nodelay(true).ok();

            let proxy = self.clone();
            tokio::spawn(async move {
                if let Err(e) = proxy.handle_connection(stream, peer_addr).await {
                    tracing::debug!("WebSocket connection error from {}: {}", peer_addr, e);
                }
            });
        }
    }

    async fn handle_connection(
        &self,
        stream: tokio::net::TcpStream,
        peer_addr: SocketAddr,
    ) -> anyhow::Result<()> {
        let client_ip = peer_addr.ip();

        // Auth: IP whitelist (API key auth for WS is harder at TCP level, skip for now)
        if !self.whitelist.is_allowed(&client_ip) {
            tracing::debug!("WS: rejected non-whitelisted IP: {}", client_ip);
            return Ok(());
        }

        // Rate limit
        if self.rate_limiter.check_rps(&client_ip).is_err() {
            self.stats.record_rate_limited(client_ip);
            return Ok(());
        }

        // Pick upstream node
        let node = self.router.pick_ws_node().ok_or_else(|| {
            anyhow::anyhow!("No healthy nodes for WebSocket")
        })?;

        let downstream = tokio_tungstenite::accept_async(stream).await?;
        tracing::debug!("WS: accepted from {} -> routing to {}", peer_addr, node.config.id);

        let ip_stats = self.stats.get_or_create(client_ip);
        ip_stats.active_ws_conns.fetch_add(1, Ordering::Relaxed);

        let (upstream, _) =
            tokio_tungstenite::connect_async(&node.config.ws_url).await?;

        let (ds_write, ds_read) = downstream.split();
        let (us_write, us_read) = upstream.split();

        let stats_clone = ip_stats.clone();

        let client_to_upstream = ds_read.forward(us_write);
        let upstream_to_client = us_read
            .inspect(move |msg| {
                if let Ok(msg) = msg {
                    if !matches!(msg, Message::Ping(_) | Message::Pong(_) | Message::Close(_)) {
                        stats_clone
                            .total_ws_messages
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
            .forward(ds_write);

        tokio::select! {
            r = client_to_upstream => {
                if let Err(e) = r {
                    tracing::debug!("WS client->upstream ended for {}: {}", peer_addr, e);
                }
            }
            r = upstream_to_client => {
                if let Err(e) = r {
                    tracing::debug!("WS upstream->client ended for {}: {}", peer_addr, e);
                }
            }
        }

        ip_stats.active_ws_conns.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }
}
