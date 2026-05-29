use crate::cluster::grpc_pool::GrpcPool;
use crate::cluster::router::Router;
use crate::middleware::api_keys::ApiKeyStore;
use crate::middleware::ip_whitelist::IpWhitelist;
use crate::middleware::rate_limiter::RateLimiterMiddleware;
use crate::proxy::grpc_filter::CompiledFilters;
use crate::proxy::grpc_mux::GrpcMultiplexer;
use crate::proxy::subscribe_supervisor::{SupervisedStream, SupervisorBuilder};
use crate::stats::Stats;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

pub mod geyser_proto {
    pub mod solana {
        pub mod storage {
            pub mod confirmed_block {
                tonic::include_proto!("solana.storage.confirmed_block");
            }
        }
    }
    tonic::include_proto!("geyser");
}

use geyser_proto::geyser_client::GeyserClient;
use geyser_proto::geyser_server::{Geyser, GeyserServer};
use geyser_proto::*;

/// Max concurrent gRPC streams per IP (replaces RPS rate limiting for gRPC)
const MAX_STREAMS_PER_IP: u64 = 500;

pub struct YellowstoneProxy {
    router: Arc<Router>,
    grpc_pool: Arc<GrpcPool>,
    mux: Arc<GrpcMultiplexer>,
    whitelist: IpWhitelist,
    api_key_store: ApiKeyStore,
    rate_limiter: RateLimiterMiddleware,
    stats: Arc<Stats>,
}

impl YellowstoneProxy {
    pub fn new(
        router: Arc<Router>,
        grpc_pool: Arc<GrpcPool>,
        mux: Arc<GrpcMultiplexer>,
        whitelist: IpWhitelist,
        api_key_store: ApiKeyStore,
        rate_limiter: RateLimiterMiddleware,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            router,
            grpc_pool,
            mux,
            whitelist,
            api_key_store,
            rate_limiter,
            stats,
        }
    }

    pub fn into_server(self) -> GeyserServer<Self> {
        GeyserServer::new(self)
    }

    async fn get_upstream(&self) -> Result<(GeyserClient<Channel>, String, String), Status> {
        let node = self
            .router
            .pick_grpc_node()
            .ok_or_else(|| Status::unavailable("No healthy gRPC nodes"))?;
        let node_id = node.config.id.clone();
        let x_token = node.config.grpc_x_token.clone();

        let channel = if let Some(managed) = self.grpc_pool.get_channel(&node_id) {
            managed.get().await?
        } else {
            let endpoint = Channel::from_shared(node.config.grpc_url.clone())
                .map_err(|e| Status::internal(format!("Invalid URL: {}", e)))?
                .tcp_nodelay(true)
                .http2_keep_alive_interval(std::time::Duration::from_secs(10))
                .keep_alive_timeout(std::time::Duration::from_secs(20))
                .connect_timeout(std::time::Duration::from_secs(5));
            endpoint
                .connect()
                .await
                .map_err(|e| Status::unavailable(format!("Connect failed: {}", e)))?
        };

        let client = GeyserClient::new(channel);
        Ok((client, x_token, node_id))
    }

    fn check_access(&self, req: &Request<()>) -> Result<std::net::IpAddr, Status> {
        let ip = extract_client_ip(req)?;

        if let Some(key) = req
            .metadata()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
        {
            if self.api_key_store.validate(key).is_some() {
                return Ok(ip);
            }
        }

        if !self.whitelist.is_allowed(&ip) {
            return Err(Status::permission_denied("Unauthorized"));
        }

        Ok(ip)
    }

    fn inject_token<T>(&self, mut req: Request<T>, x_token: &str) -> Request<T> {
        if !x_token.is_empty() {
            if let Ok(val) = x_token.parse() {
                req.metadata_mut().insert("x-token", val);
            }
        }
        req
    }

    /// Direct upstream subscribe (1:1) — fallback for subscriptions the mux can't serve.
    /// `first_msg` is the already-consumed first SubscribeRequest.
    async fn subscribe_direct(
        &self,
        first_msg: SubscribeRequest,
        mut inbound: Streaming<SubscribeRequest>,
        ip: std::net::IpAddr,
        ip_stats: Arc<crate::stats::IpStats>,
    ) -> Result<Response<SupervisedStream<Result<SubscribeUpdate, Status>>>, Status> {
        let (mut upstream, x_token, node_id) = self.get_upstream().await?;
        let grpc_pool = self.grpc_pool.clone();

        let (upstream_tx, upstream_rx) = mpsc::channel::<SubscribeRequest>(256);

        // Re-inject the consumed first message.
        upstream_tx
            .send(first_msg)
            .await
            .map_err(|_| Status::internal("Channel send failed"))?;

        let mut upstream_req = Request::new(ReceiverStream::new(upstream_rx));
        if !x_token.is_empty() {
            if let Ok(val) = x_token.parse() {
                upstream_req.metadata_mut().insert("x-token", val);
            }
        }

        let upstream_resp = upstream.subscribe(upstream_req).await?;
        let mut upstream_stream = upstream_resp.into_inner();

        let (downstream_tx, downstream_rx) =
            mpsc::channel::<Result<SubscribeUpdate, Status>>(2048);

        let mut sb = SupervisorBuilder::new(
            ip_stats.active_grpc_streams.clone(),
            self.stats.supervisor_counters(),
        );

        // Client → upstream forwarder. Bidi gRPC allows the client to half-close the
        // request side and keep receiving, so request EOF must NOT cancel the response
        // side — this task just exits silently and the response forwarder lives on.
        let token = sb.token();
        sb.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    _ = upstream_tx.closed() => break,
                    item = inbound.next() => match item {
                        Some(Ok(msg)) => {
                            let permit = tokio::select! {
                                biased;
                                _ = token.cancelled() => break,
                                _ = upstream_tx.closed() => break,
                                p = upstream_tx.reserve() => match p {
                                    Ok(p) => p,
                                    Err(_) => break,
                                },
                            };
                            permit.send(msg);
                        }
                        Some(Err(e)) => {
                            tracing::debug!("gRPC direct: downstream read error: {}", e);
                            break;
                        }
                        None => break,
                    },
                }
            }
        });

        // Forward upstream → client. Race the upstream Stream against the downstream
        // Receiver being dropped — this is the path that previously zombied when the
        // client disappeared during a long idle window.
        let token = sb.token();
        let pool_for_err = grpc_pool.clone();
        let node_for_err = node_id.clone();
        sb.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    _ = downstream_tx.closed() => break,
                    item = upstream_stream.next() => match item {
                        Some(Ok(msg)) => {
                            let permit = tokio::select! {
                                biased;
                                _ = token.cancelled() => break,
                                _ = downstream_tx.closed() => break,
                                p = downstream_tx.reserve() => match p {
                                    Ok(p) => p,
                                    Err(_) => break,
                                },
                            };
                            permit.send(Ok(msg));
                        }
                        Some(Err(e)) => {
                            tracing::debug!(
                                "gRPC direct: upstream error, invalidating channel: {}",
                                e
                            );
                            pool_for_err.invalidate(&node_for_err);
                            let _ = downstream_tx.send(Err(e)).await;
                            break;
                        }
                        None => break,
                    }
                }
            }
        });

        let handle = sb.start();
        tracing::debug!("gRPC direct: {} routed to direct upstream", ip);
        Ok(Response::new(SupervisedStream::new(downstream_rx, handle)))
    }
}

fn extract_client_ip<T>(req: &Request<T>) -> Result<std::net::IpAddr, Status> {
    req.remote_addr()
        .map(|addr| addr.ip())
        .ok_or_else(|| Status::internal("Cannot determine client IP"))
}

#[tonic::async_trait]
impl Geyser for YellowstoneProxy {
    type SubscribeStream = SupervisedStream<Result<SubscribeUpdate, Status>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let ip = extract_client_ip(&request)?;

        // Auth check: API key or IP whitelist
        let has_key = request
            .metadata()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .and_then(|k| self.api_key_store.validate(k))
            .is_some();

        if !has_key && !self.whitelist.is_allowed(&ip) {
            return Err(Status::permission_denied("Unauthorized"));
        }

        let ip_stats = self.stats.get_or_create(ip);

        // Concurrent-stream limit per IP. The supervisor owns the active counter
        // (incremented in `start()`, decremented exactly once on drop), so we read
        // here but do NOT touch it.
        let current_streams = ip_stats.active_grpc_streams.load(Ordering::Relaxed);
        if current_streams >= MAX_STREAMS_PER_IP {
            return Err(Status::resource_exhausted(format!(
                "Too many concurrent gRPC streams ({}), max {}",
                current_streams, MAX_STREAMS_PER_IP
            )));
        }

        ip_stats
            .total_grpc_requests
            .fetch_add(1, Ordering::Relaxed);

        let mut inbound = request.into_inner();

        // Read the first SubscribeRequest to determine routing
        let first_msg = match inbound.next().await {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => return Err(Status::internal(format!("Stream error: {}", e))),
            None => return Err(Status::invalid_argument("Empty subscribe stream")),
        };

        let filters = CompiledFilters::from_request(&first_msg);

        // Route: if subscription needs types the mux doesn't serve, use direct upstream
        if filters.needs_direct_connection() || !filters.has_mux_subscriptions() {
            tracing::debug!(
                "gRPC subscribe from {}: routing to direct upstream (accounts={}, blocks={}, tx_status={})",
                ip,
                filters.has_accounts,
                filters.has_blocks,
                filters.has_transactions_status,
            );
            return self
                .subscribe_direct(first_msg, inbound, ip, ip_stats)
                .await;
        }

        // Mux path: subscribe to broadcast with client's filters
        tracing::debug!(
            "gRPC subscribe from {}: routing to mux ({} tx filters, {} slot filters)",
            ip,
            filters.transactions.len(),
            filters.slots.len(),
        );

        // Channel for injecting pong responses back to the client
        let (pong_tx, pong_rx) = mpsc::channel::<SubscribeUpdate>(16);

        let mut sb = SupervisorBuilder::new(
            ip_stats.active_grpc_streams.clone(),
            self.stats.supervisor_counters(),
        );
        let token = sb.token();

        let filtered_rx = self
            .mux
            .subscribe_filtered(filters, pong_rx, token.clone());

        // Handle subsequent client messages (pings → respond with pong).
        // Cancel-safe: races request reads against cancellation and pong_tx.closed().
        sb.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    _ = pong_tx.closed() => break,
                    item = inbound.next() => match item {
                        Some(Ok(msg)) => {
                            if let Some(ping) = msg.ping {
                                let pong = SubscribeUpdate {
                                    filters: vec![],
                                    created_at: None,
                                    update_oneof: Some(
                                        subscribe_update::UpdateOneof::Pong(SubscribeUpdatePong {
                                            id: ping.id,
                                        }),
                                    ),
                                };
                                let permit = tokio::select! {
                                    biased;
                                    _ = token.cancelled() => break,
                                    _ = pong_tx.closed() => break,
                                    p = pong_tx.reserve() => match p {
                                        Ok(p) => p,
                                        Err(_) => break,
                                    },
                                };
                                permit.send(pong);
                            }
                            // Filter updates in subsequent messages are ignored for now.
                        }
                        Some(Err(e)) => {
                            tracing::debug!("gRPC mux: client stream error: {}", e);
                            break;
                        }
                        None => break, // half-close on request side, response stays alive
                    },
                }
            }
        });

        let handle = sb.start();
        Ok(Response::new(SupervisedStream::new(filtered_rx, handle)))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        let (mut upstream, _, _) = self.get_upstream().await?;
        upstream.ping(request).await
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        let (mut upstream, _, _) = self.get_upstream().await?;
        upstream.get_latest_blockhash(request).await
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        let (mut upstream, _, _) = self.get_upstream().await?;
        upstream.get_block_height(request).await
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        let (mut upstream, _, _) = self.get_upstream().await?;
        upstream.get_slot(request).await
    }

    async fn is_blockhash_valid(
        &self,
        request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        let (mut upstream, _, _) = self.get_upstream().await?;
        upstream.is_blockhash_valid(request).await
    }

    async fn get_version(
        &self,
        request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        let (mut upstream, _, _) = self.get_upstream().await?;
        upstream.get_version(request).await
    }
}

use futures_util::StreamExt;

pub async fn run_grpc_proxy(
    bind_addr: SocketAddr,
    proxy: YellowstoneProxy,
    grpc_config: &crate::config::GrpcConfig,
) -> anyhow::Result<()> {
    tracing::info!("Yellowstone gRPC proxy listening on {}", bind_addr);

    // Build the listener ourselves so we can set TCP_USER_TIMEOUT and a configurable
    // TCP keepalive on the listening socket. Both are inherited by accepted sockets.
    let incoming = crate::proxy::tcp_listener::build_incoming(
        bind_addr,
        grpc_config.tcp_keepalive_secs,
        grpc_config.tcp_user_timeout_secs,
    )?;

    tonic::transport::Server::builder()
        .initial_connection_window_size(grpc_config.connection_window_bytes)
        .initial_stream_window_size(grpc_config.stream_window_bytes)
        .http2_adaptive_window(Some(grpc_config.adaptive_window))
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(grpc_config.keepalive_interval_secs)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(grpc_config.keepalive_timeout_secs)))
        .concurrency_limit_per_connection(grpc_config.concurrency_limit)
        .max_frame_size(Some(32 * 1024)) // 32KB frames
        .add_service(proxy.into_server())
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}
