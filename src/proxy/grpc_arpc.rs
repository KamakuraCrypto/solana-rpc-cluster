use crate::cluster::grpc_pool::GrpcPool;
use crate::cluster::router::Router;
use crate::middleware::api_keys::ApiKeyStore;
use crate::middleware::ip_whitelist::IpWhitelist;
use crate::middleware::rate_limiter::RateLimiterMiddleware;
use crate::proxy::subscribe_supervisor::{SupervisedStream, SupervisorBuilder};
use crate::stats::Stats;
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

pub mod arpc_v1_proto {
    tonic::include_proto!("arpc.legacy");
}

pub mod arpc_v2_proto {
    tonic::include_proto!("arpc.v2");
}

use arpc_v1_proto::arpc_service_client::ArpcServiceClient;
use arpc_v1_proto::arpc_service_server::{ArpcService, ArpcServiceServer};
use arpc_v2_proto::arpc_service_v2_client::ArpcServiceV2Client;
use arpc_v2_proto::arpc_service_v2_server::{ArpcServiceV2, ArpcServiceV2Server};

// ═══════════════════════════════════════════════════════════
// aRPC v1 Proxy
// ═══════════════════════════════════════════════════════════

pub struct ArpcV1Proxy {
    router: Arc<Router>,
    grpc_pool: Arc<GrpcPool>,
    whitelist: IpWhitelist,
    api_key_store: ApiKeyStore,
    rate_limiter: RateLimiterMiddleware,
    stats: Arc<Stats>,
}

impl ArpcV1Proxy {
    pub fn new(
        router: Arc<Router>,
        grpc_pool: Arc<GrpcPool>,
        whitelist: IpWhitelist,
        api_key_store: ApiKeyStore,
        rate_limiter: RateLimiterMiddleware,
        stats: Arc<Stats>,
    ) -> Self {
        Self { router, grpc_pool, whitelist, api_key_store, rate_limiter, stats }
    }

    async fn get_upstream(&self) -> Result<(ArpcServiceClient<Channel>, String), Status> {
        let node = self.router.pick_grpc_node().ok_or_else(|| {
            Status::unavailable("No healthy aRPC nodes")
        })?;
        let node_id = node.config.id.clone();

        // aRPC uses arpc_url, not grpc_url. For the pool, we use the grpc channel
        // but aRPC might be on a different port. Create ad-hoc for aRPC.
        let endpoint = Channel::from_shared(node.config.arpc_url.clone())
            .map_err(|e| Status::internal(format!("Invalid URL: {}", e)))?
            .tcp_nodelay(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(10))
            .keep_alive_timeout(std::time::Duration::from_secs(20))
            .connect_timeout(std::time::Duration::from_secs(5));

        let channel = endpoint.connect().await
            .map_err(|e| Status::unavailable(format!("aRPC connect failed: {}", e)))?;

        Ok((ArpcServiceClient::new(channel), node_id))
    }
}

fn extract_client_ip<T>(req: &Request<T>) -> Result<std::net::IpAddr, Status> {
    req.remote_addr()
        .map(|addr| addr.ip())
        .ok_or_else(|| Status::internal("Cannot determine client IP"))
}

fn check_access(
    req_ip: std::net::IpAddr,
    whitelist: &IpWhitelist,
    api_key_store: &ApiKeyStore,
    rate_limiter: &RateLimiterMiddleware,
    stats: &Stats,
    metadata: &tonic::metadata::MetadataMap,
) -> Result<(), Status> {
    // Check API key
    if let Some(key) = metadata.get("x-api-key").and_then(|v| v.to_str().ok()) {
        if api_key_store.validate(key).is_some() {
            return Ok(());
        }
    }

    if !whitelist.is_allowed(&req_ip) {
        return Err(Status::permission_denied("Unauthorized"));
    }

    if rate_limiter.check_rps(&req_ip).is_err() {
        stats.record_rate_limited(req_ip);
        return Err(Status::resource_exhausted("Rate limit exceeded"));
    }

    Ok(())
}

#[tonic::async_trait]
impl ArpcService for ArpcV1Proxy {
    type SubscribeStream = SupervisedStream<Result<arpc_v1_proto::SubscribeResponse, Status>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<arpc_v1_proto::SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let ip = extract_client_ip(&request)?;
        check_access(ip, &self.whitelist, &self.api_key_store, &self.rate_limiter, &self.stats, request.metadata())?;

        let ip_stats = self.stats.get_or_create(ip);
        ip_stats.total_arpc_requests.fetch_add(1, Ordering::Relaxed);

        let (mut upstream, _node_id) = self.get_upstream().await?;
        let mut inbound = request.into_inner();

        let (upstream_tx, upstream_rx) = mpsc::channel::<arpc_v1_proto::SubscribeRequest>(256);

        let upstream_resp = upstream
            .subscribe(Request::new(ReceiverStream::new(upstream_rx)))
            .await?;
        let mut upstream_stream = upstream_resp.into_inner();

        let (downstream_tx, downstream_rx) =
            mpsc::channel::<Result<arpc_v1_proto::SubscribeResponse, Status>>(2048);

        let mut sb = SupervisorBuilder::new(
            ip_stats.active_arpc_streams.clone(),
            self.stats.supervisor_counters(),
        );

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
                            tracing::debug!("aRPC v1 downstream read error: {}", e);
                            break;
                        }
                        None => break,
                    },
                }
            }
        });

        let token = sb.token();
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
                            let _ = downstream_tx.send(Err(e)).await;
                            break;
                        }
                        None => break,
                    },
                }
            }
        });

        let handle = sb.start();
        Ok(Response::new(SupervisedStream::new(downstream_rx, handle)))
    }
}

// ═══════════════════════════════════════════════════════════
// aRPC v2 Proxy
// ═══════════════════════════════════════════════════════════

pub struct ArpcV2Proxy {
    router: Arc<Router>,
    grpc_pool: Arc<GrpcPool>,
    whitelist: IpWhitelist,
    api_key_store: ApiKeyStore,
    rate_limiter: RateLimiterMiddleware,
    stats: Arc<Stats>,
}

impl ArpcV2Proxy {
    pub fn new(
        router: Arc<Router>,
        grpc_pool: Arc<GrpcPool>,
        whitelist: IpWhitelist,
        api_key_store: ApiKeyStore,
        rate_limiter: RateLimiterMiddleware,
        stats: Arc<Stats>,
    ) -> Self {
        Self { router, grpc_pool, whitelist, api_key_store, rate_limiter, stats }
    }

    async fn get_upstream(&self) -> Result<(ArpcServiceV2Client<Channel>, String), Status> {
        let node = self.router.pick_grpc_node().ok_or_else(|| {
            Status::unavailable("No healthy aRPC v2 nodes")
        })?;
        let node_id = node.config.id.clone();

        let endpoint = Channel::from_shared(node.config.arpc_url.clone())
            .map_err(|e| Status::internal(format!("Invalid URL: {}", e)))?
            .tcp_nodelay(true)
            .http2_keep_alive_interval(std::time::Duration::from_secs(10))
            .keep_alive_timeout(std::time::Duration::from_secs(20))
            .connect_timeout(std::time::Duration::from_secs(5));

        let channel = endpoint.connect().await
            .map_err(|e| Status::unavailable(format!("aRPC v2 connect failed: {}", e)))?;

        Ok((ArpcServiceV2Client::new(channel), node_id))
    }
}

/// Build a supervised one-way forwarder: upstream Stream → downstream mpsc, cancel-safe.
/// Used by aRPC v2's server-streaming methods (subscribe_entries, subscribe_slots).
fn supervise_server_streaming<S, T>(
    stats: &Stats,
    ip_stats: Arc<crate::stats::IpStats>,
    mut upstream_stream: S,
) -> SupervisedStream<Result<T, Status>>
where
    S: tokio_stream::Stream<Item = Result<T, Status>> + Unpin + Send + 'static,
    T: Send + 'static,
{
    let (downstream_tx, downstream_rx) = mpsc::channel::<Result<T, Status>>(2048);
    let mut sb = SupervisorBuilder::new(
        ip_stats.active_arpc_streams.clone(),
        stats.supervisor_counters(),
    );
    let token = sb.token();
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
                        let _ = downstream_tx.send(Err(e)).await;
                        break;
                    }
                    None => break,
                },
            }
        }
    });
    let handle = sb.start();
    SupervisedStream::new(downstream_rx, handle)
}

#[tonic::async_trait]
impl ArpcServiceV2 for ArpcV2Proxy {
    type SubscribeEntriesStream = SupervisedStream<Result<arpc_v2_proto::SubscribeEntriesResponse, Status>>;
    type SubscribeTransactionsStream = SupervisedStream<Result<arpc_v2_proto::SubscribeTransactionsResponse, Status>>;
    type SubscribeSlotsStream = SupervisedStream<Result<arpc_v2_proto::SubscribeSlotsResponse, Status>>;

    async fn subscribe_entries(
        &self,
        request: Request<arpc_v2_proto::SubscribeEntriesRequest>,
    ) -> Result<Response<Self::SubscribeEntriesStream>, Status> {
        let ip = extract_client_ip(&request)?;
        check_access(ip, &self.whitelist, &self.api_key_store, &self.rate_limiter, &self.stats, request.metadata())?;

        let ip_stats = self.stats.get_or_create(ip);

        let (mut upstream, _) = self.get_upstream().await?;
        let upstream_resp = upstream.subscribe_entries(request).await?;
        let upstream_stream = upstream_resp.into_inner();

        Ok(Response::new(supervise_server_streaming(
            &self.stats,
            ip_stats,
            upstream_stream,
        )))
    }

    async fn subscribe_transactions(
        &self,
        request: Request<Streaming<arpc_v2_proto::SubscribeTransactionsRequest>>,
    ) -> Result<Response<Self::SubscribeTransactionsStream>, Status> {
        let ip = extract_client_ip(&request)?;
        check_access(ip, &self.whitelist, &self.api_key_store, &self.rate_limiter, &self.stats, request.metadata())?;

        let ip_stats = self.stats.get_or_create(ip);

        let (mut upstream, _) = self.get_upstream().await?;
        let mut inbound = request.into_inner();

        let (upstream_tx, upstream_rx) =
            mpsc::channel::<arpc_v2_proto::SubscribeTransactionsRequest>(256);

        let upstream_resp = upstream
            .subscribe_transactions(Request::new(ReceiverStream::new(upstream_rx)))
            .await?;
        let mut upstream_stream = upstream_resp.into_inner();

        let (downstream_tx, downstream_rx) = mpsc::channel::<
            Result<arpc_v2_proto::SubscribeTransactionsResponse, Status>,
        >(2048);

        let mut sb = SupervisorBuilder::new(
            ip_stats.active_arpc_streams.clone(),
            self.stats.supervisor_counters(),
        );

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
                            tracing::debug!("aRPC v2 tx downstream error: {}", e);
                            break;
                        }
                        None => break,
                    },
                }
            }
        });

        let token = sb.token();
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
                            let _ = downstream_tx.send(Err(e)).await;
                            break;
                        }
                        None => break,
                    },
                }
            }
        });

        let handle = sb.start();
        Ok(Response::new(SupervisedStream::new(downstream_rx, handle)))
    }

    async fn subscribe_slots(
        &self,
        request: Request<arpc_v2_proto::SubscribeSlotsRequest>,
    ) -> Result<Response<Self::SubscribeSlotsStream>, Status> {
        let ip = extract_client_ip(&request)?;
        check_access(ip, &self.whitelist, &self.api_key_store, &self.rate_limiter, &self.stats, request.metadata())?;

        let ip_stats = self.stats.get_or_create(ip);

        let (mut upstream, _) = self.get_upstream().await?;
        let upstream_resp = upstream.subscribe_slots(request).await?;
        let upstream_stream = upstream_resp.into_inner();

        Ok(Response::new(supervise_server_streaming(
            &self.stats,
            ip_stats,
            upstream_stream,
        )))
    }
}

pub async fn run_arpc_proxy(
    bind_addr: SocketAddr,
    v1_proxy: ArpcV1Proxy,
    v2_proxy: ArpcV2Proxy,
    grpc_config: &crate::config::GrpcConfig,
) -> anyhow::Result<()> {
    tracing::info!("aRPC proxy (v1 + v2) listening on {}", bind_addr);

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
        .add_service(ArpcServiceServer::new(v1_proxy))
        .add_service(ArpcServiceV2Server::new(v2_proxy))
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}
