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

/// Monotonic stream counter used to label streams in logs.
static STREAM_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub struct YellowstoneProxy {
    router: Arc<Router>,
    grpc_pool: Arc<GrpcPool>,
    mux: Arc<GrpcMultiplexer>,
    whitelist: IpWhitelist,
    api_key_store: ApiKeyStore,
    rate_limiter: RateLimiterMiddleware,
    stats: Arc<Stats>,
    replay_on_reconnect: bool,
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
        replay_on_reconnect: bool,
    ) -> Self {
        Self {
            router,
            grpc_pool,
            mux,
            whitelist,
            api_key_store,
            rate_limiter,
            stats,
            replay_on_reconnect,
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

    /// Direct upstream subscribe — for subscriptions the mux can't serve (accounts,
    /// blocks, transaction status, unsupported filter options).
    ///
    /// The client's stream is NEVER terminated because of the upstream: an upstream
    /// stream error/EOF triggers a transparent reconnect (to any available node), the
    /// client's latest SubscribeRequest is re-sent, and when possible the subscription
    /// resumes from the last seen slot so nothing is missed. The client stream only ends
    /// when the client goes away.
    async fn subscribe_direct(
        &self,
        first_msg: SubscribeRequest,
        mut inbound: Streaming<SubscribeRequest>,
        ip: std::net::IpAddr,
        ip_stats: Arc<crate::stats::IpStats>,
        label: String,
    ) -> Result<Response<SupervisedStream<Result<SubscribeUpdate, Status>>>, Status> {
        let router = self.router.clone();
        let grpc_pool = self.grpc_pool.clone();
        let replay = self.replay_on_reconnect;

        // Client → manager: subsequent SubscribeRequests (filter updates, pings).
        let (client_tx, mut client_rx) = mpsc::channel::<SubscribeRequest>(256);
        let (downstream_tx, downstream_rx) =
            mpsc::channel::<Result<SubscribeUpdate, Status>>(2048);

        let mut sb = SupervisorBuilder::new(
            ip_stats.active_grpc_streams.clone(),
            self.stats.supervisor_counters(),
        );

        // Client → manager forwarder. Bidi gRPC allows the client to half-close the
        // request side and keep receiving, so request EOF must NOT cancel the response
        // side — this task just exits silently and the manager lives on.
        let token = sb.token();
        let fwd_label = label.clone();
        sb.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    _ = client_tx.closed() => break,
                    item = inbound.next() => match item {
                        Some(Ok(msg)) => {
                            let permit = tokio::select! {
                                biased;
                                _ = token.cancelled() => break,
                                _ = client_tx.closed() => break,
                                p = client_tx.reserve() => match p {
                                    Ok(p) => p,
                                    Err(_) => break,
                                },
                            };
                            permit.send(msg);
                        }
                        Some(Err(e)) => {
                            tracing::info!("gRPC direct[{}]: client request stream error: {}", fwd_label, e);
                            break;
                        }
                        None => {
                            tracing::debug!("gRPC direct[{}]: client half-closed request side", fwd_label);
                            break;
                        }
                    },
                }
            }
        });

        // Upstream manager: owns the upstream stream, reconnects forever.
        let token = sb.token();
        let mgr_label = label.clone();
        sb.spawn(async move {
            let started = std::time::Instant::now();
            let mut latest_req = first_msg;
            let mut last_slot: u64 = 0;
            let mut total_forwarded: u64 = 0;
            let mut attempts: u64 = 0;
            let mut backoff = std::time::Duration::from_secs(1);
            // from_slot to request on the next connect (None = live)
            let mut replay_from: Option<u64> = None;
            let mut client_open = true;
            let close_reason: &str;

            'reconnect: loop {
                attempts += 1;
                let node = match router.pick_grpc_node().or_else(|| {
                    router.get_grpc_eligible_nodes().into_iter().min_by_key(|n| n.config.priority)
                }) {
                    Some(n) => n,
                    None => {
                        tracing::error!("gRPC direct[{}]: no gRPC-eligible nodes configured", mgr_label);
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => { close_reason = "cancelled"; break 'reconnect },
                            _ = downstream_tx.closed() => { close_reason = "client gone"; break 'reconnect },
                            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => continue 'reconnect,
                        }
                    }
                };
                let node_id = node.config.id.clone();

                // Build the upstream request stream with the latest client filters.
                let (upstream_tx, upstream_rx) = mpsc::channel::<SubscribeRequest>(256);
                let mut req = latest_req.clone();
                if let Some(fs) = replay_from {
                    req.from_slot = Some(fs);
                }
                let _ = upstream_tx.send(req).await;
                let mut upstream_req = Request::new(ReceiverStream::new(upstream_rx));
                if !node.config.grpc_x_token.is_empty() {
                    if let Ok(val) = node.config.grpc_x_token.parse() {
                        upstream_req.metadata_mut().insert("x-token", val);
                    }
                }

                let connect = async {
                    let channel = if let Some(managed) = grpc_pool.get_channel(&node_id) {
                        managed.get().await?
                    } else {
                        Channel::from_shared(node.config.grpc_url.clone())
                            .map_err(|e| Status::internal(format!("Invalid URL: {}", e)))?
                            .tcp_nodelay(true)
                            .http2_keep_alive_interval(std::time::Duration::from_secs(10))
                            .keep_alive_timeout(std::time::Duration::from_secs(20))
                            .connect_timeout(std::time::Duration::from_secs(5))
                            .connect()
                            .await
                            .map_err(|e| Status::unavailable(format!("Connect failed: {}", e)))?
                    };
                    let mut client = GeyserClient::new(channel);
                    client.subscribe(upstream_req).await.map(|r| r.into_inner())
                };

                let mut upstream_stream = tokio::select! {
                    biased;
                    _ = token.cancelled() => { close_reason = "cancelled"; break 'reconnect },
                    _ = downstream_tx.closed() => { close_reason = "client gone"; break 'reconnect },
                    r = connect => match r {
                        Ok(s) => s,
                        Err(e) => {
                            grpc_pool.invalidate(&node_id);
                            tracing::warn!(
                                "gRPC direct[{}]: upstream {} subscribe failed (attempt {}): {}",
                                mgr_label, node_id, attempts, e
                            );
                            // Only a rejected from_slot (not a plain connect failure)
                            // means we must go live next time.
                            if replay_from.is_some() && is_replay_rejection(&e) {
                                tracing::warn!("gRPC direct[{}]: replay from {:?} rejected by upstream, resuming live", mgr_label, replay_from);
                                replay_from = None;
                            }
                            tokio::select! {
                                biased;
                                _ = token.cancelled() => { close_reason = "cancelled"; break 'reconnect },
                                _ = downstream_tx.closed() => { close_reason = "client gone"; break 'reconnect },
                                _ = tokio::time::sleep(backoff) => {},
                            }
                            backoff = (backoff * 2).min(std::time::Duration::from_secs(10));
                            continue 'reconnect;
                        }
                    },
                };

                let established = std::time::Instant::now();
                let mut stream_msgs: u64 = 0;
                if attempts == 1 {
                    tracing::info!("gRPC direct[{}]: upstream stream established on {}", mgr_label, node_id);
                } else {
                    tracing::info!(
                        "gRPC direct[{}]: upstream stream re-established on {} (attempt {}, replay_from={:?})",
                        mgr_label, node_id, attempts, replay_from
                    );
                }

                let stream_err: Option<(bool, String)> = loop {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => { close_reason = "cancelled"; break 'reconnect },
                        _ = downstream_tx.closed() => { close_reason = "client gone"; break 'reconnect },
                        msg = client_rx.recv(), if client_open => match msg {
                            Some(msg) => {
                                // Pings don't change filters (Yellowstone semantics); any other
                                // request replaces the subscription and must survive reconnects.
                                if msg.ping.is_none() {
                                    latest_req = msg.clone();
                                }
                                if upstream_tx.send(msg).await.is_err() {
                                    break Some((false, "upstream request side closed".to_string()));
                                }
                            }
                            None => {
                                // Client forwarder gone (half-close). Keep streaming, stop
                                // polling the request side.
                                client_open = false;
                            }
                        },
                        item = upstream_stream.next() => match item {
                            Some(Ok(msg)) => {
                                stream_msgs += 1;
                                if let Some(subscribe_update::UpdateOneof::Slot(s)) = &msg.update_oneof {
                                    if s.slot > last_slot { last_slot = s.slot; }
                                }
                                // Pongs from the upstream answer client pings we forwarded; pass them on.
                                let permit = tokio::select! {
                                    biased;
                                    _ = token.cancelled() => { close_reason = "cancelled"; break 'reconnect },
                                    _ = downstream_tx.closed() => { close_reason = "client gone"; break 'reconnect },
                                    p = downstream_tx.reserve() => match p {
                                        Ok(p) => p,
                                        Err(_) => { close_reason = "client gone"; break 'reconnect },
                                    },
                                };
                                permit.send(Ok(msg));
                                total_forwarded += 1;
                            }
                            Some(Err(e)) => break Some((is_replay_rejection(&e), e.to_string())),
                            None => break None,
                        }
                    }
                };

                let lived = established.elapsed();
                grpc_pool.invalidate(&node_id);
                match &stream_err {
                    Some((_, e)) => tracing::warn!(
                        "gRPC direct[{}]: upstream {} stream failed after {:?} ({} msgs): {} — reconnecting, client stream kept open",
                        mgr_label, node_id, lived, stream_msgs, e
                    ),
                    None => tracing::warn!(
                        "gRPC direct[{}]: upstream {} stream ended after {:?} ({} msgs) — reconnecting, client stream kept open",
                        mgr_label, node_id, lived, stream_msgs
                    ),
                }

                // Decide replay: resume from the last seen slot unless this replay attempt
                // was rejected (explicit status, or established-then-died with no data).
                let rejected = replay_from.is_some()
                    && (stream_msgs == 0 || matches!(stream_err, Some((true, _))));
                replay_from = if !replay {
                    None
                } else if rejected {
                    tracing::warn!("gRPC direct[{}]: replay from {:?} rejected, resuming live", mgr_label, replay_from);
                    None
                } else if last_slot > 0 {
                    Some(last_slot)
                } else {
                    None
                };

                if lived >= std::time::Duration::from_secs(5) {
                    backoff = std::time::Duration::from_secs(1);
                }
                tokio::select! {
                    biased;
                    _ = token.cancelled() => { close_reason = "cancelled"; break 'reconnect },
                    _ = downstream_tx.closed() => { close_reason = "client gone"; break 'reconnect },
                    _ = tokio::time::sleep(backoff) => {},
                }
                backoff = (backoff * 2).min(std::time::Duration::from_secs(10));
            }

            tracing::info!(
                "gRPC direct[{}]: stream closed: {} (lived {:?}, forwarded={}, upstream_attempts={}, last_slot={})",
                mgr_label, close_reason, started.elapsed(), total_forwarded, attempts, last_slot
            );
        });

        let handle = sb.start();
        tracing::info!("gRPC direct[{}]: {} routed to direct upstream", label, ip);
        Ok(Response::new(SupervisedStream::new(downstream_rx, handle)))
    }
}

/// Does this upstream status look like a rejected `from_slot` replay (as opposed to a
/// transport/connect failure)? Yellowstone answers an unreplayable from_slot with an
/// argument/precondition style status, never with Unavailable.
fn is_replay_rejection(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange | tonic::Code::FailedPrecondition
    ) || status.message().to_ascii_lowercase().contains("from_slot")
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
        let stream_no = STREAM_SEQ.fetch_add(1, Ordering::Relaxed);
        let label = format!("{}#{}", ip, stream_no);

        // Route: if subscription needs types the mux doesn't serve, use direct upstream
        if filters.needs_direct_connection() || !filters.has_mux_subscriptions() {
            tracing::info!(
                "gRPC subscribe[{}]: direct path (accounts={} blocks={} tx_status={} tx={} slots={} blocks_meta={} entry={} footer={} unsupported_opts={} from_slot={:?})",
                label,
                first_msg.accounts.len(),
                first_msg.blocks.len(),
                first_msg.transactions_status.len(),
                filters.transactions.len(),
                filters.slots.len(),
                filters.blocks_meta.len(),
                filters.entry.len(),
                filters.block_footer.len(),
                filters.has_unsupported_options,
                first_msg.from_slot,
            );
            return self
                .subscribe_direct(first_msg, inbound, ip, ip_stats, label)
                .await;
        }

        // Mux path: subscribe to broadcast with client's filters
        tracing::info!(
            "gRPC subscribe[{}]: mux path (tx={} slots={} blocks_meta={} entry={} footer={})",
            label,
            filters.transactions.len(),
            filters.slots.len(),
            filters.blocks_meta.len(),
            filters.entry.len(),
            filters.block_footer.len(),
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
            .subscribe_filtered(label.clone(), filters, pong_rx, token.clone());

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
                            tracing::info!("gRPC mux[{}]: client request stream error: {}", label, e);
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

    type SubscribeDeshredStream = ReceiverStream<Result<SubscribeUpdateDeshred, Status>>;

    async fn subscribe_deshred(
        &self,
        _request: Request<Streaming<SubscribeDeshredRequest>>,
    ) -> Result<Response<Self::SubscribeDeshredStream>, Status> {
        Err(Status::unimplemented("SubscribeDeshred is not supported by this proxy"))
    }

    type SubscribeGossipStream = ReceiverStream<Result<SubscribeUpdateGossip, Status>>;

    async fn subscribe_gossip(
        &self,
        _request: Request<SubscribeGossipRequest>,
    ) -> Result<Response<Self::SubscribeGossipStream>, Status> {
        Err(Status::unimplemented("SubscribeGossip is not supported by this proxy"))
    }

    async fn subscribe_replay_info(
        &self,
        request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        let (mut upstream, _, _) = self.get_upstream().await?;
        upstream.subscribe_replay_info(request).await
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
