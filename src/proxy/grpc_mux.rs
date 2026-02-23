use super::grpc_filter::CompiledFilters;
use super::grpc_yellowstone::geyser_proto::geyser_client::GeyserClient;
use super::grpc_yellowstone::geyser_proto::subscribe_update::UpdateOneof;
use super::grpc_yellowstone::geyser_proto::*;
use crate::cluster::grpc_pool::GrpcPool;
use crate::cluster::router::Router;
use crate::cluster::NodeEntry;
use dashmap::DashMap;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Status};

pub struct GrpcMultiplexer {
    broadcast_tx: broadcast::Sender<Arc<SubscribeUpdate>>,
    /// Latest slot seen from upstream (for monitoring)
    pub upstream_slot: Arc<AtomicU64>,
    /// Total messages received from upstream (counts every region's deliveries)
    pub messages_received: Arc<AtomicU64>,
    client_channel_capacity: usize,
    max_lag_events: u64,
    _upstream_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl GrpcMultiplexer {
    pub fn new(
        router: Arc<Router>,
        grpc_pool: Arc<GrpcPool>,
        grpc_config: &crate::config::GrpcConfig,
    ) -> Self {
        let broadcast_capacity = grpc_config.broadcast_capacity;
        let client_channel_capacity = grpc_config.client_channel_capacity;
        let max_lag_events = grpc_config.max_lag_events;
        let (broadcast_tx, _) = broadcast::channel(broadcast_capacity);
        let upstream_slot = Arc::new(AtomicU64::new(0));
        let messages_received = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();

        if grpc_config.multi_region_enabled {
            // Multi-region: spawn one upstream loop per gRPC-eligible node, all feeding
            // a shared raw channel; a dedup worker forwards first-arrivals to broadcast.
            let nodes = router.get_grpc_eligible_nodes();
            tracing::info!(
                "gRPC multiplexer: multi-region mode with {} upstream(s)",
                nodes.len()
            );

            let (raw_tx, raw_rx) = mpsc::channel::<(String, SubscribeUpdate)>(32_768);
            let dedup = Arc::new(DedupCache::new(Duration::from_secs(
                grpc_config.dedup_window_secs,
            )));

            for node in &nodes {
                let raw_tx = raw_tx.clone();
                let pool = grpc_pool.clone();
                let node = node.clone();
                let h = tokio::spawn(async move {
                    run_upstream_loop_for_node(node, pool, raw_tx).await;
                });
                handles.push(h);
            }
            drop(raw_tx);

            let dedup_for_worker = dedup.clone();
            let bc_tx = broadcast_tx.clone();
            let slot_clone = upstream_slot.clone();
            let msgs_clone = messages_received.clone();
            let nodes_for_stats = nodes.clone();
            let h = tokio::spawn(async move {
                run_dedup_worker(
                    raw_rx,
                    dedup_for_worker,
                    bc_tx,
                    slot_clone,
                    msgs_clone,
                    nodes_for_stats,
                )
                .await;
            });
            handles.push(h);

            // TTL eviction task
            let dedup_evict = dedup.clone();
            let evict_interval =
                Duration::from_secs(grpc_config.dedup_eviction_interval_secs.max(1));
            let h = tokio::spawn(async move {
                let mut tick = tokio::time::interval(evict_interval);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    dedup_evict.evict_expired();
                }
            });
            handles.push(h);
        } else {
            // Legacy single-upstream mode — picks the best healthy node each reconnect
            tracing::info!("gRPC multiplexer: single-upstream mode");
            let bc_tx = broadcast_tx.clone();
            let slot = upstream_slot.clone();
            let msgs = messages_received.clone();
            let h = tokio::spawn(async move {
                run_single_upstream_loop(router, grpc_pool, bc_tx, slot, msgs).await;
            });
            handles.push(h);
        }

        Self {
            broadcast_tx,
            upstream_slot,
            messages_received,
            client_channel_capacity,
            max_lag_events,
            _upstream_handles: handles,
        }
    }

    /// Subscribe a downstream client with compiled filters.
    /// Returns a receiver that yields filtered SubscribeUpdate messages.
    /// Also takes a sender for injecting pong responses from client pings.
    pub fn subscribe_filtered(
        &self,
        filters: CompiledFilters,
        pong_rx: mpsc::Receiver<SubscribeUpdate>,
    ) -> mpsc::Receiver<Result<SubscribeUpdate, Status>> {
        let mut broadcast_rx = self.broadcast_tx.subscribe();
        let (tx, rx) = mpsc::channel::<Result<SubscribeUpdate, Status>>(self.client_channel_capacity);
        let mut pong_rx = pong_rx;
        let max_lag_events = self.max_lag_events;

        tokio::spawn(async move {
            let mut lag_count: u64 = 0;

            loop {
                tokio::select! {
                    result = broadcast_rx.recv() => {
                        match result {
                            Ok(update) => {
                                if let Some(matched_filters) = filters.match_update(&update) {
                                    let mut forwarded = (*update).clone();
                                    forwarded.filters = matched_filters;
                                    match tx.try_send(Ok(forwarded)) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(msg)) => {
                                            // Channel full — block briefly for momentary spikes
                                            if tx.send(msg).await.is_err() {
                                                break;
                                            }
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                lag_count += 1;
                                tracing::warn!(
                                    "gRPC mux: downstream consumer lagged by {} messages (event {}/{})",
                                    n, lag_count, max_lag_events,
                                );
                                if lag_count >= max_lag_events {
                                    tracing::warn!(
                                        "gRPC mux: disconnecting slow consumer after {} lag events",
                                        lag_count,
                                    );
                                    let _ = tx.send(Err(Status::data_loss(
                                        "Stream too slow: disconnected due to repeated message lag"
                                    ))).await;
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                tracing::info!("gRPC mux: broadcast channel closed");
                                break;
                            }
                        }
                    }
                    Some(pong) = pong_rx.recv() => {
                        if tx.send(Ok(pong)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        rx
    }

    pub fn active_subscribers(&self) -> usize {
        self.broadcast_tx.receiver_count()
    }
}

// ═══════════════════════════════════════════════════════════
// Dedup
// ═══════════════════════════════════════════════════════════

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
enum DedupKey {
    Transaction(Vec<u8>),
    TransactionStatus(Vec<u8>),
    Slot(u64, i32),
    BlockMeta(u64),
    Entry(u64, u64),
}

struct DedupCache {
    seen: DashMap<DedupKey, ()>,
    fifo: Mutex<VecDeque<(DedupKey, Instant)>>,
    ttl: Duration,
}

impl DedupCache {
    fn new(ttl: Duration) -> Self {
        Self {
            seen: DashMap::with_capacity(65536),
            fifo: Mutex::new(VecDeque::with_capacity(65536)),
            ttl,
        }
    }

    /// Returns true if this is the first time we've seen this key.
    fn check_and_insert(&self, key: DedupKey) -> bool {
        use dashmap::mapref::entry::Entry;
        match self.seen.entry(key.clone()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(());
                if let Ok(mut fifo) = self.fifo.lock() {
                    fifo.push_back((key, Instant::now()));
                }
                true
            }
        }
    }

    /// Evict entries older than ttl.
    fn evict_expired(&self) {
        let mut fifo = match self.fifo.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let now = Instant::now();
        while let Some((_, when)) = fifo.front() {
            if now.duration_since(*when) >= self.ttl {
                if let Some((k, _)) = fifo.pop_front() {
                    self.seen.remove(&k);
                }
            } else {
                break;
            }
        }
    }
}

fn compute_dedup_key(update: &SubscribeUpdate) -> Option<DedupKey> {
    match update.update_oneof.as_ref()? {
        UpdateOneof::Transaction(tx) => {
            let info = tx.transaction.as_ref()?;
            Some(DedupKey::Transaction(info.signature.clone()))
        }
        UpdateOneof::TransactionStatus(s) => {
            Some(DedupKey::TransactionStatus(s.signature.clone()))
        }
        UpdateOneof::Slot(s) => Some(DedupKey::Slot(s.slot, s.status)),
        UpdateOneof::BlockMeta(b) => Some(DedupKey::BlockMeta(b.slot)),
        UpdateOneof::Entry(e) => Some(DedupKey::Entry(e.slot, e.index)),
        _ => None, // Ping/Pong/Account/Block — pass through unchecked
    }
}

async fn run_dedup_worker(
    mut raw_rx: mpsc::Receiver<(String, SubscribeUpdate)>,
    dedup: Arc<DedupCache>,
    broadcast_tx: broadcast::Sender<Arc<SubscribeUpdate>>,
    upstream_slot: Arc<AtomicU64>,
    messages_received: Arc<AtomicU64>,
    nodes: Vec<Arc<NodeEntry>>,
) {
    let node_lookup: HashMap<String, Arc<NodeEntry>> = nodes
        .into_iter()
        .map(|n| (n.config.id.clone(), n))
        .collect();

    let mut last_log = Instant::now();
    let mut total_in: u64 = 0;
    let mut total_out: u64 = 0;

    while let Some((node_id, update)) = raw_rx.recv().await {
        total_in += 1;
        messages_received.fetch_add(1, Ordering::Relaxed);

        if let Some(n) = node_lookup.get(&node_id) {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            n.health.last_msg_ms.store(now_ms, Ordering::Relaxed);
        }

        if let Some(UpdateOneof::Slot(s)) = &update.update_oneof {
            upstream_slot.store(s.slot, Ordering::Relaxed);
        }

        let is_new = match compute_dedup_key(&update) {
            Some(k) => dedup.check_and_insert(k),
            None => true,
        };

        if !is_new {
            if let Some(n) = node_lookup.get(&node_id) {
                n.health.dupes.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }

        if let Some(n) = node_lookup.get(&node_id) {
            n.health.wins.fetch_add(1, Ordering::Relaxed);
        }

        total_out += 1;
        let _ = broadcast_tx.send(Arc::new(update));

        if last_log.elapsed() >= Duration::from_secs(60) {
            tracing::info!(
                "gRPC mux: in={} out={} dedup_rate={:.1}% subscribers={} slot={}",
                total_in,
                total_out,
                if total_in > 0 {
                    100.0 * (total_in - total_out) as f64 / total_in as f64
                } else {
                    0.0
                },
                broadcast_tx.receiver_count(),
                upstream_slot.load(Ordering::Relaxed),
            );
            last_log = Instant::now();
        }
    }
}

// ═══════════════════════════════════════════════════════════
// Per-node upstream loop (multi-region mode)
// ═══════════════════════════════════════════════════════════

async fn run_upstream_loop_for_node(
    node: Arc<NodeEntry>,
    grpc_pool: Arc<GrpcPool>,
    raw_tx: mpsc::Sender<(String, SubscribeUpdate)>,
) {
    use rand::Rng;

    let node_id = node.config.id.clone();
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        tracing::info!("gRPC mux[{}]: connecting to upstream...", node_id);

        match connect_and_stream_node(&node, &grpc_pool, &raw_tx).await {
            Ok(()) => {
                tracing::info!("gRPC mux[{}]: upstream stream ended cleanly", node_id);
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                tracing::warn!("gRPC mux[{}]: upstream error: {}", node_id, e);
            }
        }

        // Add jitter: random 0-50% of backoff duration to prevent thundering herd
        let jitter_ms = rand::thread_rng().gen_range(0..=(backoff.as_millis() as u64 / 2));
        let sleep_dur = backoff + Duration::from_millis(jitter_ms);
        tracing::info!(
            "gRPC mux[{}]: reconnecting in {:?}...",
            node_id,
            sleep_dur
        );
        tokio::time::sleep(sleep_dur).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn connect_and_stream_node(
    node: &Arc<NodeEntry>,
    grpc_pool: &GrpcPool,
    raw_tx: &mpsc::Sender<(String, SubscribeUpdate)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let node_id = node.config.id.clone();
    let x_token = node.config.grpc_x_token.clone();

    tracing::info!(
        "gRPC mux[{}]: using upstream {}",
        node_id,
        node.config.grpc_url
    );

    let channel = if let Some(managed) = grpc_pool.get_channel(&node_id) {
        managed.get().await?
    } else {
        let endpoint = tonic::transport::Channel::from_shared(node.config.grpc_url.clone())?
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(10));
        endpoint.connect().await?
    };

    let mut client = GeyserClient::new(channel);
    let subscribe_req = build_mux_subscribe_request();

    let (req_tx, req_rx) = mpsc::channel::<SubscribeRequest>(16);
    req_tx
        .send(subscribe_req)
        .await
        .map_err(|_| "Failed to send initial request")?;

    let mut request = Request::new(ReceiverStream::new(req_rx));
    if !x_token.is_empty() {
        if let Ok(val) = x_token.parse() {
            request.metadata_mut().insert("x-token", val);
        }
    }

    let response = client.subscribe(request).await?;
    let mut stream = response.into_inner();

    tracing::info!("gRPC mux[{}]: upstream stream established", node_id);

    let ping_node_id = node_id.clone();
    let ping_tx = req_tx.clone();
    let ping_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let mut ping_id = 0i32;
        loop {
            interval.tick().await;
            ping_id = ping_id.wrapping_add(1);
            let ping_req = SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: ping_id }),
                ..Default::default()
            };
            if ping_tx.send(ping_req).await.is_err() {
                tracing::debug!("gRPC mux[{}]: ping sender closed", ping_node_id);
                break;
            }
        }
    });

    use futures_util::StreamExt;
    while let Some(result) = stream.next().await {
        match result {
            Ok(update) => {
                if raw_tx.send((node_id.clone(), update)).await.is_err() {
                    // Dedup worker dropped — shut down cleanly
                    ping_handle.abort();
                    return Ok(());
                }
            }
            Err(e) => {
                tracing::warn!("gRPC mux[{}]: upstream stream error: {}", node_id, e);
                grpc_pool.invalidate(&node_id);
                ping_handle.abort();
                return Err(Box::new(e));
            }
        }
    }

    ping_handle.abort();
    Ok(())
}

// ═══════════════════════════════════════════════════════════
// Legacy single-upstream loop (when multi_region_enabled = false)
// ═══════════════════════════════════════════════════════════

async fn run_single_upstream_loop(
    router: Arc<Router>,
    grpc_pool: Arc<GrpcPool>,
    broadcast_tx: broadcast::Sender<Arc<SubscribeUpdate>>,
    upstream_slot: Arc<AtomicU64>,
    messages_received: Arc<AtomicU64>,
) {
    use rand::Rng;

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    loop {
        tracing::info!("gRPC multiplexer: connecting to upstream...");

        match connect_and_stream_single(
            &router,
            &grpc_pool,
            &broadcast_tx,
            &upstream_slot,
            &messages_received,
        )
        .await
        {
            Ok(()) => {
                tracing::info!("gRPC multiplexer: upstream stream ended cleanly");
                backoff = Duration::from_secs(1);
            }
            Err(e) => {
                tracing::warn!("gRPC multiplexer: upstream error: {}", e);
            }
        }

        let jitter_ms = rand::thread_rng().gen_range(0..=(backoff.as_millis() as u64 / 2));
        let sleep_dur = backoff + Duration::from_millis(jitter_ms);
        tracing::info!("gRPC multiplexer: reconnecting in {:?}...", sleep_dur);
        tokio::time::sleep(sleep_dur).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn connect_and_stream_single(
    router: &Router,
    grpc_pool: &GrpcPool,
    broadcast_tx: &broadcast::Sender<Arc<SubscribeUpdate>>,
    upstream_slot: &AtomicU64,
    messages_received: &AtomicU64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let node = router.pick_grpc_node().ok_or("No healthy gRPC nodes")?;
    let node_id = node.config.id.clone();
    let x_token = node.config.grpc_x_token.clone();

    tracing::info!(
        "gRPC multiplexer: using node {} ({})",
        node_id,
        node.config.grpc_url
    );

    let channel = if let Some(managed) = grpc_pool.get_channel(&node_id) {
        managed.get().await?
    } else {
        let endpoint = tonic::transport::Channel::from_shared(node.config.grpc_url.clone())?
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(10));
        endpoint.connect().await?
    };

    let mut client = GeyserClient::new(channel);
    let subscribe_req = build_mux_subscribe_request();

    let (req_tx, req_rx) = mpsc::channel::<SubscribeRequest>(16);
    req_tx
        .send(subscribe_req)
        .await
        .map_err(|_| "Failed to send initial request")?;

    let mut request = Request::new(ReceiverStream::new(req_rx));
    if !x_token.is_empty() {
        if let Ok(val) = x_token.parse() {
            request.metadata_mut().insert("x-token", val);
        }
    }

    let response = client.subscribe(request).await?;
    let mut stream = response.into_inner();

    tracing::info!(
        "gRPC multiplexer: upstream stream established on node {}",
        node_id
    );

    let ping_tx = req_tx.clone();
    let ping_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let mut ping_id = 0i32;
        loop {
            interval.tick().await;
            ping_id = ping_id.wrapping_add(1);
            let ping_req = SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: ping_id }),
                ..Default::default()
            };
            if ping_tx.send(ping_req).await.is_err() {
                break;
            }
        }
    });

    use futures_util::StreamExt;
    let mut msg_count: u64 = 0;
    let mut last_log = Instant::now();

    while let Some(result) = stream.next().await {
        match result {
            Ok(update) => {
                msg_count += 1;
                messages_received.fetch_add(1, Ordering::Relaxed);

                if let Some(ref oneof) = update.update_oneof {
                    if let UpdateOneof::Slot(s) = oneof {
                        upstream_slot.store(s.slot, Ordering::Relaxed);
                    }
                }

                if last_log.elapsed() >= Duration::from_secs(60) {
                    tracing::info!(
                        "gRPC mux: {} messages processed, {} active subscribers, slot {}",
                        msg_count,
                        broadcast_tx.receiver_count(),
                        upstream_slot.load(Ordering::Relaxed),
                    );
                    last_log = Instant::now();
                }

                let _ = broadcast_tx.send(Arc::new(update));
            }
            Err(e) => {
                tracing::warn!("gRPC multiplexer: upstream stream error: {}", e);
                grpc_pool.invalidate(&node_id);
                ping_handle.abort();
                return Err(Box::new(e));
            }
        }
    }

    ping_handle.abort();
    Ok(())
}

fn build_mux_subscribe_request() -> SubscribeRequest {
    let mut transactions = HashMap::new();
    transactions.insert(
        "mux_tx".to_string(),
        SubscribeRequestFilterTransactions {
            vote: None,
            failed: None,
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    let mut slots = HashMap::new();
    slots.insert(
        "mux_slot".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: Some(false),
        },
    );

    let mut blocks_meta = HashMap::new();
    blocks_meta.insert(
        "mux_blocks_meta".to_string(),
        SubscribeRequestFilterBlocksMeta {},
    );

    let mut entry = HashMap::new();
    entry.insert("mux_entry".to_string(), SubscribeRequestFilterEntry {});

    SubscribeRequest {
        accounts: HashMap::new(),
        slots,
        transactions,
        transactions_status: HashMap::new(),
        blocks: HashMap::new(),
        blocks_meta,
        entry,
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}
