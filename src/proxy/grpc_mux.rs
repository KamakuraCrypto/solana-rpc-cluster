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
use tokio_util::sync::CancellationToken;
use tonic::{Request, Status};

/// A stream that lived at least this long before failing is considered to have been
/// healthy, so the reconnect backoff restarts from the minimum.
const HEALTHY_STREAM_MIN: Duration = Duration::from_secs(5);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(10);

/// Slot-tracking counters shared by all upstream loops, exposed for logging/dashboard.
pub struct SlotTracker {
    /// Highest processed slot seen so far
    pub last_slot: AtomicU64,
    /// Number of processed slots whose parent was never observed (a real gap)
    pub gaps: AtomicU64,
    /// Ring of recently seen processed slots (for parent-link gap detection)
    recent: Mutex<VecDeque<u64>>,
}

impl SlotTracker {
    pub fn new() -> Self {
        Self {
            last_slot: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
            recent: Mutex::new(VecDeque::with_capacity(2048)),
        }
    }

    /// Record a slot update. Returns `Some(parent)` if the parent slot was never seen,
    /// which means we missed that slot entirely (not merely a leader-skipped slot).
    pub fn observe(&self, slot: u64, parent: Option<u64>, status: i32) -> Option<u64> {
        // Only track "processed" (SlotStatus::SlotProcessed == 0); other statuses repeat slots.
        if status != 0 {
            return None;
        }
        let mut recent = match self.recent.lock() {
            Ok(g) => g,
            Err(_) => return None,
        };
        let mut missing = None;
        if let Some(p) = parent {
            if let Some(&oldest) = recent.front() {
                // Only judge parents inside our window; older ones are unknowable.
                if p >= oldest && !recent.contains(&p) {
                    missing = Some(p);
                }
            }
        }
        if !recent.contains(&slot) {
            recent.push_back(slot);
            if recent.len() > 2048 {
                recent.pop_front();
            }
        }
        let mut cur = self.last_slot.load(Ordering::Relaxed);
        while slot > cur {
            match self.last_slot.compare_exchange_weak(cur, slot, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
        if missing.is_some() {
            self.gaps.fetch_add(1, Ordering::Relaxed);
        }
        missing
    }
}

pub struct GrpcMultiplexer {
    broadcast_tx: broadcast::Sender<Arc<SubscribeUpdate>>,
    /// Latest slot seen from upstream (for monitoring)
    pub upstream_slot: Arc<AtomicU64>,
    /// Total messages received from upstream (counts every region's deliveries)
    pub messages_received: Arc<AtomicU64>,
    /// Upstream (re)connect count, for monitoring
    pub upstream_reconnects: Arc<AtomicU64>,
    pub slots: Arc<SlotTracker>,
    client_channel_capacity: usize,
    _upstream_handles: Vec<tokio::task::JoinHandle<()>>,
}

/// Shared state handed to every upstream loop.
#[derive(Clone)]
struct UpstreamShared {
    upstream_slot: Arc<AtomicU64>,
    messages_received: Arc<AtomicU64>,
    reconnects: Arc<AtomicU64>,
    slots: Arc<SlotTracker>,
    /// Re-subscribe with `from_slot` after a reconnect to backfill the gap.
    replay_on_reconnect: bool,
}

impl GrpcMultiplexer {
    pub fn new(
        router: Arc<Router>,
        grpc_pool: Arc<GrpcPool>,
        grpc_config: &crate::config::GrpcConfig,
    ) -> Self {
        let broadcast_capacity = grpc_config.broadcast_capacity;
        let client_channel_capacity = grpc_config.client_channel_capacity;
        let (broadcast_tx, _) = broadcast::channel(broadcast_capacity);
        let upstream_slot = Arc::new(AtomicU64::new(0));
        let messages_received = Arc::new(AtomicU64::new(0));
        let reconnects = Arc::new(AtomicU64::new(0));
        let slots = Arc::new(SlotTracker::new());

        let mut handles = Vec::new();

        if grpc_config.max_lag_events != 0 {
            tracing::info!(
                "gRPC mux: max_lag_events={} is ignored — slow consumers are never disconnected, lag is only logged",
                grpc_config.max_lag_events
            );
        }

        let shared = UpstreamShared {
            upstream_slot: upstream_slot.clone(),
            messages_received: messages_received.clone(),
            reconnects: reconnects.clone(),
            slots: slots.clone(),
            replay_on_reconnect: grpc_config.replay_on_reconnect,
        };

        if grpc_config.multi_region_enabled {
            // Multi-region: spawn one upstream loop per gRPC-eligible node, all feeding
            // a shared raw channel; a dedup worker forwards first-arrivals to broadcast.
            let nodes = router.get_grpc_eligible_nodes();
            tracing::info!(
                "gRPC multiplexer: multi-region mode with {} upstream(s): {:?}",
                nodes.len(),
                nodes.iter().map(|n| n.config.id.as_str()).collect::<Vec<_>>()
            );

            let (raw_tx, raw_rx) = mpsc::channel::<(String, SubscribeUpdate)>(32_768);
            let dedup = Arc::new(DedupCache::new(Duration::from_secs(
                grpc_config.dedup_window_secs,
            )));

            for node in &nodes {
                let raw_tx = raw_tx.clone();
                let pool = grpc_pool.clone();
                let node = node.clone();
                // Multi-region: other regions cover a gap, and replayed messages older than
                // the dedup window would be forwarded as duplicates — so no replay here.
                let shared = UpstreamShared { replay_on_reconnect: false, ..shared.clone() };
                let h = tokio::spawn(async move {
                    run_upstream_loop_for_node(node, pool, raw_tx, shared).await;
                });
                handles.push(h);
            }
            drop(raw_tx);

            let dedup_for_worker = dedup.clone();
            let bc_tx = broadcast_tx.clone();
            let shared_for_worker = shared.clone();
            let nodes_for_stats = nodes.clone();
            let h = tokio::spawn(async move {
                run_dedup_worker(raw_rx, dedup_for_worker, bc_tx, shared_for_worker, nodes_for_stats)
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
            // Single-upstream mode — picks the best node each reconnect, replays from the
            // last seen slot after a reconnect so this node's own clients miss nothing.
            tracing::info!(
                "gRPC multiplexer: single-upstream mode (replay_on_reconnect={})",
                grpc_config.replay_on_reconnect
            );
            let bc_tx = broadcast_tx.clone();
            let h = tokio::spawn(async move {
                run_single_upstream_loop(router, grpc_pool, bc_tx, shared).await;
            });
            handles.push(h);
        }

        Self {
            broadcast_tx,
            upstream_slot,
            messages_received,
            upstream_reconnects: reconnects,
            slots,
            client_channel_capacity,
            _upstream_handles: handles,
        }
    }

    /// Subscribe a downstream client with compiled filters.
    /// Returns a receiver that yields filtered SubscribeUpdate messages.
    /// Also takes a sender for injecting pong responses from client pings.
    /// `cancel` lets a supervising task tear down the spawn promptly when the client
    /// disconnects, instead of waiting for the next broadcast message to notice the dead receiver.
    ///
    /// A slow consumer is never disconnected: if it falls behind the broadcast ring it
    /// loses the oldest messages (logged, counted) and keeps streaming.
    pub fn subscribe_filtered(
        &self,
        label: String,
        filters: CompiledFilters,
        pong_rx: mpsc::Receiver<SubscribeUpdate>,
        cancel: CancellationToken,
    ) -> mpsc::Receiver<Result<SubscribeUpdate, Status>> {
        let mut broadcast_rx = self.broadcast_tx.subscribe();
        let (tx, rx) = mpsc::channel::<Result<SubscribeUpdate, Status>>(self.client_channel_capacity);
        let mut pong_rx = pong_rx;

        tokio::spawn(async move {
            let started = Instant::now();
            let mut forwarded: u64 = 0;
            let mut lag_events: u64 = 0;
            let mut lag_messages: u64 = 0;
            let mut last_lag_log = Instant::now() - Duration::from_secs(3600);
            let reason;

            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => { reason = "cancelled (client gone)"; break },
                    _ = tx.closed() => { reason = "response stream dropped"; break },
                    result = broadcast_rx.recv() => {
                        match result {
                            Ok(update) => {
                                // Pongs answer the proxy's own upstream pings; never forward them.
                                if matches!(update.update_oneof, Some(UpdateOneof::Pong(_))) {
                                    continue;
                                }
                                if let Some(matched_filters) = filters.match_update(&update) {
                                    let mut fwd = (*update).clone();
                                    fwd.filters = matched_filters;
                                    match tx.try_send(Ok(fwd)) {
                                        Ok(()) => forwarded += 1,
                                        Err(mpsc::error::TrySendError::Full(msg)) => {
                                            // Channel full — wait for room (cancel-safe via reserve()).
                                            let permit = tokio::select! {
                                                biased;
                                                _ = cancel.cancelled() => { reason = "cancelled (client gone)"; break },
                                                _ = tx.closed() => { reason = "response stream dropped"; break },
                                                p = tx.reserve() => match p {
                                                    Ok(p) => p,
                                                    Err(_) => { reason = "response stream dropped"; break },
                                                },
                                            };
                                            permit.send(msg);
                                            forwarded += 1;
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            reason = "response stream dropped";
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                lag_events += 1;
                                lag_messages += n;
                                if last_lag_log.elapsed() >= Duration::from_secs(10) {
                                    last_lag_log = Instant::now();
                                    tracing::warn!(
                                        "gRPC mux[{}]: slow consumer lost {} messages (lag events={}, lost total={}); stream kept open",
                                        label, n, lag_events, lag_messages,
                                    );
                                }
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                reason = "broadcast channel closed";
                                break;
                            }
                        }
                    }
                    Some(pong) = pong_rx.recv() => {
                        let permit = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => { reason = "cancelled (client gone)"; break },
                            _ = tx.closed() => { reason = "response stream dropped"; break },
                            p = tx.reserve() => match p {
                                Ok(p) => p,
                                Err(_) => { reason = "response stream dropped"; break },
                            },
                        };
                        permit.send(Ok(pong));
                    }
                }
            }

            tracing::info!(
                "gRPC mux[{}]: stream closed: {} (lived {:?}, forwarded={}, lag_events={}, lost={})",
                label,
                reason,
                started.elapsed(),
                forwarded,
                lag_events,
                lag_messages,
            );
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
    // Alpenglow can produce competing candidate blocks per slot. bank_id is node-local,
    // so cross-region keys use consensus-global hashes to tell candidates apart.
    BlockMeta(u64, String),
    Entry(u64, u64, Vec<u8>),
    BlockFooter(u64, Vec<u8>),
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
        UpdateOneof::BlockMeta(b) => Some(DedupKey::BlockMeta(b.slot, b.blockhash.clone())),
        UpdateOneof::Entry(e) => Some(DedupKey::Entry(e.slot, e.index, e.hash.clone())),
        UpdateOneof::BlockFooter(f) => Some(DedupKey::BlockFooter(f.slot, f.bank_hash.clone())),
        _ => None, // Ping/Pong/Account/Block/EntryUpdateParent — pass through unchecked
    }
}

/// Feed a slot update into the trackers; logs a WARN if a real gap is detected.
fn track_slot(shared: &UpstreamShared, source: &str, update: &SubscribeUpdate) {
    if let Some(UpdateOneof::Slot(s)) = &update.update_oneof {
        shared.upstream_slot.store(s.slot, Ordering::Relaxed);
        if let Some(missing) = shared.slots.observe(s.slot, s.parent, s.status) {
            tracing::warn!(
                "gRPC mux[{}]: SLOT GAP — slot {} has parent {} which was never received (gaps total={})",
                source,
                s.slot,
                missing,
                shared.slots.gaps.load(Ordering::Relaxed),
            );
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn run_dedup_worker(
    mut raw_rx: mpsc::Receiver<(String, SubscribeUpdate)>,
    dedup: Arc<DedupCache>,
    broadcast_tx: broadcast::Sender<Arc<SubscribeUpdate>>,
    shared: UpstreamShared,
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
        shared.messages_received.fetch_add(1, Ordering::Relaxed);

        if let Some(n) = node_lookup.get(&node_id) {
            n.health.last_msg_ms.store(now_ms(), Ordering::Relaxed);
        }

        // Pongs answer this proxy's own pings; they are per-connection and never fanned out.
        if matches!(update.update_oneof, Some(UpdateOneof::Pong(_))) {
            continue;
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

        // Track slots on the deduplicated stream: this is exactly what clients see.
        track_slot(&shared, "aggregate", &update);

        total_out += 1;
        let _ = broadcast_tx.send(Arc::new(update));

        if last_log.elapsed() >= Duration::from_secs(60) {
            tracing::info!(
                "gRPC mux: in={} out={} dedup_rate={:.1}% subscribers={} slot={} slot_gaps={} upstream_reconnects={}",
                total_in,
                total_out,
                if total_in > 0 {
                    100.0 * (total_in - total_out) as f64 / total_in as f64
                } else {
                    0.0
                },
                broadcast_tx.receiver_count(),
                shared.upstream_slot.load(Ordering::Relaxed),
                shared.slots.gaps.load(Ordering::Relaxed),
                shared.reconnects.load(Ordering::Relaxed),
            );
            last_log = Instant::now();
        }
    }
}

// ═══════════════════════════════════════════════════════════
// Reconnect backoff
// ═══════════════════════════════════════════════════════════

struct Backoff {
    cur: Duration,
}

impl Backoff {
    fn new() -> Self {
        Self { cur: BACKOFF_MIN }
    }

    /// Compute the next sleep. A stream that was healthy for a while resets the backoff;
    /// a stream that died immediately escalates it (with 0–50% jitter).
    fn next(&mut self, stream_lived: Option<Duration>) -> Duration {
        use rand::Rng;
        if matches!(stream_lived, Some(d) if d >= HEALTHY_STREAM_MIN) {
            self.cur = BACKOFF_MIN;
        }
        let jitter_ms = rand::thread_rng().gen_range(0..=(self.cur.as_millis() as u64 / 2));
        let sleep = self.cur + Duration::from_millis(jitter_ms);
        self.cur = (self.cur * 2).min(BACKOFF_MAX);
        sleep
    }
}

/// Outcome of one upstream stream attempt.
struct StreamOutcome {
    /// How long the stream was established (None if it never got established)
    lived: Option<Duration>,
    messages: u64,
    error: Option<String>,
    /// The upstream explicitly rejected the `from_slot` replay request
    replay_rejected: bool,
}

fn is_replay_rejection(status: &Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::InvalidArgument | tonic::Code::OutOfRange | tonic::Code::FailedPrecondition
    ) || status.message().to_ascii_lowercase().contains("from_slot")
}

// ═══════════════════════════════════════════════════════════
// Per-node upstream loop (multi-region mode)
// ═══════════════════════════════════════════════════════════

async fn run_upstream_loop_for_node(
    node: Arc<NodeEntry>,
    grpc_pool: Arc<GrpcPool>,
    raw_tx: mpsc::Sender<(String, SubscribeUpdate)>,
    shared: UpstreamShared,
) {
    let node_id = node.config.id.clone();
    let mut backoff = Backoff::new();

    loop {
        shared.reconnects.fetch_add(1, Ordering::Relaxed);
        let outcome = connect_and_stream_node(&node, &grpc_pool, &raw_tx, &shared).await;
        match &outcome.error {
            None => {
                if raw_tx.is_closed() {
                    tracing::info!("gRPC mux[{}]: dedup worker gone, upstream loop exiting", node_id);
                    return;
                }
                tracing::warn!(
                    "gRPC mux[{}]: upstream stream ended (lived {:?}, {} msgs)",
                    node_id,
                    outcome.lived.unwrap_or_default(),
                    outcome.messages
                );
            }
            Some(e) => tracing::warn!(
                "gRPC mux[{}]: upstream stream failed: {} (lived {:?}, {} msgs)",
                node_id,
                e,
                outcome.lived.unwrap_or_default(),
                outcome.messages
            ),
        }
        let sleep_dur = backoff.next(outcome.lived);
        tracing::info!("gRPC mux[{}]: reconnecting in {:?}...", node_id, sleep_dur);
        tokio::time::sleep(sleep_dur).await;
    }
}

async fn open_channel(
    node: &NodeEntry,
    grpc_pool: &GrpcPool,
) -> Result<tonic::transport::Channel, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(managed) = grpc_pool.get_channel(&node.config.id) {
        Ok(managed.get().await?)
    } else {
        let endpoint = tonic::transport::Channel::from_shared(node.config.grpc_url.clone())?
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(10));
        Ok(endpoint.connect().await?)
    }
}

fn spawn_upstream_pinger(node_id: String, req_tx: mpsc::Sender<SubscribeRequest>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        let mut ping_id = 0i32;
        loop {
            interval.tick().await;
            ping_id = ping_id.wrapping_add(1);
            let ping_req = SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: ping_id }),
                ..Default::default()
            };
            if req_tx.send(ping_req).await.is_err() {
                tracing::debug!("gRPC mux[{}]: ping sender closed", node_id);
                break;
            }
        }
    })
}

async fn connect_and_stream_node(
    node: &Arc<NodeEntry>,
    grpc_pool: &GrpcPool,
    raw_tx: &mpsc::Sender<(String, SubscribeUpdate)>,
    shared: &UpstreamShared,
) -> StreamOutcome {
    let node_id = node.config.id.clone();
    let mut outcome = StreamOutcome { lived: None, messages: 0, error: None, replay_rejected: false };

    tracing::info!("gRPC mux[{}]: connecting to {}", node_id, node.config.grpc_url);

    let channel = match open_channel(node, grpc_pool).await {
        Ok(c) => c,
        Err(e) => {
            outcome.error = Some(format!("connect: {}", e));
            return outcome;
        }
    };

    let mut client = GeyserClient::new(channel);
    let (req_tx, req_rx) = mpsc::channel::<SubscribeRequest>(16);
    let _ = req_tx.send(build_mux_subscribe_request(None)).await;

    let mut request = Request::new(ReceiverStream::new(req_rx));
    if !node.config.grpc_x_token.is_empty() {
        if let Ok(val) = node.config.grpc_x_token.parse() {
            request.metadata_mut().insert("x-token", val);
        }
    }

    let mut stream = match client.subscribe(request).await {
        Ok(r) => r.into_inner(),
        Err(e) => {
            grpc_pool.invalidate(&node_id);
            outcome.error = Some(format!("subscribe: {}", e));
            return outcome;
        }
    };

    let established = Instant::now();
    tracing::info!("gRPC mux[{}]: upstream stream established", node_id);
    let ping_handle = spawn_upstream_pinger(node_id.clone(), req_tx.clone());

    use futures_util::StreamExt;
    while let Some(result) = stream.next().await {
        match result {
            Ok(update) => {
                outcome.messages += 1;
                if raw_tx.send((node_id.clone(), update)).await.is_err() {
                    break; // dedup worker dropped
                }
            }
            Err(e) => {
                grpc_pool.invalidate(&node_id);
                outcome.error = Some(e.to_string());
                break;
            }
        }
    }
    let _ = shared; // slot tracking happens on the deduplicated stream
    ping_handle.abort();
    outcome.lived = Some(established.elapsed());
    outcome
}

// ═══════════════════════════════════════════════════════════
// Single-upstream loop (when multi_region_enabled = false)
// ═══════════════════════════════════════════════════════════

async fn run_single_upstream_loop(
    router: Arc<Router>,
    grpc_pool: Arc<GrpcPool>,
    broadcast_tx: broadcast::Sender<Arc<SubscribeUpdate>>,
    shared: UpstreamShared,
) {
    let mut backoff = Backoff::new();
    // Slot to replay from on the next (re)connect, if any.
    let mut replay_from: Option<u64> = None;

    loop {
        // Prefer a healthy node, but never refuse to connect just because the health
        // checker's probe failed — the stream itself is the real health signal.
        let node = router.pick_grpc_node().or_else(|| {
            let eligible = router.get_grpc_eligible_nodes();
            if !eligible.is_empty() {
                tracing::warn!(
                    "gRPC multiplexer: no node passes health checks, trying {} anyway",
                    eligible[0].config.id
                );
            }
            eligible.into_iter().min_by_key(|n| n.config.priority)
        });

        let node = match node {
            Some(n) => n,
            None => {
                tracing::error!("gRPC multiplexer: no gRPC-eligible nodes configured");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        shared.reconnects.fetch_add(1, Ordering::Relaxed);
        let outcome =
            connect_and_stream_single(&node, &grpc_pool, &broadcast_tx, &shared, replay_from).await;

        match &outcome.error {
            None => tracing::warn!(
                "gRPC multiplexer[{}]: upstream stream ended (lived {:?}, {} msgs)",
                node.config.id,
                outcome.lived.unwrap_or_default(),
                outcome.messages
            ),
            Some(e) => tracing::warn!(
                "gRPC multiplexer[{}]: upstream stream failed: {} (lived {:?}, {} msgs)",
                node.config.id,
                e,
                outcome.lived.unwrap_or_default(),
                outcome.messages
            ),
        }

        // Replay from the slot after the last one we fully observed. If a replay attempt
        // itself failed before delivering anything, the server rejected from_slot (too old
        // or unsupported) — fall back to a live subscription.
        // A plain connect failure (never established) keeps the replay slot; only an
        // established stream that delivered nothing, or an explicit rejection, drops it.
        let rejected = replay_from.is_some()
            && ((outcome.lived.is_some() && outcome.messages == 0) || outcome.replay_rejected);
        replay_from = if !shared.replay_on_reconnect {
            None
        } else if rejected {
            tracing::warn!(
                "gRPC multiplexer[{}]: replay from slot {:?} rejected, reconnecting live",
                node.config.id,
                replay_from
            );
            None
        } else {
            let last = shared.slots.last_slot.load(Ordering::Relaxed);
            if last > 0 { Some(last) } else { None }
        };

        let sleep_dur = backoff.next(outcome.lived);
        tracing::info!(
            "gRPC multiplexer: reconnecting in {:?} (replay_from={:?})...",
            sleep_dur,
            replay_from
        );
        tokio::time::sleep(sleep_dur).await;
    }
}

async fn connect_and_stream_single(
    node: &Arc<NodeEntry>,
    grpc_pool: &GrpcPool,
    broadcast_tx: &broadcast::Sender<Arc<SubscribeUpdate>>,
    shared: &UpstreamShared,
    replay_from: Option<u64>,
) -> StreamOutcome {
    let node_id = node.config.id.clone();
    let mut outcome = StreamOutcome { lived: None, messages: 0, error: None, replay_rejected: false };

    tracing::info!(
        "gRPC multiplexer: connecting to node {} ({}) replay_from={:?}",
        node_id,
        node.config.grpc_url,
        replay_from
    );

    let channel = match open_channel(node, grpc_pool).await {
        Ok(c) => c,
        Err(e) => {
            outcome.error = Some(format!("connect: {}", e));
            return outcome;
        }
    };

    let mut client = GeyserClient::new(channel);
    let (req_tx, req_rx) = mpsc::channel::<SubscribeRequest>(16);
    let _ = req_tx.send(build_mux_subscribe_request(replay_from)).await;

    let mut request = Request::new(ReceiverStream::new(req_rx));
    if !node.config.grpc_x_token.is_empty() {
        if let Ok(val) = node.config.grpc_x_token.parse() {
            request.metadata_mut().insert("x-token", val);
        }
    }

    let mut stream = match client.subscribe(request).await {
        Ok(r) => r.into_inner(),
        Err(e) => {
            grpc_pool.invalidate(&node_id);
            outcome.replay_rejected = replay_from.is_some() && is_replay_rejection(&e);
            outcome.error = Some(format!("subscribe: {}", e));
            return outcome;
        }
    };

    let established = Instant::now();
    tracing::info!("gRPC multiplexer: upstream stream established on node {}", node_id);
    let ping_handle = spawn_upstream_pinger(node_id.clone(), req_tx.clone());

    use futures_util::StreamExt;
    let mut last_log = Instant::now();

    while let Some(result) = stream.next().await {
        match result {
            Ok(update) => {
                outcome.messages += 1;
                shared.messages_received.fetch_add(1, Ordering::Relaxed);
                node.health.last_msg_ms.store(now_ms(), Ordering::Relaxed);
                node.health.wins.fetch_add(1, Ordering::Relaxed);

                if matches!(update.update_oneof, Some(UpdateOneof::Pong(_))) {
                    continue;
                }
                track_slot(shared, &node_id, &update);

                if last_log.elapsed() >= Duration::from_secs(60) {
                    tracing::info!(
                        "gRPC mux: {} messages this stream, {} active subscribers, slot {}, slot_gaps={}, upstream_reconnects={}",
                        outcome.messages,
                        broadcast_tx.receiver_count(),
                        shared.upstream_slot.load(Ordering::Relaxed),
                        shared.slots.gaps.load(Ordering::Relaxed),
                        shared.reconnects.load(Ordering::Relaxed),
                    );
                    last_log = Instant::now();
                }

                let _ = broadcast_tx.send(Arc::new(update));
            }
            Err(e) => {
                grpc_pool.invalidate(&node_id);
                outcome.replay_rejected = replay_from.is_some() && is_replay_rejection(&e);
                outcome.error = Some(e.to_string());
                break;
            }
        }
    }

    ping_handle.abort();
    outcome.lived = Some(established.elapsed());
    outcome
}

fn build_mux_subscribe_request(from_slot: Option<u64>) -> SubscribeRequest {
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
            cuckoo_account_include: None,
            token_accounts: None,
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
    entry.insert(
        "mux_entry".to_string(),
        SubscribeRequestFilterEntry {
            include_update_parent: None,
        },
    );

    let mut block_footer = HashMap::new();
    block_footer.insert(
        "mux_block_footer".to_string(),
        SubscribeRequestFilterBlockFooter {
            include_certificates: None,
        },
    );

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
        from_slot,
        block_footer,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(slot: u64, blockhash: &str, bank_id: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            filters: vec![],
            created_at: None,
            update_oneof: Some(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot,
                blockhash: blockhash.to_string(),
                bank_id,
                ..Default::default()
            })),
        }
    }

    #[test]
    fn dedup_keeps_competing_candidates_apart() {
        // Two candidate blocks in the same slot must both pass the dedup.
        assert_ne!(
            compute_dedup_key(&meta(10, "candA", 1)),
            compute_dedup_key(&meta(10, "candB", 2)),
        );
    }

    #[test]
    fn dedup_ignores_node_local_bank_id() {
        // Same block seen via two regions (different node-local bank_id) is one key.
        assert_eq!(
            compute_dedup_key(&meta(10, "candA", 1)),
            compute_dedup_key(&meta(10, "candA", 77)),
        );
    }

    #[test]
    fn slot_tracker_detects_missing_parent_but_not_skipped_slots() {
        let t = SlotTracker::new();
        assert_eq!(t.observe(100, None, 0), None);
        assert_eq!(t.observe(101, Some(100), 0), None);
        // Leader skipped 102: 103's parent is 101, which we saw — not a gap.
        assert_eq!(t.observe(103, Some(101), 0), None);
        // We never received 104 but 105 says it is its parent — a real gap.
        assert_eq!(t.observe(105, Some(104), 0), Some(104));
        assert_eq!(t.gaps.load(Ordering::Relaxed), 1);
        // Non-processed statuses are ignored.
        assert_eq!(t.observe(200, Some(199), 1), None);
        assert_eq!(t.last_slot.load(Ordering::Relaxed), 105);
    }

    #[test]
    fn backoff_resets_after_healthy_stream() {
        let mut b = Backoff::new();
        let _ = b.next(None);
        let _ = b.next(Some(Duration::from_millis(10)));
        assert_eq!(b.cur, Duration::from_secs(4));
        let s = b.next(Some(Duration::from_secs(60)));
        assert!(s < Duration::from_secs(2), "healthy stream must reset to minimum, got {:?}", s);
    }
}
