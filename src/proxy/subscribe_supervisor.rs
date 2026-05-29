use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;

/// Cooperative shutdown grace before forced abort.
const COOP_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// Counters consumed by the supervisor. Shared with the dashboard.
#[derive(Clone)]
pub struct SupervisorCounters {
    pub active: Arc<AtomicU64>,
    pub drops_total: Arc<AtomicU64>,
    pub forwarder_aborted_total: Arc<AtomicU64>,
}

/// Builder collects spawned forwarder tasks and the cancellation token they share.
/// Call `start()` after wiring up forwarders to spawn the supervisor and get a handle
/// that, when dropped, triggers cooperative shutdown.
pub struct SupervisorBuilder {
    token: CancellationToken,
    joinset: JoinSet<()>,
    per_ip_counter: Arc<AtomicU64>,
    global: SupervisorCounters,
}

impl SupervisorBuilder {
    pub fn new(per_ip_counter: Arc<AtomicU64>, global: SupervisorCounters) -> Self {
        Self {
            token: CancellationToken::new(),
            joinset: JoinSet::new(),
            per_ip_counter,
            global,
        }
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn spawn<F>(&mut self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.joinset.spawn(fut);
    }

    /// Increment both per-IP and global active counters, then spawn the supervisor task.
    /// The returned handle must be embedded in the response stream so its drop drives shutdown.
    pub fn start(self) -> SupervisorHandle {
        self.per_ip_counter.fetch_add(1, Ordering::Relaxed);
        self.global.active.fetch_add(1, Ordering::Relaxed);

        let (drop_tx, drop_rx) = oneshot::channel::<()>();
        let token = self.token;
        let per_ip = self.per_ip_counter;
        let global = self.global;
        let mut joinset = self.joinset;

        tokio::spawn(async move {
            // Wait for any termination signal: handle dropped, external cancel,
            // or all children finished on their own.
            tokio::select! {
                _ = drop_rx => {},
                _ = token.cancelled() => {},
                _ = async {
                    while joinset.join_next().await.is_some() {}
                } => {},
            }

            // Signal cooperative shutdown to anything still running.
            token.cancel();

            // Give children a brief window to exit cleanly.
            let grace = tokio::time::sleep(COOP_SHUTDOWN_GRACE);
            tokio::pin!(grace);
            loop {
                tokio::select! {
                    _ = &mut grace => break,
                    res = joinset.join_next() => {
                        if res.is_none() { break; }
                    }
                }
            }

            // Anything still alive gets killed; count it so we can alert on it.
            let remaining = joinset.len();
            if remaining > 0 {
                global
                    .forwarder_aborted_total
                    .fetch_add(remaining as u64, Ordering::Relaxed);
                joinset.abort_all();
                while joinset.join_next().await.is_some() {}
            }

            // Decrement counters exactly once.
            per_ip.fetch_sub(1, Ordering::Relaxed);
            global.active.fetch_sub(1, Ordering::Relaxed);
            global.drops_total.fetch_add(1, Ordering::Relaxed);
        });

        SupervisorHandle { _drop_signal: drop_tx }
    }
}

/// Lifetime-binding handle for a supervised subscribe. Dropping it tells the supervisor
/// task to begin cooperative shutdown. Embed inside a `SupervisedStream`.
pub struct SupervisorHandle {
    _drop_signal: oneshot::Sender<()>,
}

/// Wraps an `Unpin` response stream and carries a `SupervisorHandle`. The handle drops
/// when the stream drops, which is when tonic's response body is finished — i.e., when
/// the client disconnects or the upstream stream ends.
pub struct SupervisedStream<T> {
    inner: tokio_stream::wrappers::ReceiverStream<T>,
    _handle: SupervisorHandle,
}

impl<T> SupervisedStream<T> {
    pub fn new(rx: tokio::sync::mpsc::Receiver<T>, handle: SupervisorHandle) -> Self {
        Self {
            inner: tokio_stream::wrappers::ReceiverStream::new(rx),
            _handle: handle,
        }
    }
}

impl<T> Stream for SupervisedStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use tokio::sync::mpsc;
    use tokio::time::{sleep, timeout, Duration as TokioDuration};

    fn make_counters() -> (Arc<AtomicU64>, SupervisorCounters) {
        let per_ip = Arc::new(AtomicU64::new(0));
        let global = SupervisorCounters {
            active: Arc::new(AtomicU64::new(0)),
            drops_total: Arc::new(AtomicU64::new(0)),
            forwarder_aborted_total: Arc::new(AtomicU64::new(0)),
        };
        (per_ip, global)
    }

    /// Regression: dropping the response stream while upstream is silent must
    /// promptly cancel the forwarder and drop the counter.
    #[tokio::test]
    async fn drop_during_idle_upstream_cancels_and_decrements() {
        let (per_ip, global) = make_counters();
        let mut sb = SupervisorBuilder::new(per_ip.clone(), global.clone());
        let token = sb.token();

        let (downstream_tx, downstream_rx) = mpsc::channel::<u32>(8);

        // Forwarder simulating an upstream that never sends. It must exit on cancel.
        sb.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => break,
                    _ = downstream_tx.closed() => break,
                    _ = sleep(TokioDuration::from_secs(60)) => {},
                }
            }
        });

        let handle = sb.start();
        let stream = SupervisedStream::new(downstream_rx, handle);
        assert_eq!(per_ip.load(Ordering::Relaxed), 1);
        assert_eq!(global.active.load(Ordering::Relaxed), 1);

        drop(stream);

        // Supervisor must finish shutdown within the cooperative window.
        let deadline = TokioDuration::from_secs(3);
        timeout(deadline, async {
            while global.drops_total.load(Ordering::Relaxed) == 0 {
                sleep(TokioDuration::from_millis(10)).await;
            }
        })
        .await
        .expect("supervisor did not finish in time");

        assert_eq!(per_ip.load(Ordering::Relaxed), 0);
        assert_eq!(global.active.load(Ordering::Relaxed), 0);
        assert_eq!(global.forwarder_aborted_total.load(Ordering::Relaxed), 0);
    }

    /// Misbehaving forwarder that ignores cancellation must be aborted and counted.
    #[tokio::test]
    async fn stuck_forwarder_is_aborted() {
        let (per_ip, global) = make_counters();
        let mut sb = SupervisorBuilder::new(per_ip.clone(), global.clone());

        sb.spawn(async move {
            // Sleeps for a very long time, ignoring cancellation entirely.
            sleep(TokioDuration::from_secs(3600)).await;
        });

        let handle = sb.start();
        drop(handle);

        let deadline = TokioDuration::from_secs(5);
        timeout(deadline, async {
            while global.drops_total.load(Ordering::Relaxed) == 0 {
                sleep(TokioDuration::from_millis(20)).await;
            }
        })
        .await
        .expect("supervisor did not finish in time");

        assert_eq!(per_ip.load(Ordering::Relaxed), 0);
        assert!(global.forwarder_aborted_total.load(Ordering::Relaxed) >= 1);
    }
}
