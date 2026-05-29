// Phase 0.6 — Yellowstone shadow test (v2)
//
// Probes a real Yellowstone-gRPC endpoint to validate Design H assumptions:
//   - Does the server apply mid-stream `SubscribeRequest` updates atomically?
//   - Can a single account-filter union grow to 25k+ pubkeys?
//   - Do unrelated update types (slots, blocks_meta) pause during filter changes?
//   - Does newly-added pubkey membership take effect (with real active wallets)?
//   - What's the per-IP concurrent-stream cap behavior?
//
// Usage:
//   yellowstone_shadow_test <endpoint> <wallets_json_path>
//
// wallets_json: JSON array of base58 pubkey strings (e.g., from
// https://github.com/nyxx-stack/solana-active-wallets-public latest/daily.json).
//
// Run from a box already whitelisted for the target endpoint.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Request;

pub mod solana {
    pub mod storage {
        pub mod confirmed_block {
            tonic::include_proto!("solana.storage.confirmed_block");
        }
    }
}

mod geyser_proto {
    tonic::include_proto!("geyser");
}

use futures_util::StreamExt;
use geyser_proto::geyser_client::GeyserClient;
use geyser_proto::subscribe_update::UpdateOneof;
use geyser_proto::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
};

fn build_request(account_pubkeys: &[String]) -> SubscribeRequest {
    let mut accounts = std::collections::HashMap::new();
    accounts.insert(
        "shadow_accounts".to_string(),
        SubscribeRequestFilterAccounts {
            account: account_pubkeys.to_vec(),
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: None,
        },
    );

    let mut slots = std::collections::HashMap::new();
    slots.insert(
        "shadow_slots".to_string(),
        SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: Some(false),
        },
    );

    let mut blocks_meta = std::collections::HashMap::new();
    blocks_meta.insert("shadow_blocks_meta".to_string(), SubscribeRequestFilterBlocksMeta {});

    SubscribeRequest {
        accounts,
        slots,
        transactions: Default::default(),
        transactions_status: Default::default(),
        blocks: Default::default(),
        blocks_meta,
        entry: Default::default(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    }
}

#[derive(Default, Debug, Clone)]
struct Counters {
    accounts: u64,
    slots: u64,
    blocks_meta: u64,
    pings: u64,
    pongs: u64,
    other: u64,
    errors: u64,
    last_slot: u64,
    first_account_after_swap_at: Option<Instant>,
}

async fn make_stream(
    endpoint_url: &str,
    initial: &[String],
) -> anyhow::Result<(mpsc::Sender<SubscribeRequest>, tonic::Streaming<geyser_proto::SubscribeUpdate>)> {
    let endpoint = Channel::from_shared(endpoint_url.to_string())?
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20))
        .connect_timeout(Duration::from_secs(10));
    let channel = endpoint.connect().await?;
    let mut client = GeyserClient::new(channel)
        // generous max frame size — large account-filter requests are big
        .max_encoding_message_size(64 * 1024 * 1024)
        .max_decoding_message_size(64 * 1024 * 1024);

    // Use a big buffer so try_send rarely fails; we still log when it does.
    let (req_tx, req_rx) = mpsc::channel::<SubscribeRequest>(2048);
    req_tx.send(build_request(initial)).await?;

    let response = client.subscribe(Request::new(ReceiverStream::new(req_rx))).await?;
    Ok((req_tx, response.into_inner()))
}

async fn run_test(endpoint_url: &str, wallets_path: &str) -> anyhow::Result<()> {
    println!("# Yellowstone shadow-test v2");
    println!("- Endpoint: `{endpoint_url}`");
    println!("- Wallets file: `{wallets_path}`");
    println!("- Time: {}", chrono::Utc::now().to_rfc3339());

    // Load real wallets.
    let bytes = std::fs::read(wallets_path)?;
    let wallets: Vec<String> = serde_json::from_slice(&bytes)?;
    println!("- Loaded {} real active wallet pubkeys", wallets.len());
    println!();
    let n = wallets.len();
    if n < 100 {
        anyhow::bail!("wallet list too small: {n}");
    }

    // Stream 1.
    let initial: Vec<String> = wallets[..50].to_vec();
    let (req_tx, mut response) = make_stream(endpoint_url, &initial).await?;

    let counters = Arc::new(Mutex::new(Counters::default()));
    let counters_for_reader = counters.clone();
    let reader = tokio::spawn(async move {
        while let Some(item) = response.next().await {
            let mut c = counters_for_reader.lock().await;
            match item {
                Ok(update) => match update.update_oneof {
                    Some(UpdateOneof::Account(_)) => {
                        c.accounts += 1;
                        if c.first_account_after_swap_at.is_none() {
                            c.first_account_after_swap_at = Some(Instant::now());
                        }
                    }
                    Some(UpdateOneof::Slot(s)) => {
                        c.slots += 1;
                        c.last_slot = s.slot;
                    }
                    Some(UpdateOneof::BlockMeta(_)) => c.blocks_meta += 1,
                    Some(UpdateOneof::Ping(_)) => c.pings += 1,
                    Some(UpdateOneof::Pong(_)) => c.pongs += 1,
                    _ => c.other += 1,
                },
                Err(e) => {
                    eprintln!("STREAM ERROR: {e}");
                    c.errors += 1;
                    break;
                }
            }
        }
    });

    // ── 1. baseline ─────────────────────────────────────────────────────
    println!("## 1. Baseline — 50 real wallets, 30s");
    let before = counters.lock().await.clone();
    tokio::time::sleep(Duration::from_secs(30)).await;
    let after = counters.lock().await.clone();
    print_delta("baseline_30s", &before, &after);

    // ── 2. grow test ────────────────────────────────────────────────────
    println!();
    println!("## 2. Grow test (real wallets)");
    let mut union: Vec<String> = initial.clone();
    let grow_targets: &[usize] = &[200, 1_000, 5_000, 10_000, 20_000, n.min(25_000)];
    for &target in grow_targets {
        let target = target.min(n);
        if target <= union.len() {
            continue;
        }
        union.extend_from_slice(&wallets[union.len()..target]);
        let bytes_estimate: usize = union.iter().map(|s| s.len() + 2).sum();
        println!();
        println!("### Grow → {} pubkeys (~{} KB request)", union.len(), bytes_estimate / 1024);
        let send_t = Instant::now();
        match req_tx.try_send(build_request(&union)) {
            Ok(()) => println!("- try_send OK"),
            Err(mpsc::error::TrySendError::Full(_)) => {
                println!("- ⚠ try_send returned Full; falling back to .send().await");
                req_tx.send(build_request(&union)).await?;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                println!("- ❌ try_send returned Closed; stream is dead");
                break;
            }
        }
        // reset first-seen to measure swap latency
        counters.lock().await.first_account_after_swap_at = None;
        let pre = counters.lock().await.clone();
        tokio::time::sleep(Duration::from_secs(15)).await;
        let post = counters.lock().await.clone();
        let first_seen = post
            .first_account_after_swap_at
            .map(|t| t.saturating_duration_since(send_t));
        println!("- send→serialized: {:?}", send_t.elapsed());
        println!("- first account update after swap: {:?}", first_seen);
        print_delta(&format!("after_grow_{}_15s", union.len()), &pre, &post);
        if post.errors > pre.errors {
            println!("- ❌ STREAM ERROR DURING THIS GROW — bailing");
            break;
        }
    }

    // ── 3. shrink test ──────────────────────────────────────────────────
    println!();
    println!("## 3. Shrink test (back to 50 pubkeys)");
    union.truncate(50);
    let send_t = Instant::now();
    req_tx.send(build_request(&union)).await?;
    let pre = counters.lock().await.clone();
    tokio::time::sleep(Duration::from_secs(15)).await;
    let post = counters.lock().await.clone();
    println!("- shrink request sent + 15s observed");
    print_delta("after_shrink_15s", &pre, &post);
    println!("- elapsed since send: {:?}", send_t.elapsed());

    // ── 4. churn test (60s, 5s cadence) ─────────────────────────────────
    println!();
    println!("## 4. Churn test — alternate 200 ↔ 5000 wallets every 5s for 60s");
    let churn_start = Instant::now();
    let mut iter_count: u32 = 0;
    let mut send_full: u32 = 0;
    while churn_start.elapsed() < Duration::from_secs(60) {
        let target: usize = if iter_count % 2 == 0 { 200 } else { 5_000 };
        let target = target.min(n);
        let mut u: Vec<String> = wallets[..target].to_vec();
        u.truncate(target);
        let pre = counters.lock().await.clone();
        match req_tx.try_send(build_request(&u)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                send_full += 1;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                eprintln!("Churn: channel closed at iter {iter_count}");
                break;
            }
        }
        println!(
            "- churn iter {iter_count} (target={}) — accounts so far: {}",
            target, pre.accounts
        );
        iter_count += 1;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    let final_c = counters.lock().await.clone();
    println!("- churn done: iterations={iter_count} send_full_count={send_full}");
    println!("- final counters: accounts={} slots={} blocks_meta={} errors={}",
        final_c.accounts, final_c.slots, final_c.blocks_meta, final_c.errors);

    // ── 5. per-IP concurrent-stream cap test ────────────────────────────
    println!();
    println!("## 5. Per-IP concurrent-stream cap test");
    println!("- attempting to open a 2nd concurrent Subscribe stream from the same process/IP");
    match make_stream(endpoint_url, &initial).await {
        Ok((tx2, mut stream2)) => {
            println!("- 2nd stream connected successfully");
            // Read 5s to see if it actually delivers
            let count2 = Arc::new(Mutex::new(0u64));
            let cc = count2.clone();
            let h = tokio::spawn(async move {
                let start = Instant::now();
                while start.elapsed() < Duration::from_secs(5) {
                    match tokio::time::timeout(Duration::from_secs(1), stream2.next()).await {
                        Ok(Some(Ok(_))) => *cc.lock().await += 1,
                        Ok(Some(Err(e))) => {
                            eprintln!("2nd stream error: {e}");
                            break;
                        }
                        Ok(None) => break,
                        Err(_) => {}
                    }
                }
            });
            let _ = tokio::time::timeout(Duration::from_secs(6), h).await;
            println!("- 2nd stream messages in 5s: {}", *count2.lock().await);
            drop(tx2);
        }
        Err(e) => {
            println!("- 2nd stream failed to connect: {e}");
        }
    }

    // ── 6. wrap-up ──────────────────────────────────────────────────────
    println!();
    println!("## 6. Wrap-up");
    drop(req_tx);
    let _ = tokio::time::timeout(Duration::from_secs(5), reader).await;
    let final_counters = counters.lock().await.clone();
    println!("- final cumulative: accounts={} slots={} blocks_meta={} pings={} errors={} last_slot={}",
        final_counters.accounts, final_counters.slots, final_counters.blocks_meta,
        final_counters.pings, final_counters.errors, final_counters.last_slot);
    Ok(())
}

fn print_delta(label: &str, before: &Counters, after: &Counters) {
    println!(
        "- {label}: Δaccounts={} Δslots={} Δblocks_meta={} Δpings={} Δpongs={} Δerrors={} last_slot={}",
        after.accounts - before.accounts,
        after.slots - before.slots,
        after.blocks_meta - before.blocks_meta,
        after.pings - before.pings,
        after.pongs - before.pongs,
        after.errors - before.errors,
        after.last_slot,
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let endpoint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://fra.corvus-labs.io:10101".to_string());
    let wallets = std::env::args().nth(2).unwrap_or_else(|| "/tmp/wallets.json".to_string());
    run_test(&endpoint, &wallets).await
}
