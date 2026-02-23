use super::grpc_yellowstone::geyser_proto::subscribe_update::UpdateOneof;
use super::grpc_yellowstone::geyser_proto::*;
use std::collections::{HashMap, HashSet};

/// Pre-compiled filters from a client's SubscribeRequest.
/// Account keys are stored as raw bytes for fast comparison against
/// the proto's Vec<u8> fields (avoiding repeated base58 encode/decode).
pub struct CompiledFilters {
    pub transactions: HashMap<String, CompiledTxFilter>,
    pub slots: HashMap<String, CompiledSlotFilter>,
    pub blocks_meta: Vec<String>,
    pub entry: Vec<String>,
    // Types that require direct upstream connection (not served by mux)
    pub has_accounts: bool,
    pub has_blocks: bool,
    pub has_transactions_status: bool,
}

pub struct CompiledTxFilter {
    pub vote: Option<bool>,
    pub failed: Option<bool>,
    pub signature: Option<Vec<u8>>,
    pub account_include: HashSet<Vec<u8>>,
    pub account_exclude: HashSet<Vec<u8>>,
    pub account_required: HashSet<Vec<u8>>,
}

pub struct CompiledSlotFilter {
    pub filter_by_commitment: Option<bool>,
}

fn decode_b58(s: &str) -> Option<Vec<u8>> {
    bs58::decode(s).into_vec().ok()
}

fn decode_b58_set(keys: &[String]) -> HashSet<Vec<u8>> {
    keys.iter().filter_map(|s| decode_b58(s)).collect()
}

impl CompiledFilters {
    pub fn from_request(req: &SubscribeRequest) -> Self {
        let transactions = req
            .transactions
            .iter()
            .map(|(name, f)| {
                (
                    name.clone(),
                    CompiledTxFilter {
                        vote: f.vote,
                        failed: f.failed,
                        signature: f.signature.as_ref().and_then(|s| decode_b58(s)),
                        account_include: decode_b58_set(&f.account_include),
                        account_exclude: decode_b58_set(&f.account_exclude),
                        account_required: decode_b58_set(&f.account_required),
                    },
                )
            })
            .collect();

        let slots = req
            .slots
            .iter()
            .map(|(name, f)| {
                (
                    name.clone(),
                    CompiledSlotFilter {
                        filter_by_commitment: f.filter_by_commitment,
                    },
                )
            })
            .collect();

        let blocks_meta = req.blocks_meta.keys().cloned().collect();
        let entry = req.entry.keys().cloned().collect();

        Self {
            transactions,
            slots,
            blocks_meta,
            entry,
            has_accounts: !req.accounts.is_empty(),
            has_blocks: !req.blocks.is_empty(),
            has_transactions_status: !req.transactions_status.is_empty(),
        }
    }

    /// Returns true if this subscription requires a direct upstream connection
    /// (contains filter types the mux doesn't serve).
    pub fn needs_direct_connection(&self) -> bool {
        self.has_accounts || self.has_blocks || self.has_transactions_status
    }

    /// Returns true if these filters subscribe to anything the mux can serve.
    pub fn has_mux_subscriptions(&self) -> bool {
        !self.transactions.is_empty()
            || !self.slots.is_empty()
            || !self.blocks_meta.is_empty()
            || !self.entry.is_empty()
    }

    /// Match a SubscribeUpdate against these filters.
    /// Returns the list of matching filter names, or None if no filter matches.
    pub fn match_update(&self, update: &SubscribeUpdate) -> Option<Vec<String>> {
        let oneof = update.update_oneof.as_ref()?;

        match oneof {
            UpdateOneof::Transaction(tx_update) => self.match_transaction(tx_update),
            UpdateOneof::Slot(_slot_update) => {
                if self.slots.is_empty() {
                    None
                } else {
                    Some(self.slots.keys().cloned().collect())
                }
            }
            UpdateOneof::BlockMeta(_) => {
                if self.blocks_meta.is_empty() {
                    None
                } else {
                    Some(self.blocks_meta.clone())
                }
            }
            UpdateOneof::Entry(_) => {
                if self.entry.is_empty() {
                    None
                } else {
                    Some(self.entry.clone())
                }
            }
            UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => Some(vec![]),
            // Account, Block, TransactionStatus are not served by mux
            _ => None,
        }
    }

    fn match_transaction(&self, tx: &SubscribeUpdateTransaction) -> Option<Vec<String>> {
        if self.transactions.is_empty() {
            return None;
        }

        let info = tx.transaction.as_ref()?;
        let account_keys = collect_tx_account_keys(info);

        let mut matched = Vec::new();
        for (name, filter) in &self.transactions {
            if matches_tx_filter(filter, info, &account_keys) {
                matched.push(name.clone());
            }
        }

        if matched.is_empty() {
            None
        } else {
            Some(matched)
        }
    }
}

/// Collect all account keys from a transaction (message keys + loaded addresses from meta).
fn collect_tx_account_keys(info: &SubscribeUpdateTransactionInfo) -> Vec<&[u8]> {
    let mut keys = Vec::new();

    if let Some(ref tx) = info.transaction {
        if let Some(ref msg) = tx.message {
            for k in &msg.account_keys {
                keys.push(k.as_slice());
            }
        }
    }

    if let Some(ref meta) = info.meta {
        for k in &meta.loaded_writable_addresses {
            keys.push(k.as_slice());
        }
        for k in &meta.loaded_readonly_addresses {
            keys.push(k.as_slice());
        }
    }

    keys
}

fn matches_tx_filter(
    filter: &CompiledTxFilter,
    info: &SubscribeUpdateTransactionInfo,
    account_keys: &[&[u8]],
) -> bool {
    // Vote filter
    if let Some(want_vote) = filter.vote {
        if want_vote != info.is_vote {
            return false;
        }
    }

    // Failed filter
    if let Some(want_failed) = filter.failed {
        let is_failed = info.meta.as_ref().map_or(false, |m| m.err.is_some());
        if want_failed != is_failed {
            return false;
        }
    }

    // Signature filter
    if let Some(ref want_sig) = filter.signature {
        if info.signature != *want_sig {
            return false;
        }
    }

    // Account include: at least one tx account must be in the filter set
    if !filter.account_include.is_empty() {
        let has_match = account_keys
            .iter()
            .any(|k| filter.account_include.contains(*k));
        if !has_match {
            return false;
        }
    }

    // Account exclude: no tx account may be in the filter set
    if !filter.account_exclude.is_empty() {
        let has_excluded = account_keys
            .iter()
            .any(|k| filter.account_exclude.contains(*k));
        if has_excluded {
            return false;
        }
    }

    // Account required: ALL filter accounts must appear in the tx
    if !filter.account_required.is_empty() {
        let all_present = filter
            .account_required
            .iter()
            .all(|req| account_keys.iter().any(|k| *k == req.as_slice()));
        if !all_present {
            return false;
        }
    }

    true
}
