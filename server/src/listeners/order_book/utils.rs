use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{
        Coin, Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;
use std::collections::VecDeque;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub(super) async fn process_rmp_file(dir: &Path) -> Result<PathBuf> {
    let output_path = dir.join("out.json");
    let payload = json!({
        "type": "fileSnapshot",
        "request": {
            "type": "l4Snapshots",
            "includeUsers": true,
            "includeTriggerOrders": false
        },
        "outPath": output_path,
        "includeHeightInOutput": true
    });

    let client = Client::new();
    client
        .post("http://localhost:3001/info")
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(output_path)
}

fn validate_side_consistency<O: InnerOrder + PartialEq + Debug>(
    coin: &Coin,
    side_label: &str,
    received_orders: &[O],
    expected_orders: &[O],
) -> Result<()> {
    if received_orders.len() != expected_orders.len() {
        return Err(format!(
            "Order count mismatch for {coin:?} {side_label}, expected: {} received: {}",
            expected_orders.len(),
            received_orders.len()
        )
        .into());
    }
    let mut expected_by_oid = HashMap::with_capacity(expected_orders.len());
    for order in expected_orders {
        if expected_by_oid.insert(order.oid(), order).is_some() {
            return Err(format!("Duplicate order oid in expected snapshot for {coin:?} {side_label}: {order:?}").into());
        }
    }
    for order in received_orders {
        match expected_by_oid.remove(&order.oid()) {
            None => {
                return Err(format!("Unexpected order in stored snapshot for {coin:?} {side_label}: {order:?}").into());
            }
            Some(expected_order) if expected_order != order => {
                return Err(format!(
                    "Order mismatch for {coin:?} {side_label}, expected: {expected_order:?} received: {order:?}"
                )
                .into());
            }
            Some(_) => {}
        }
    }
    if let Some(missing_order) = expected_by_oid.into_values().next() {
        return Err(format!("Missing order in stored snapshot for {coin:?} {side_label}: {missing_order:?}").into());
    }
    Ok(())
}

pub(super) fn validate_snapshot_consistency<O: InnerOrder + PartialEq + Debug>(
    snapshot: &Snapshots<O>,
    expected: &Snapshots<O>,
    ignore_spot: bool,
) -> Result<()> {
    let mut expected_map: HashMap<_, _> =
        expected.as_ref().iter().filter(|(coin, _)| !coin.is_spot() || !ignore_spot).collect();

    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let stored_book = book.as_ref();
        if let Some(expected_book) = expected_map.remove(coin) {
            for (i, (stored_orders, expected_orders)) in
                stored_book.as_ref().iter().zip(expected_book.as_ref()).enumerate()
            {
                let side_label = if i == 0 { "bids" } else { "asks" };
                validate_side_consistency(coin, side_label, stored_orders, expected_orders)?;
            }
        } else if !stored_book[0].is_empty() || !stored_book[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
    }
    if let Some((coin, _)) = expected_map.into_iter().next() {
        return Err(format!("Extra orderbook detected: {}", coin.value()).into());
    }
    Ok(())
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub(super) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| {
                let mut entries = Vec::new();
                let snapshot = order_book.to_l2_snapshot(None, None, None);
                entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
                let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
                    if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
                        let snapshot = last_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
                        entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
                    }
                };
                for n_sig_figs in (2..=5).rev() {
                    if n_sig_figs == 5 {
                        for mantissa in [None, Some(2), Some(5)] {
                            if mantissa == Some(5) {
                                // Some(2) is NOT a superset of this info!
                                add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                            } else {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                            }
                        }
                    } else {
                        add_new_snapshot(Some(n_sig_figs), None, 1);
                    }
                }
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

pub(super) struct BatchQueue<T> {
    deque: VecDeque<Batch<T>>,
    last_ts: Option<u64>,
}

impl<T> BatchQueue<T> {
    pub(super) const fn new() -> Self {
        Self { deque: VecDeque::new(), last_ts: None }
    }

    pub(super) fn push(&mut self, block: Batch<T>) -> bool {
        if let Some(last_ts) = self.last_ts {
            if last_ts >= block.block_number() {
                return false;
            }
        }
        self.last_ts = Some(block.block_number());
        self.deque.push_back(block);
        true
    }

    pub(super) fn pop_front(&mut self) -> Option<Batch<T>> {
        self.deque.pop_front()
    }

    pub(super) fn front(&self) -> Option<&Batch<T>> {
        self.deque.front()
    }
}
