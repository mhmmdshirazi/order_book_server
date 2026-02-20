use crate::{
    order_book::Side,
    prelude::*,
    types::{
        Fill, Trade,
        node_data::{Batch, NodeDataFill},
    },
};
use alloy::primitives::Address;
use chrono::{DateTime, NaiveDateTime};
use fs::File;
use log::{error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::HashMap,
    env,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{
    broadcast::Sender,
    mpsc::{UnboundedSender, unbounded_channel},
};
use tokio::time::{Duration, sleep};

pub(crate) async fn hl_listen(trade_tx: Sender<Arc<Trade>>, dir: PathBuf) -> Result<()> {
    let fills_dir = wait_for_trade_dir(&dir).await?;
    info!("Monitoring fills directory: {}", fills_dir.display());

    let mut listener = TradeRelayListener::new(trade_tx);
    let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
    let mut watcher = recommended_watcher(move |res| {
        send_fs_event(&fs_event_tx, res);
    })?;

    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    loop {
        match fs_event_rx.recv().await {
            Some(Ok(event)) => {
                if event.kind.is_create() || event.kind.is_modify() {
                    let new_path = &event.paths[0];
                    if new_path.starts_with(&fills_dir) && new_path.is_file() {
                        listener.process_update(&event, new_path)?;
                    }
                }
            }
            Some(Err(err)) => {
                error!("Watcher error: {err}");
                return Err(format!("Watcher error: {err}").into());
            }
            None => {
                error!("Channel closed. Listener exiting");
                return Err("Channel closed.".into());
            }
        }
    }
}

async fn wait_for_trade_dir(home_dir: &Path) -> Result<PathBuf> {
    let mut attempts = 0_u64;
    loop {
        if let Ok(custom_dir) = env::var("HL_TRADE_RELAY_DIR") {
            let custom_path = PathBuf::from(custom_dir);
            if custom_path.exists() {
                return Ok(custom_path.canonicalize()?);
            }
            if attempts == 0 || attempts % 30 == 0 {
                warn!("`HL_TRADE_RELAY_DIR` is set but does not exist yet: {}", custom_path.display());
            }
        }

        for candidate in candidate_trade_dirs(home_dir) {
            if candidate.exists() {
                return Ok(candidate.canonicalize()?);
            }
        }

        if attempts == 0 || attempts % 30 == 0 {
            warn!(
                "Waiting for trade output directory under {} (expected one of node_fills/node_fills_by_block/node_trades)",
                home_dir.join("hl/data").display()
            );
        }
        attempts += 1;
        sleep(Duration::from_secs(1)).await;
    }
}

fn candidate_trade_dirs(home_dir: &Path) -> Vec<PathBuf> {
    let data = home_dir.join("hl/data");
    vec![
        data.join("node_fills"),
        data.join("node_fills/hourly"),
        data.join("node_fills_by_block"),
        data.join("node_fills_by_block/hourly"),
        data.join("node_trades"),
        data.join("node_trades/hourly"),
    ]
}

fn send_fs_event(
    fs_event_tx: &UnboundedSender<std::result::Result<Event, notify::Error>>,
    res: std::result::Result<Event, notify::Error>,
) {
    if let Err(err) = fs_event_tx.send(res) {
        error!("Error sending fs event to processor via channel: {err}");
    }
}

struct TradeRelayListener {
    fill_file: Option<File>,
    pending_fills: HashMap<u64, HashMap<Side, NodeDataFill>>,
    partial_line: String,
    trade_tx: Sender<Arc<Trade>>,
}

impl TradeRelayListener {
    fn new(trade_tx: Sender<Arc<Trade>>) -> Self {
        Self { fill_file: None, pending_fills: HashMap::new(), partial_line: String::new(), trade_tx }
    }

    fn process_update(&mut self, event: &Event, new_path: &PathBuf) -> Result<()> {
        if event.kind.is_create() {
            self.on_file_creation(new_path.clone())?;
        } else if self.fill_file.is_some() {
            self.on_file_modification()?;
        } else {
            let mut new_file = File::open(new_path)?;
            new_file.seek(SeekFrom::End(0))?;
            self.fill_file = Some(new_file);
        }
        Ok(())
    }

    fn on_file_creation(&mut self, new_file: PathBuf) -> Result<()> {
        if let Some(file) = self.fill_file.as_mut() {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            if !buf.is_empty() {
                self.process_data(buf)?;
            }
            // Previous file is finalized now; if it ended without a newline, process it as a
            // complete final record instead of waiting for bytes from the next file.
            self.flush_partial_line_on_rotation();
        }
        self.fill_file = Some(File::open(new_file)?);
        Ok(())
    }

    fn on_file_modification(&mut self) -> Result<()> {
        let mut buf = String::new();
        let file = self.fill_file.as_mut().ok_or("No file being tracked")?;
        file.read_to_string(&mut buf)?;
        self.process_data(buf)?;
        Ok(())
    }

    fn process_data(&mut self, data: String) -> Result<()> {
        self.partial_line.push_str(&data);
        let chunk = std::mem::take(&mut self.partial_line);
        let has_trailing_newline = chunk.ends_with('\n');
        let mut lines = chunk.split('\n').collect::<Vec<_>>();
        if !has_trailing_newline {
            self.partial_line = lines.pop().unwrap_or_default().to_string();
        }

        for line in lines {
            let line = line.trim_end_matches('\r');
            if line.is_empty() {
                continue;
            }
            let events = match parse_trade_events(line) {
                Ok(events) => events,
                Err(err) => {
                    warn!("Skipping unparsable fills line: {err}, line: {:?}", &line[..100.min(line.len())]);
                    continue;
                }
            };
            self.process_events(events);
        }
        Ok(())
    }

    fn process_events(&mut self, events: Vec<TradeEvent>) {
        for event in events {
            match event {
                TradeEvent::Trade(trade) => {
                    let _unused = self.trade_tx.send(Arc::new(trade));
                }
                TradeEvent::Fill(fill) => self.process_fill(fill),
            }
        }
    }

    fn process_fill(&mut self, fill: NodeDataFill) {
        let tid = fill.1.tid;
        let by_side = self.pending_fills.entry(tid).or_default();
        by_side.insert(fill.1.side, fill);
        if by_side.len() == 2 {
            if let Some(fills) = self.pending_fills.remove(&tid) {
                let trade = Trade::from_fills(fills);
                let _unused = self.trade_tx.send(Arc::new(trade));
            }
        }
    }

    fn flush_partial_line_on_rotation(&mut self) {
        if self.partial_line.is_empty() {
            return;
        }
        let line = std::mem::take(&mut self.partial_line);
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            return;
        }
        match parse_trade_events(line) {
            Ok(events) => self.process_events(events),
            Err(err) => {
                warn!("Dropping incomplete or invalid trailing fills line during file rotation: {err}");
            }
        }
    }
}

enum TradeEvent {
    Fill(NodeDataFill),
    Trade(Trade),
}

#[derive(Debug, Deserialize)]
struct NodeDataFillObject {
    user: Address,
    #[serde(flatten)]
    fill: Fill,
}

#[derive(Debug, Deserialize)]
struct RawTradeSideInfo {
    user: Address,
}

#[derive(Debug, Deserialize)]
struct RawTrade {
    coin: String,
    side: Side,
    px: String,
    sz: String,
    hash: String,
    time: Value,
    tid: Option<u64>,
    side_info: Vec<RawTradeSideInfo>,
}

fn parse_trade_events(line: &str) -> Result<Vec<TradeEvent>> {
    if let Ok(batch) = serde_json::from_str::<Batch<NodeDataFill>>(line) {
        return Ok(batch.events().into_iter().map(TradeEvent::Fill).collect());
    }
    if let Ok(fill) = serde_json::from_str::<NodeDataFill>(line) {
        return Ok(vec![TradeEvent::Fill(fill)]);
    }
    if let Ok(fill) = serde_json::from_str::<NodeDataFillObject>(line) {
        return Ok(vec![TradeEvent::Fill(NodeDataFill(fill.user, fill.fill))]);
    }
    if let Ok(fills) = serde_json::from_str::<Vec<NodeDataFill>>(line) {
        return Ok(fills.into_iter().map(TradeEvent::Fill).collect());
    }
    if let Ok(raw_trade) = serde_json::from_str::<RawTrade>(line) {
        return Ok(vec![TradeEvent::Trade(raw_trade_to_trade(raw_trade)?)]);
    }
    if let Ok(raw_trades) = serde_json::from_str::<Vec<RawTrade>>(line) {
        return raw_trades.into_iter().map(raw_trade_to_trade).map(|trade| trade.map(TradeEvent::Trade)).collect();
    }

    let value: Value = serde_json::from_str(line)?;
    if let Some(events) = value.get("events") {
        if let Ok(fills) = serde_json::from_value::<Vec<NodeDataFill>>(events.clone()) {
            return Ok(fills.into_iter().map(TradeEvent::Fill).collect());
        }
        if let Ok(raw_trades) = serde_json::from_value::<Vec<RawTrade>>(events.clone()) {
            return raw_trades.into_iter().map(raw_trade_to_trade).map(|trade| trade.map(TradeEvent::Trade)).collect();
        }
    }
    Err("Unsupported fills format".into())
}

fn raw_trade_to_trade(raw_trade: RawTrade) -> Result<Trade> {
    let [left, right]: [RawTradeSideInfo; 2] =
        raw_trade.side_info.try_into().map_err(|_| "Raw trade must have exactly two side_info entries")?;
    let users = match raw_trade.side {
        // assume side_info[0] is taker and trade.side marks taker side.
        Side::Ask => [right.user, left.user],
        Side::Bid => [left.user, right.user],
    };
    Ok(Trade::from_parts(
        raw_trade.coin,
        raw_trade.side,
        raw_trade.px,
        raw_trade.sz,
        raw_trade.hash,
        parse_trade_time_to_millis(&raw_trade.time)?,
        raw_trade.tid.unwrap_or(0),
        users,
    ))
}

fn parse_trade_time_to_millis(value: &Value) -> Result<u64> {
    match value {
        Value::Number(num) => num.as_u64().ok_or("Invalid trade time number".into()),
        Value::String(s) => {
            if let Ok(ts) = s.parse::<u64>() {
                return Ok(ts);
            }
            if let Ok(ts) = DateTime::parse_from_rfc3339(s) {
                #[allow(clippy::unwrap_used)]
                return Ok(ts.timestamp_millis().try_into().unwrap());
            }
            if let Ok(ts) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
                #[allow(clippy::unwrap_used)]
                return Ok(ts.and_utc().timestamp_millis().try_into().unwrap());
            }
            Err(format!("Unsupported trade time string format: {s}").into())
        }
        _ => Err("Unsupported trade time type".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_unbatched_fill_event_object() -> Result<()> {
        let line = r#"{"user":"0x0000000000000000000000000000000000000001","coin":"BTC","px":"1","sz":"2","side":"A","time":1,"startPosition":"0","dir":"Open Long","closedPnl":"0","hash":"0x1","oid":1,"crossed":true,"fee":"0","tid":42,"feeToken":"USDC","liquidation":null}"#;
        let events = parse_trade_events(line)?;
        assert!(matches!(events.as_slice(), [TradeEvent::Fill(_)]));
        Ok(())
    }

    #[test]
    fn parse_raw_trade_line() -> Result<()> {
        let line = r#"{"coin":"BTC","side":"A","time":"2025-06-24T02:56:36.172847427","px":"2393.9","sz":"0.1539","hash":"0x2b21750229be769650b604261eaac1018c00c45812652efbbdd35fe0ecb201a1","side_info":[{"user":"0x0000000000000000000000000000000000000002"},{"user":"0x0000000000000000000000000000000000000003"}]}"#;
        let events = parse_trade_events(line)?;
        assert!(matches!(events.as_slice(), [TradeEvent::Trade(_)]));
        Ok(())
    }
}
