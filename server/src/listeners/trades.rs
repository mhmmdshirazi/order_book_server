use crate::{
    order_book::Side,
    prelude::*,
    types::{
        Trade,
        node_data::{Batch, EventSource, NodeDataFill},
    },
};
use fs::File;
use log::{error, info};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{
    broadcast::Sender,
    mpsc::{UnboundedSender, unbounded_channel},
};

pub(crate) async fn hl_listen(trade_tx: Sender<Arc<Trade>>, dir: PathBuf) -> Result<()> {
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
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
    trade_tx: Sender<Arc<Trade>>,
}

impl TradeRelayListener {
    fn new(trade_tx: Sender<Arc<Trade>>) -> Self {
        Self { fill_file: None, pending_fills: HashMap::new(), trade_tx }
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
        let total_len = data.len();
        for line in data.lines() {
            if line.is_empty() {
                continue;
            }
            let batch = match serde_json::from_str::<Batch<NodeDataFill>>(line) {
                Ok(batch) => batch,
                Err(err) => {
                    error!("Fills serialization error: {err}, line: {:?}", &line[..100.min(line.len())]);
                    #[allow(clippy::unwrap_used)]
                    let total_len: i64 = total_len.try_into().unwrap();
                    if let Some(file) = self.fill_file.as_mut() {
                        let _unused = file.seek_relative(-total_len);
                    }
                    break;
                }
            };
            self.process_batch(batch);
        }
        Ok(())
    }

    fn process_batch(&mut self, batch: Batch<NodeDataFill>) {
        for fill in batch.events() {
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
    }
}
