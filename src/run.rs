use crate::{
    checkpoint::{self, Checkpoint, CheckpointWriter},
    engine::Engine,
    telemetry::{self, Counters},
    wal::{Event, Record, WalReader, WalWriter},
};
use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::Notify,
    time::{sleep_until, timeout},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

pub const ENDPOINT: &str =
    "wss://data-stream.binance.vision/stream?streams=btcusdt@trade/btcusdt@bookTicker";

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
const CONNECT_RETRIES: u32 = 8;

fn reconnect_delay(retry: u32) -> Duration {
    Duration::from_millis((250 * (1_u64 << retry.min(5))).min(5000))
}

async fn connect(endpoint: &str, connection: u64) -> Result<Socket> {
    for attempt in 0..=CONNECT_RETRIES {
        let delay = if connection == 1 && attempt == 0 {
            Duration::ZERO
        } else {
            reconnect_delay(if connection == 1 {
                attempt - 1
            } else {
                attempt
            })
        };
        tracing::info!(
            connection,
            attempt = attempt + 1,
            delay_ms = delay.as_millis() as u64,
            "WebSocket connection attempt"
        );
        tokio::time::sleep(delay).await;
        let result = timeout(Duration::from_secs(15), connect_async(endpoint))
            .await
            .context("WebSocket connect timed out")
            .and_then(|result| result.map_err(Into::into));
        match result {
            Ok((socket, _)) => return Ok(socket),
            Err(error) => {
                tracing::warn!(connection, attempt = attempt + 1, error = %error, "WebSocket connection attempt failed");
                if attempt == CONNECT_RETRIES {
                    return Err(error.context("WebSocket connection retries exhausted"));
                }
            }
        }
    }
    unreachable!()
}

#[derive(Debug, Clone, Args, Serialize, Deserialize)]
pub struct Config {
    #[arg(long, default_value_t = 8192)]
    pub queue_capacity: usize,
    #[arg(long, default_value_t = 1024)]
    pub batch_size: usize,
    #[arg(long, default_value_t = 5)]
    pub batch_ms: u64,
    #[arg(long, default_value_t = 100)]
    pub enqueue_timeout_ms: u64,
    #[arg(long, default_value_t = 1000)]
    pub max_event_age_ms: u64,
    #[arg(long, default_value_t = 10_000)]
    pub idle_timeout_ms: u64,
}

impl Config {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.queue_capacity > 0 && self.batch_size > 0,
            "queue and batch sizes must be positive"
        );
        ensure!(
            self.batch_ms > 0
                && self.enqueue_timeout_ms > 0
                && self.max_event_age_ms > 0
                && self.idle_timeout_ms > 0,
            "timeouts must be positive"
        );
        Ok(())
    }

    fn send_timeout(&self) -> Duration {
        Duration::from_millis(self.enqueue_timeout_ms)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    pub records: u64,
    pub controls: u64,
    pub trades: u64,
    pub books: u64,
    pub duplicates: u64,
    pub invalid: u64,
    pub signals: u64,
    pub last_seq: u64,
    pub last_signal_seq: u64,
    pub digest: String,
    pub signal_digest: String,
    pub activity: Activity,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rate {
    pub messages: u64,
    pub trades: u64,
    pub books: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Activity {
    pub ready_ns: u64,
    pub duration_ns: u64,
    pub max_trade_gap_ns: u64,
    pub max_book_gap_ns: u64,
    pub seconds: BTreeMap<u64, Rate>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Stages {
    pub queue1_enqueue: Latency,
    pub queue1_residence: Latency,
    pub queue2_enqueue: Latency,
    pub queue2_residence: Latency,
    pub batch_wait: Latency,
    pub batch_size: Latency,
    pub flush: Latency,
    pub signal_output: Latency,
    pub queue1_unobserved: u64,
    pub queue2_unobserved: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Latency {
    count: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

fn histogram() -> Histogram<u64> {
    Histogram::new(3).expect("valid histogram precision")
}
fn micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}
fn latency(hist: &Histogram<u64>) -> Latency {
    Latency {
        count: hist.len(),
        p50_us: hist.value_at_quantile(0.50),
        p95_us: hist.value_at_quantile(0.95),
        p99_us: hist.value_at_quantile(0.99),
        max_us: hist.max(),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Summary {
    pub format_version: u32,
    pub mode: String,
    pub complete: bool,
    pub errors: Vec<String>,
    pub endpoint: Option<String>,
    pub config: Option<Config>,
    pub requested_seconds: Option<u64>,
    pub elapsed_ms: u64,
    pub received: u64,
    pub durable: u64,
    pub stats: Stats,
    pub wal_sha256: String,
    pub sync_calls: u64,
    pub persistence: Latency,
    pub sync: Latency,
    pub processing: Latency,
    pub event_age: Latency,
    pub receive_to_decision: Latency,
    pub stages: Stages,
    pub drain_ms: u64,
    pub backlog_at_input_end: u64,
    pub queue1_peak: u64,
    pub queue2_peak: u64,
    pub source_wal_sha256: Option<String>,
    pub replay_speed: Option<f64>,
}

pub fn prepare_output(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(path)
        .with_context(|| format!("output directory must be new: {}", path.display()))?;
    let log = File::create(path.join("system.jsonl"))?;
    tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_writer(Mutex::new(log))
        .try_init()
        .map_err(|error| anyhow::anyhow!("initialize logging: {error}"))?;
    Ok(())
}

pub fn save_summary(out: &Path, summary: &Summary) -> Result<()> {
    let mut file = File::create(out.join("summary.json"))?;
    serde_json::to_writer_pretty(&mut file, summary)?;
    file.write_all(b"\n")?;
    file.sync_data()?;
    println!("{}", serde_json::to_string(summary)?);
    Ok(())
}

pub fn file_hash(path: &Path) -> Result<String> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

async fn hash_reader(mut reader: impl tokio::io::AsyncRead + Unpin) -> Result<String> {
    use tokio::io::AsyncReadExt;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
        tokio::task::yield_now().await;
    }
    Ok(format!("{:x}", hash.finalize()))
}

async fn async_file_hash(path: &Path) -> Result<String> {
    ensure!(
        tokio::fs::metadata(path).await?.is_file(),
        "WAL must be a regular file"
    );
    hash_reader(tokio::fs::File::open(path).await?).await
}

struct Processor<W> {
    engine: Engine,
    output: W,
    stats: Stats,
    hash: Sha256,
    signal_hash: Sha256,
    state: &'static str,
    first_ns: Option<u64>,
    last_ns: u64,
    trade_ns: Option<u64>,
    book_ns: Option<u64>,
    signal_bytes: u64,
}

impl<W: Write> Processor<W> {
    fn new(output: W) -> Self {
        Self {
            engine: Engine::default(),
            output,
            stats: Stats::default(),
            hash: Sha256::new(),
            signal_hash: Sha256::new(),
            state: "DISCONNECTED",
            first_ns: None,
            last_ns: 0,
            trade_ns: None,
            book_ns: None,
            signal_bytes: 0,
        }
    }

    fn process(&mut self, record: &Record) -> Result<ProcessTiming> {
        ensure!(
            record.seq == self.stats.last_seq + 1,
            "compute sequence gap"
        );
        let engine_start = Instant::now();
        let outcome = self.engine.on_event(record);
        let decision = Instant::now();
        let mut signal_written = None;
        let first_ns = *self.first_ns.get_or_insert(record.offset_ns);
        if self.state == "READY" {
            self.stats.activity.ready_ns += record.offset_ns.saturating_sub(self.last_ns);
        }
        self.stats.activity.duration_ns = record.offset_ns.saturating_sub(first_ns);
        self.last_ns = record.offset_ns;
        let rate = self
            .stats
            .activity
            .seconds
            .entry((record.offset_ns - first_ns) / 1_000_000_000)
            .or_default();
        if matches!(record.event, Event::Message(_)) {
            rate.messages += 1;
        }
        if outcome.class == "trade" {
            rate.trades += 1;
            self.stats.activity.max_trade_gap_ns = self
                .stats
                .activity
                .max_trade_gap_ns
                .max(record.offset_ns - self.trade_ns.unwrap_or(first_ns));
            self.trade_ns = Some(record.offset_ns);
        }
        if outcome.class == "book" {
            rate.books += 1;
            self.stats.activity.max_book_gap_ns = self
                .stats
                .activity
                .max_book_gap_ns
                .max(record.offset_ns - self.book_ns.unwrap_or(first_ns));
            self.book_ns = Some(record.offset_ns);
        }
        if !matches!(record.event, Event::Message(_)) {
            self.stats.activity.max_trade_gap_ns = self
                .stats
                .activity
                .max_trade_gap_ns
                .max(record.offset_ns - self.trade_ns.unwrap_or(first_ns));
            self.stats.activity.max_book_gap_ns = self
                .stats
                .activity
                .max_book_gap_ns
                .max(record.offset_ns - self.book_ns.unwrap_or(first_ns));
        }
        let encoded = serde_json::to_vec(&outcome)?;
        if let Some(signal) = &outcome.signal {
            let mut bytes = serde_json::to_vec(signal)?;
            bytes.push(b'\n');
            self.output.write_all(&bytes)?;
            self.output.flush()?;
            signal_written = Some(Instant::now());
            self.signal_bytes += bytes.len() as u64;
            self.signal_hash.update(&bytes);
            self.stats.signals += 1;
            self.stats.last_signal_seq = record.seq;
        }
        self.hash.update((encoded.len() as u64).to_le_bytes());
        self.hash.update(&encoded);
        match outcome.class {
            "control" => self.stats.controls += 1,
            "trade" => self.stats.trades += 1,
            "book" => self.stats.books += 1,
            "duplicate" => self.stats.duplicates += 1,
            "invalid" => self.stats.invalid += 1,
            _ => unreachable!("engine class"),
        }
        if outcome.state != self.state {
            tracing::info!(
                seq = record.seq,
                state = outcome.state,
                "engine state changed"
            );
            self.state = outcome.state;
        }
        self.stats.records += 1;
        self.stats.last_seq = record.seq;
        Ok(ProcessTiming {
            engine: decision.duration_since(engine_start),
            decision,
            signal_written,
        })
    }
    fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            format_version: 1,
            last_seq: self.stats.last_seq,
            records: self.stats.records,
            signals: self.stats.signals,
            last_signal_seq: self.stats.last_signal_seq,
            signal_bytes: self.signal_bytes,
            digest: format!("{:x}", self.hash.clone().finalize()),
            signal_digest: format!("{:x}", self.signal_hash.clone().finalize()),
        }
    }
    fn finish(mut self) -> Stats {
        self.stats.digest = format!("{:x}", self.hash.finalize());
        self.stats.signal_digest = format!("{:x}", self.signal_hash.finalize());
        self.stats
    }
}

struct ProcessTiming {
    engine: Duration,
    decision: Instant,
    signal_written: Option<Instant>,
}
struct TimedRecord {
    record: Record,
    received: Instant,
    queued_at: Arc<OnceLock<Instant>>,
    taken_at: Instant,
}
impl TimedRecord {
    fn new(record: Record, received: Instant) -> Self {
        Self {
            record,
            received,
            queued_at: Arc::new(OnceLock::new()),
            taken_at: received,
        }
    }
    fn residence(&self, now: Instant) -> Option<Duration> {
        self.queued_at
            .get()
            .and_then(|time| now.checked_duration_since(*time))
    }
}
struct Worker<T> {
    stats: T,
    error: Option<String>,
}
struct WalStats {
    durable: u64,
    sync_calls: u64,
    persistence: Histogram<u64>,
    sync: Histogram<u64>,
    flush: Histogram<u64>,
    queue: Histogram<u64>,
    enqueue: Histogram<u64>,
    batch_wait: Histogram<u64>,
    batch_size: Histogram<u64>,
    queue_unobserved: u64,
}
struct ComputeStats {
    stats: Stats,
    processing: Histogram<u64>,
    age: Histogram<u64>,
    decision: Histogram<u64>,
    queue: Histogram<u64>,
    signals: Histogram<u64>,
    age_violations: u64,
    queue_unobserved: u64,
}
fn persist(
    wal: WalWriter,
    rx: flume::Receiver<TimedRecord>,
    tx: flume::Sender<TimedRecord>,
    config: &Config,
    counters: &Counters,
) -> Worker<WalStats> {
    persist_with_sync(wal, rx, tx, config, counters, |wal| {
        wal.0.get_ref().sync_data().context("WAL sync_data failed")
    })
}
fn persist_with_sync(
    mut wal: WalWriter,
    rx: flume::Receiver<TimedRecord>,
    tx: flume::Sender<TimedRecord>,
    config: &Config,
    counters: &Counters,
    mut sync: impl FnMut(&mut WalWriter) -> Result<()>,
) -> Worker<WalStats> {
    let mut stats = WalStats {
        durable: 0,
        sync_calls: 0,
        persistence: histogram(),
        sync: histogram(),
        flush: histogram(),
        queue: histogram(),
        enqueue: histogram(),
        batch_wait: histogram(),
        batch_size: histogram(),
        queue_unobserved: 0,
    };
    let mut batch = Vec::with_capacity(config.batch_size);
    let result = (|| -> Result<()> {
        while let Ok(mut first) = rx.recv() {
            let now = Instant::now();
            if let Some(wait) = first.residence(now) {
                stats.queue.record(micros(wait))?;
            } else {
                stats.queue_unobserved += 1;
            }
            first.taken_at = now;
            let deadline = now + Duration::from_millis(config.batch_ms);
            batch.push(first);
            while batch.len() < config.batch_size {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match rx.recv_timeout(remaining) {
                    Ok(mut event) => {
                        let now = Instant::now();
                        if let Some(wait) = event.residence(now) {
                            stats.queue.record(micros(wait))?;
                        } else {
                            stats.queue_unobserved += 1;
                        }
                        event.taken_at = now;
                        batch.push(event);
                    }
                    Err(
                        flume::RecvTimeoutError::Timeout | flume::RecvTimeoutError::Disconnected,
                    ) => break,
                }
            }
            for event in &batch {
                wal.append(&event.record)?;
            }
            let flush_start = Instant::now();
            wal.0.flush()?;
            let flush_end = Instant::now();
            let sync_start = Instant::now();
            sync(&mut wal)?;
            let synced = Instant::now();
            stats
                .flush
                .record(micros(flush_end.duration_since(flush_start)))?;
            for event in &batch {
                stats
                    .batch_wait
                    .record(micros(sync_start.duration_since(event.taken_at)))?;
            }
            stats
                .sync
                .record(micros(synced.duration_since(sync_start)))?;
            stats.sync_calls += 1;
            stats.batch_size.record(batch.len() as u64)?;
            stats.durable += batch.len() as u64;
            counters.durable.store(stats.durable, Ordering::Relaxed);
            for mut event in batch.drain(..) {
                stats
                    .persistence
                    .record(micros(synced.duration_since(event.received)))?;
                let stamp = Arc::new(OnceLock::new());
                event.queued_at = stamp.clone();
                let start = Instant::now();
                counters.queue2_peak.fetch_max(
                    (tx.len() + 1).min(config.queue_capacity) as u64,
                    Ordering::Relaxed,
                );
                tx.send_timeout(event, config.send_timeout())
                    .map_err(|_| anyhow::anyhow!("queue 2 closed or enqueue timed out"))?;
                let sent = Instant::now();
                let _ = stamp.set(sent);
                stats.enqueue.record(micros(sent.duration_since(start)))?;
            }
        }
        Ok(())
    })();
    Worker {
        stats,
        error: result.err().map(|error| format!("{error:#}")),
    }
}
fn compute(
    rx: flume::Receiver<TimedRecord>,
    signals: File,
    checkpoints: Option<CheckpointWriter>,
    config: &Config,
    counters: &Counters,
) -> Worker<ComputeStats> {
    let mut processor = Processor::new(BufWriter::new(signals));
    let mut checkpoints = checkpoints;
    let mut processing = histogram();
    let mut age = histogram();
    let mut decision = histogram();
    let mut queue = histogram();
    let mut signal_latency = histogram();
    let mut age_violations = 0;
    let mut queue_unobserved = 0;
    let mut next_checkpoint = Instant::now() + Duration::from_secs(1);
    let mut checkpoint = |processor: &mut Processor<BufWriter<File>>| -> Result<()> {
        processor.output.flush()?;
        processor.output.get_ref().sync_data()?;
        if let Some(writer) = &mut checkpoints {
            writer.append(&processor.checkpoint())?;
        }
        Ok(())
    };
    let result = (|| -> Result<()> {
        loop {
            match rx.recv_timeout(next_checkpoint.saturating_duration_since(Instant::now())) {
                Ok(event) => {
                    let start = Instant::now();
                    if let Some(wait) = event.residence(start) {
                        queue.record(micros(wait))?;
                    } else {
                        queue_unobserved += 1;
                    }
                    let event_age = start.duration_since(event.received);
                    age.record(micros(event_age))?;
                    if event_age > Duration::from_millis(config.max_event_age_ms) {
                        age_violations += 1;
                    }
                    let timing = processor.process(&event.record)?;
                    processing.record(micros(timing.engine))?;
                    decision.record(micros(timing.decision.duration_since(event.received)))?;
                    if let Some(end) = timing.signal_written {
                        signal_latency.record(micros(end.duration_since(event.received)))?;
                    }
                    counters
                        .processed
                        .store(processor.stats.records, Ordering::Relaxed);
                }
                Err(flume::RecvTimeoutError::Disconnected) => break,
                Err(flume::RecvTimeoutError::Timeout) => {}
            }
            if Instant::now() >= next_checkpoint {
                checkpoint(&mut processor)?;
                next_checkpoint = Instant::now() + Duration::from_secs(1);
            }
        }
        checkpoint(&mut processor)?;
        Ok(())
    })();
    Worker {
        stats: ComputeStats {
            stats: processor.finish(),
            processing,
            age,
            decision,
            queue,
            signals: signal_latency,
            age_violations,
            queue_unobserved,
        },
        error: result.err().map(|error| format!("{error:#}")),
    }
}

struct Recorder<'a> {
    tx: &'a flume::Sender<TimedRecord>,
    received: &'a AtomicU64,
    config: &'a Config,
    counters: &'a Counters,
    origin: Instant,
    last_seq: u64,
    connection: u64,
    connected: bool,
}

impl Recorder<'_> {
    async fn push(&mut self, event: Event) -> Result<()> {
        let now = Instant::now();
        self.received.fetch_add(1, Ordering::Relaxed);
        let seq = self.last_seq + 1;
        let connection = self.connection + u64::from(matches!(event, Event::Connected));
        let connected = match event {
            Event::Connected => true,
            Event::Disconnected(_) | Event::Finished(_) => false,
            Event::Message(_) => self.connected,
        };
        let record = Record {
            seq,
            connection,
            utc_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_nanos()
                .try_into()?,
            offset_ns: now.duration_since(self.origin).as_nanos().try_into()?,
            event,
        };
        let event = TimedRecord::new(record, now);
        let stamp = event.queued_at.clone();
        self.counters.queue1_peak.fetch_max(
            (self.tx.len() + 1).min(self.config.queue_capacity) as u64,
            Ordering::Relaxed,
        );
        timeout(self.config.send_timeout(), self.tx.send_async(event))
            .await
            .context("queue 1 enqueue timed out")?
            .map_err(|_| anyhow::anyhow!("queue 1 closed"))?;
        let sent = Instant::now();
        let _ = stamp.set(sent);
        self.counters
            .queue1_enqueue
            .lock()
            .expect("queue metrics")
            .record(micros(sent.duration_since(now)))?;
        self.last_seq = seq;
        self.connection = connection;
        self.connected = connected;
        Ok(())
    }
}

// Socket errors reconnect; persistence/queue errors stop the entire pipeline.
async fn receive_connection(socket: &mut Socket, recorder: &mut Recorder<'_>) -> Result<String> {
    let idle_timeout = Duration::from_millis(recorder.config.idle_timeout_ms);
    let mut activity_deadline = tokio::time::Instant::now() + idle_timeout;
    loop {
        let message = match tokio::time::timeout_at(activity_deadline, socket.next()).await {
            Ok(message) => message,
            Err(_) => return Ok("market-data idle timeout".into()),
        };
        match message {
            Some(Ok(Message::Text(text))) => {
                activity_deadline = tokio::time::Instant::now() + idle_timeout;
                recorder.push(Event::Message(text.to_string())).await?
            }
            Some(Ok(Message::Ping(payload))) => {
                match timeout(
                    recorder.config.send_timeout(),
                    socket.send(Message::Pong(payload)),
                )
                .await
                {
                    Ok(Ok(())) => {
                        tracing::info!(connection = recorder.connection, "WebSocket Ping answered")
                    }
                    Ok(Err(error)) => return Ok(format!("WebSocket Pong failed: {error}")),
                    Err(_) => return Ok("WebSocket Pong send timed out".into()),
                }
            }
            Some(Ok(Message::Pong(_))) => {}
            Some(Ok(Message::Close(_))) | None => {
                return Ok("WebSocket disconnected before sampling deadline".into());
            }
            Some(Ok(other)) => bail!("unexpected WebSocket frame: {}", other.len()),
            Some(Err(error)) => return Ok(format!("WebSocket disconnected: {error}")),
        }
    }
}

async fn receive(
    endpoint: &str,
    seconds: u64,
    tx: &flume::Sender<TimedRecord>,
    received: &AtomicU64,
    failure: &Notify,
    config: &Config,
    counters: &Counters,
) -> Vec<String> {
    let mut recorder = Recorder {
        tx,
        received,
        config,
        counters,
        origin: Instant::now(),
        last_seq: 0,
        connection: 0,
        connected: false,
    };
    let mut socket = None;
    let mut errors = vec![];
    let result: Result<()> = tokio::select! {
        interrupted = crate::shutdown::cancelled() => interrupted.context("install shutdown handler").and_then(|()| bail!("sampling interrupted before deadline")),
        _ = failure.notified() => Err(anyhow::anyhow!("pipeline worker failed")),
        result = async {
            socket = Some(connect(endpoint, 1).await?);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
            recorder.push(Event::Connected).await?;
            tracing::info!(connection = recorder.connection, endpoint, seconds, "WebSocket connected");
            tokio::select! {
                _ = sleep_until(deadline) => Ok(()),
                result = async {
                    loop {
                        let reason = receive_connection(socket.as_mut().unwrap(), &mut recorder).await?;
                        errors.push(format!("connection {} disconnected: {reason}", recorder.connection));
                        tracing::warn!(connection = recorder.connection, %reason, "market-data gap started");
                        recorder.push(Event::Disconnected(reason)).await?;
                        socket = None;
                        let connection = recorder.connection + 1;
                        let next = connect(endpoint, connection).await?;
                        socket = Some(next);
                        recorder.push(Event::Connected).await?;
                        tracing::info!(connection, "WebSocket reconnected; warming up after data gap");
                    }
                } => result,
            }
        } => result,
    };
    let terminal = match result {
        Ok(()) if received.load(Ordering::Relaxed) == recorder.last_seq => {
            Some(Event::Finished("duration_elapsed".into()))
        }
        Ok(()) => {
            // A cancelled send may already have reached the receiver. Do not
            // append controls after an unconfirmed send or claim a complete run.
            errors.push("input enqueue was not confirmed before sampling deadline".into());
            None
        }
        Err(error) => {
            tracing::error!(error = %error, "network sampling failed");
            errors.push(format!("{error:#}"));
            if recorder.connected && received.load(Ordering::Relaxed) == recorder.last_seq {
                Some(Event::Disconnected(error.to_string()))
            } else {
                None
            }
        }
    };
    if let Some(event) = terminal
        && let Err(error) = recorder.push(event).await
    {
        tracing::error!(error = %error, "failed to enqueue terminal control event");
        errors.push(format!("terminal control event: {error:#}"));
    }
    if let Some(mut socket) = socket {
        let _ = timeout(Duration::from_secs(1), socket.close(None)).await;
    }
    errors
}

struct Pipeline {
    tx: flume::Sender<TimedRecord>,
    writer: Option<thread::JoinHandle<Worker<WalStats>>>,
    consumer: thread::JoinHandle<Worker<ComputeStats>>,
    received: Arc<AtomicU64>,
    counters: Arc<Counters>,
    failure: Arc<Notify>,
    monitor: tokio::task::JoinHandle<Result<()>>,
    stop: tokio::sync::watch::Sender<bool>,
}
impl Pipeline {
    fn start(out: &Path, config: &Config, persist_wal: bool) -> Result<Self> {
        config.validate()?;
        let signals = File::options()
            .write(true)
            .create_new(true)
            .open(out.join("signals.jsonl"))?;
        let checkpoints = CheckpointWriter::create(&out.join("checkpoints.jsonl"))?;
        let (tx, rx) = flume::bounded(config.queue_capacity);
        let (durable_tx, durable_rx) = flume::bounded(config.queue_capacity);
        let counters = Arc::new(Counters::default());
        let failure = Arc::new(Notify::new());
        let received = Arc::new(AtomicU64::new(0));
        let wal = if persist_wal {
            Some(WalWriter::create(&out.join("raw.wal"))?)
        } else {
            None
        };
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let input = tx.downgrade();
        let output = persist_wal.then(|| durable_tx.downgrade());
        let path = out.to_path_buf();
        let monitor_received = received.clone();
        let monitor_counters = counters.clone();
        let monitor = tokio::spawn(async move {
            telemetry::monitor(
                &path,
                input,
                output,
                monitor_received,
                monitor_counters,
                stopped,
            )
            .await
        });
        let (writer, compute_rx) = if persist_wal {
            let wal = wal.unwrap();
            let config = config.clone();
            let counters = counters.clone();
            let failure = failure.clone();
            let worker = thread::Builder::new().name("wal".into()).spawn(move || {
                let result = persist(wal, rx, durable_tx, &config, &counters);
                if result.error.is_some() {
                    failure.notify_one();
                }
                result
            })?;
            (Some(worker), durable_rx)
        } else {
            drop(durable_tx);
            (None, rx)
        };
        let config = config.clone();
        let compute_counters = counters.clone();
        let compute_failure = failure.clone();
        let consumer = thread::Builder::new()
            .name("engine".into())
            .spawn(move || {
                let result = compute(
                    compute_rx,
                    signals,
                    Some(checkpoints),
                    &config,
                    &compute_counters,
                );
                if result.error.is_some() {
                    compute_failure.notify_one();
                }
                result
            })?;
        Ok(Self {
            tx,
            writer,
            consumer,
            received,
            counters,
            failure,
            monitor,
            stop,
        })
    }

    async fn finish(self, config: Config, mut errors: Vec<String>) -> Result<Summary> {
        let received = self.received.load(Ordering::Relaxed);
        let processed = self.counters.processed.load(Ordering::Relaxed);
        let drain = Instant::now();
        drop(self.tx);
        let writer = self.writer;
        let consumer = self.consumer;
        let (wal, compute) = tokio::task::spawn_blocking(move || -> Result<_> {
            let writer = writer
                .map(|worker| {
                    worker
                        .join()
                        .map_err(|_| anyhow::anyhow!("WAL thread panicked"))
                })
                .transpose()?;
            let consumer = consumer
                .join()
                .map_err(|_| anyhow::anyhow!("compute thread panicked"))?;
            Ok((writer, consumer))
        })
        .await??;
        let drain_ms = drain.elapsed().as_millis() as u64;
        let _ = self.stop.send(true);
        if let Err(error) = self.monitor.await? {
            errors.push(format!("resource monitoring: {error:#}"));
        }
        let empty_wal = || WalStats {
            durable: 0,
            sync_calls: 0,
            persistence: histogram(),
            sync: histogram(),
            flush: histogram(),
            queue: histogram(),
            enqueue: histogram(),
            batch_wait: histogram(),
            batch_size: histogram(),
            queue_unobserved: 0,
        };
        let persisted = wal.is_some();
        let wal = match wal {
            Some(worker) => {
                errors.extend(worker.error);
                worker.stats
            }
            None => empty_wal(),
        };
        errors.extend(compute.error);
        let compute = compute.stats;
        if received != compute.stats.records || (persisted && received != wal.durable) {
            errors.push("received/durable/processed counts differ".into());
        }
        if compute.stats.invalid > 0 {
            errors.push("invalid market events observed".into());
        }
        if compute.stats.trades == 0 || compute.stats.books == 0 {
            errors.push("both trade and book streams must contain events".into());
        }
        if compute.age_violations > 0 {
            errors.push(format!(
                "{} events exceeded configured age limit",
                compute.age_violations
            ));
        }
        Ok(Summary {
            format_version: 2,
            mode: String::new(),
            complete: errors.is_empty(),
            errors,
            endpoint: None,
            config: Some(config),
            requested_seconds: None,
            elapsed_ms: 0,
            received,
            durable: wal.durable,
            stats: compute.stats,
            wal_sha256: String::new(),
            sync_calls: wal.sync_calls,
            persistence: latency(&wal.persistence),
            sync: latency(&wal.sync),
            processing: latency(&compute.processing),
            event_age: latency(&compute.age),
            receive_to_decision: latency(&compute.decision),
            stages: Stages {
                queue1_enqueue: latency(
                    &self.counters.queue1_enqueue.lock().expect("queue metrics"),
                ),
                queue1_residence: latency(if persisted {
                    &wal.queue
                } else {
                    &compute.queue
                }),
                queue2_enqueue: latency(&wal.enqueue),
                queue2_residence: if persisted {
                    latency(&compute.queue)
                } else {
                    latency(&histogram())
                },
                batch_wait: latency(&wal.batch_wait),
                batch_size: latency(&wal.batch_size),
                flush: latency(&wal.flush),
                signal_output: latency(&compute.signals),
                queue1_unobserved: if persisted {
                    wal.queue_unobserved
                } else {
                    compute.queue_unobserved
                },
                queue2_unobserved: if persisted {
                    compute.queue_unobserved
                } else {
                    0
                },
            },
            drain_ms,
            backlog_at_input_end: received.saturating_sub(processed),
            queue1_peak: self.counters.queue1_peak.load(Ordering::Relaxed),
            queue2_peak: self.counters.queue2_peak.load(Ordering::Relaxed),
            source_wal_sha256: None,
            replay_speed: None,
        })
    }
}

pub async fn live(out: &Path, endpoint: &str, seconds: u64, config: Config) -> Result<Summary> {
    ensure!(seconds > 0, "seconds must be positive");
    let start = Instant::now();
    let pipeline = Pipeline::start(out, &config, true)?;
    let errors = receive(
        endpoint,
        seconds,
        &pipeline.tx,
        &pipeline.received,
        &pipeline.failure,
        &config,
        &pipeline.counters,
    )
    .await;
    let mut summary = pipeline.finish(config, errors).await?;
    summary.wal_sha256 = file_hash(&out.join("raw.wal"))?;
    let config = summary.config.as_ref().unwrap();
    let max_gap = summary
        .stats
        .activity
        .max_trade_gap_ns
        .max(summary.stats.activity.max_book_gap_ns);
    if max_gap > config.idle_timeout_ms.saturating_mul(1_000_000) {
        summary
            .errors
            .push("a market stream exceeded configured activity limit".into());
    }
    summary.complete = summary.errors.is_empty();
    summary.mode = "live".into();
    summary.endpoint = Some(endpoint.into());
    summary.requested_seconds = Some(seconds);
    summary.elapsed_ms = start.elapsed().as_millis() as u64;
    tracing::info!(received = summary.received, durable = summary.durable, processed = summary.stats.records, errors = ?summary.errors, "sampling finished");
    Ok(summary)
}

pub async fn replay(
    input: &Path,
    out: &Path,
    speed: f64,
    recover: bool,
    persist: bool,
    config: Config,
    mut shutdown: crate::shutdown::Shutdown,
) -> Result<Summary> {
    ensure!(
        speed.is_finite() && speed >= 0.0,
        "speed must be finite and nonnegative; 0 means unlimited"
    );
    let start = Instant::now();
    let pipeline = Pipeline::start(out, &config, persist)?;
    let mut source_hash = None;
    let mut finished = false;
    let mut validated_bytes = 0;
    let mut errors = vec![];
    let mut interrupted = false;
    let result: Result<()> = tokio::select! {
        biased;
        stopped = shutdown.cancelled() => {
            interrupted = true;
            stopped.context("wait for shutdown").and_then(|()| bail!("replay interrupted"))
        },
        _ = pipeline.failure.notified() => Err(anyhow::anyhow!("pipeline worker failed")),
        result = async {
            tracing::info!(phase = "initial", input = %input.display(), "source WAL hashing");
            source_hash = Some(async_file_hash(input).await?);
            let mut reader = WalReader::open(input)?;
            validated_bytes = reader.offset;
            let injection_origin = Instant::now();
            let mut origin_ns = None;
            loop {
                let record = match reader.read_record() {
                    Ok(Some(record)) => record,
                    Ok(None) => break,
                    Err(error) => { errors.push(format!("{error:#}")); break; },
                };
                validated_bytes = reader.offset;
                if let Event::Disconnected(reason) = &record.event { errors.push(format!("connection {} disconnected: {reason}", record.connection)); }
                if let Event::Finished(reason) = &record.event && reason != "duration_elapsed" { errors.push(format!("sampling ended early: {reason}")); }
                let origin = *origin_ns.get_or_insert(record.offset_ns);
                if speed > 0.0 {
                    let delta = Duration::try_from_secs_f64((record.offset_ns - origin) as f64 / 1e9 / speed).context("replay delay out of range")?;
                    let deadline = injection_origin.checked_add(delta).context("replay deadline out of range")?;
                    sleep_until(deadline.into()).await;
                } else if record.seq % 128 == 0 { tokio::task::yield_now().await; }
                let is_finished = matches!(record.event, Event::Finished(_));
                let event = TimedRecord::new(record, Instant::now());
                let stamp = event.queued_at.clone();
                let received = event.received;
                pipeline.received.fetch_add(1, Ordering::Relaxed);
                pipeline.counters.queue1_peak.fetch_max((pipeline.tx.len() + 1).min(config.queue_capacity) as u64, Ordering::Relaxed);
                timeout(config.send_timeout(), pipeline.tx.send_async(event)).await.context("queue 1 enqueue timed out")?.map_err(|_| anyhow::anyhow!("queue 1 closed"))?;
                let sent = Instant::now();
                let _ = stamp.set(sent);
                pipeline.counters.queue1_enqueue.lock().expect("queue metrics").record(micros(sent.duration_since(received)))?;
                finished = is_finished;
            }
            Ok(())
        } => result,
    };
    if let Err(error) = result {
        errors.push(format!("{error:#}"));
    }
    if !finished {
        errors.push("WAL has no final Finished event".into());
    }
    if recover {
        errors.push(format!(
            "recovery-only result: {validated_bytes} validated bytes"
        ));
    }
    let mut summary = pipeline.finish(config, errors).await?;
    if !interrupted {
        let result: Result<()> = tokio::select! {
            biased;
            stopped = shutdown.cancelled() => stopped.context("wait for shutdown").and_then(|()| bail!("replay interrupted during final hash verification")),
            result = async {
                if let Some(expected) = &source_hash {
                    tracing::info!(phase = "final", input = %input.display(), "source WAL hashing");
                    ensure!(async_file_hash(input).await? == *expected, "source WAL changed during replay");
                }
                if persist {
                    summary.wal_sha256 = async_file_hash(&out.join("raw.wal")).await?;
                    if summary.complete {
                        ensure!(Some(&summary.wal_sha256) == source_hash.as_ref(), "persisted WAL differs from source");
                    }
                } else if let Some(hash) = &source_hash {
                    summary.wal_sha256 = hash.clone();
                }
                Ok(())
            } => result,
        };
        if let Err(error) = result {
            summary.errors.push(format!("{error:#}"));
            summary.complete = false;
        }
    }
    summary.mode = if persist { "replay_persist" } else { "replay" }.into();
    summary.source_wal_sha256 = source_hash;
    summary.replay_speed = Some(speed);
    summary.elapsed_ms = start.elapsed().as_millis() as u64;
    Ok(summary)
}

pub fn verify(input: &Path) -> Result<Stats> {
    let summary: Summary = serde_json::from_reader(File::open(input.join("summary.json"))?)?;
    ensure!(
        summary.format_version == 2
            && summary.mode == "live"
            && summary.complete
            && summary.errors.is_empty(),
        "source is not a successful live sample"
    );
    let wal = input.join("raw.wal");
    ensure!(
        file_hash(&wal)? == summary.wal_sha256,
        "WAL file hash differs from manifest"
    );
    let mut reader = WalReader::open(&wal)?;
    let mut processor = Processor::new(std::io::sink());
    let mut finished = false;
    while let Some(record) = reader.read_record()? {
        ensure!(
            !matches!(record.event, Event::Disconnected(_)),
            "sample contains a connection gap"
        );
        if let Event::Finished(reason) = &record.event {
            ensure!(reason == "duration_elapsed", "sample ended before deadline");
        }
        finished = matches!(record.event, Event::Finished(_));
        processor.process(&record)?;
    }
    let final_checkpoint = processor.checkpoint();
    let stats = processor.finish();
    ensure!(
        finished && stats.trades > 0 && stats.books > 0 && stats.invalid == 0,
        "sample has missing or invalid events"
    );
    ensure!(
        summary.received == summary.durable && summary.durable == stats.records,
        "sample count mismatch"
    );
    ensure!(
        stats.records == stats.last_seq
            && stats.records
                == stats.controls + stats.trades + stats.books + stats.duplicates + stats.invalid,
        "event accounting mismatch"
    );
    ensure!(
        stats == summary.stats,
        "live/replay business results differ"
    );
    ensure!(
        file_hash(&input.join("signals.jsonl"))? == stats.signal_digest,
        "signal file differs from replay"
    );
    let stored_checkpoint = checkpoint::latest_verified(
        &input.join("checkpoints.jsonl"),
        &input.join("signals.jsonl"),
    )?;
    ensure!(
        stored_checkpoint.as_ref() == Some(&final_checkpoint),
        "final checkpoint missing or differs from replay"
    );
    let config = summary
        .config
        .as_ref()
        .context("missing live configuration")?;
    ensure!(
        stats
            .activity
            .max_trade_gap_ns
            .max(stats.activity.max_book_gap_ns)
            <= config.idle_timeout_ms.saturating_mul(1_000_000),
        "sample contains an inactive market stream"
    );
    ensure!(
        summary.event_age.max_us
            <= histogram().highest_equivalent(config.max_event_age_ms.saturating_mul(1000)),
        "sample exceeded event age limit"
    );
    Ok(stats)
}

#[derive(Debug, Serialize)]
pub struct RecoveryVerification {
    pub recovery_only: bool,
    pub prefix_proven: bool,
    pub verified_prefix: Option<Checkpoint>,
    pub validated_records: u64,
    pub validated_bytes: u64,
    pub recovered_records: u64,
    pub errors: Vec<String>,
}

pub fn verify_recovery(input: &Path) -> Result<RecoveryVerification> {
    let wal = input.join("raw.wal");
    let mut reader = WalReader::open(&wal)?;
    let mut processor = Processor::new(std::io::sink());
    let mut errors = vec![];
    loop {
        match reader.read_record() {
            Ok(Some(record)) => {
                processor.process(&record)?;
            }
            Ok(None) => break,
            Err(error) => {
                errors.push(format!("{error:#}"));
                break;
            }
        }
    }
    let records = processor.stats.records;
    let checkpoint = checkpoint::latest_verified_through(
        &input.join("checkpoints.jsonl"),
        &input.join("signals.jsonl"),
        records,
    )?;
    if let Some(expected) = &checkpoint {
        let mut prefix = Processor::new(std::io::sink());
        let mut reader = WalReader::open(&wal)?;
        while prefix.stats.last_seq < expected.last_seq {
            let record = reader
                .read_record()?
                .context("WAL ended before checkpoint")?;
            prefix.process(&record)?;
        }
        ensure!(
            prefix.checkpoint() == *expected,
            "checkpoint business results differ from WAL replay"
        );
    }
    Ok(RecoveryVerification {
        recovery_only: true,
        prefix_proven: checkpoint.is_some(),
        recovered_records: records - checkpoint.as_ref().map_or(0, |cp| cp.last_seq),
        verified_prefix: checkpoint,
        validated_records: records,
        validated_bytes: reader.offset,
        errors,
    })
}

pub fn report(input: &Path) -> Result<String> {
    let summary: Summary = serde_json::from_reader(File::open(input.join("summary.json"))?)?;
    let integrity = summary.received == summary.stats.records
        && (summary.mode == "replay" || summary.received == summary.durable);
    let formal = summary.mode == "live"
        && summary.complete
        && summary
            .requested_seconds
            .is_some_and(|seconds| seconds >= 900)
        && summary.stats.activity.duration_ns >= 899_000_000_000
        && summary.stats.activity.ready_ns > 0;
    let mut text = format!(
        "# YINO 性能与验收报告~\n\n模式：{}，完整运行：{}，正式 900 秒样本：{}~\n\n接收 {} / 同步 {} / 处理 {} / 信号 {}，计数完整性：{}~\n\n有效成交 {}，报价 {}，异常 {}，重复 {}，READY {:.3} 秒~\n\n成交最大空窗 {:.3} 秒，报价最大空窗 {:.3} 秒；输入结束积压 {}，排空 {}ms，队列峰值 {}/{}~\n\n| 指标 | 数量 | P50 μs | P95 μs | P99 μs | 最大 μs |\n|---|---:|---:|---:|---:|---:|\n",
        summary.mode,
        summary.complete,
        formal,
        summary.received,
        summary.durable,
        summary.stats.records,
        summary.stats.signals,
        integrity,
        summary.stats.trades,
        summary.stats.books,
        summary.stats.invalid,
        summary.stats.duplicates,
        summary.stats.activity.ready_ns as f64 / 1e9,
        summary.stats.activity.max_trade_gap_ns as f64 / 1e9,
        summary.stats.activity.max_book_gap_ns as f64 / 1e9,
        summary.backlog_at_input_end,
        summary.drain_ms,
        summary.queue1_peak,
        summary.queue2_peak
    );
    for (name, metric) in [
        ("队列1入队等待", &summary.stages.queue1_enqueue),
        ("队列1内等待", &summary.stages.queue1_residence),
        ("队列2入队等待", &summary.stages.queue2_enqueue),
        ("队列2内等待", &summary.stages.queue2_residence),
        ("取出至同步前写入和批次等待", &summary.stages.batch_wait),
        ("缓冲刷新", &summary.stages.flush),
        ("磁盘sync_data", &summary.sync),
        ("接收至持久化", &summary.persistence),
        ("引擎计算", &summary.processing),
        ("计算开始时事件年龄", &summary.event_age),
        ("接收至决策", &summary.receive_to_decision),
        ("接收至信号刷新", &summary.stages.signal_output),
    ] {
        text.push_str(&format!(
            "| {name} | {} | {} | {} | {} | {} |\n",
            metric.count, metric.p50_us, metric.p95_us, metric.p99_us, metric.max_us
        ));
    }
    let peak = summary
        .stats
        .activity
        .seconds
        .values()
        .map(|rate| rate.messages)
        .max()
        .unwrap_or(0);
    let trade_peak = summary
        .stats
        .activity
        .seconds
        .values()
        .map(|rate| rate.trades)
        .max()
        .unwrap_or(0);
    text.push_str(&format!("\n每秒行情峰值 {peak}，有效成交峰值 {trade_peak}；逐秒明细见 summary.json 的 activity.seconds，CPU/RSS/磁盘字节累计值及队列曲线见 resources.jsonl~\n\n批次大小 P50/P95/P99/最大：{}/{}/{}/{} 条；sync_data 共 {} 次~\n", summary.stages.batch_size.p50_us, summary.stages.batch_size.p95_us, summary.stages.batch_size.p99_us, summary.stages.batch_size.max_us, summary.sync_calls));
    text.push_str(&format!("\n错误：{:?}~\n\n队内等待从发送完成确认到消费开始测量，消费者早于发送确认的不可观测样本不填零，分别计数于 stages.queue1_unobserved/queue2_unobserved；此项是可观测下界，全量端到端延迟仍覆盖所有事件~\n\n计时使用本次运行时钟，业务逻辑使用录制时钟；CPU百分比按单核100%计，磁盘字节为进程物理IO累计值，页缓存可能使读取值为0；批次大小直方图的值单位为条~\n\ncomplete 表示本次指定运行通过，正式交付还要求严格 verify、检查点、全链路容量和故障验收；replay 模式不含新 WAL 持久化，不能代表全链路容量~\n", summary.errors));
    fs::write(input.join("report.md"), &text)?;
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_doubles_until_capped() {
        let delays: Vec<_> = (0..8)
            .map(|retry| reconnect_delay(retry).as_millis())
            .collect();
        assert_eq!(delays, [250, 500, 1000, 2000, 4000, 5000, 5000, 5000]);
        assert_eq!(reconnect_delay(u32::MAX), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn worker_failure_cancels_a_saturated_network_queue() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let config = Config {
            queue_capacity: 1,
            batch_size: 1,
            batch_ms: 5,
            enqueue_timeout_ms: 5000,
            max_event_age_ms: 1000,
            idle_timeout_ms: 10_000,
        };
        let received = AtomicU64::new(0);
        let counters = Counters::default();
        let failure = Notify::new();
        let (tx, rx) = flume::bounded(1);
        let server = async {
            let (socket, _) = listener.accept().await?;
            let mut socket = tokio_tungstenite::accept_async(socket).await?;
            socket.send(Message::Text("blocked".into())).await?;
            while received.load(Ordering::Relaxed) < 2 {
                tokio::task::yield_now().await;
            }
            failure.notify_one();
            std::future::pending::<()>().await;
            Ok::<_, anyhow::Error>(())
        };
        let errors = timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = receive(&endpoint, 900, &tx, &received, &failure, &config, &counters) => Ok(result),
                result = server => result.map(|()| vec![]),
            }
        })
        .await??;
        assert_eq!(errors, ["pipeline worker failed"]);
        assert_eq!(rx.recv()?.record.seq, 1);
        assert!(rx.try_recv().is_err());
        // The attempted but unconfirmed input still makes live accounting fail.
        assert_eq!(received.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[tokio::test]
    async fn worker_failure_cancels_a_pending_handshake() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let config = Config {
            queue_capacity: 1,
            batch_size: 1,
            batch_ms: 5,
            enqueue_timeout_ms: 100,
            max_event_age_ms: 1000,
            idle_timeout_ms: 10_000,
        };
        let received = AtomicU64::new(0);
        let counters = Counters::default();
        let failure = Notify::new();
        let (tx, _rx) = flume::bounded(1);
        let server = async {
            let (_socket, _) = listener.accept().await?;
            failure.notify_one();
            std::future::pending::<()>().await;
            Ok::<_, anyhow::Error>(())
        };
        let errors = timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = receive(&endpoint, 900, &tx, &received, &failure, &config, &counters) => Ok(result),
                result = server => result.map(|()| vec![]),
            }
        })
        .await??;
        assert_eq!(errors, ["pipeline worker failed"]);
        assert_eq!(received.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[test]
    fn failed_sync_never_releases_events() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut wal = WalWriter::create(&dir.path().join("raw.wal"))?;
        // /dev/null accepts writes/flush but rejects sync_data (EINVAL).
        wal.0 = BufWriter::new(File::options().write(true).open("/dev/null")?);
        let config = Config {
            queue_capacity: 1,
            batch_size: 1,
            batch_ms: 5,
            enqueue_timeout_ms: 100,
            max_event_age_ms: 1000,
            idle_timeout_ms: 10_000,
        };
        let (tx, rx) = flume::bounded(1);
        tx.send(TimedRecord::new(
            Record {
                seq: 1,
                connection: 1,
                utc_ns: 0,
                offset_ns: 0,
                event: Event::Connected,
            },
            Instant::now(),
        ))?;
        drop(tx);
        let (durable_tx, durable_rx) = flume::bounded(1);
        let result = persist(wal, rx, durable_tx, &config, &Counters::default());
        assert!(result.error.unwrap().contains("WAL sync_data failed"));
        assert_eq!(result.stats.durable, 0);
        assert!(durable_rx.try_recv().is_err());
        Ok(())
    }

    fn pressure_config() -> Config {
        Config {
            queue_capacity: 1,
            batch_size: 2,
            batch_ms: 5,
            enqueue_timeout_ms: 20,
            max_event_age_ms: 1000,
            idle_timeout_ms: 10_000,
        }
    }

    fn control_record(seq: u64, event: Event) -> TimedRecord {
        TimedRecord::new(
            Record {
                seq,
                connection: 1,
                utc_ns: seq,
                offset_ns: seq,
                event,
            },
            Instant::now(),
        )
    }

    #[test]
    fn slow_sync_keeps_entire_batch_hidden_until_sync_returns() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let wal = WalWriter::create(&dir.path().join("raw.wal"))?;
        let (tx, rx) = flume::bounded(2);
        tx.send(control_record(1, Event::Connected))?;
        tx.send(control_record(
            2,
            Event::Finished("duration_elapsed".into()),
        ))?;
        drop(tx);
        let (durable_tx, durable_rx) = flume::bounded(2);
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let worker = thread::spawn(move || {
            persist_with_sync(
                wal,
                rx,
                durable_tx,
                &pressure_config(),
                &Counters::default(),
                |wal| {
                    entered_tx.send(())?;
                    release_rx.recv_timeout(Duration::from_secs(2))?;
                    wal.0.get_ref().sync_data()?;
                    Ok(())
                },
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(2))?;
        // The hook holds sync in progress after append and flush have completed.
        assert!(matches!(
            durable_rx.recv_timeout(Duration::from_millis(30)),
            Err(flume::RecvTimeoutError::Timeout)
        ));
        release_tx.send(())?;
        for seq in 1..=2 {
            assert_eq!(
                durable_rx.recv_timeout(Duration::from_secs(2))?.record.seq,
                seq
            );
        }
        let result = worker.join().expect("WAL worker panicked");
        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.stats.durable, 2);
        assert_eq!(result.stats.sync_calls, 1);
        assert!(durable_rx.try_recv().is_err());
        Ok(())
    }

    #[test]
    fn saturated_queue2_stops_delivery_but_preserves_the_synced_batch() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("raw.wal");
        let wal = WalWriter::create(&path)?;
        let (tx, rx) = flume::bounded(3);
        tx.send(control_record(1, Event::Connected))?;
        tx.send(control_record(2, Event::Message("raw event".into())))?;
        tx.send(control_record(
            3,
            Event::Finished("duration_elapsed".into()),
        ))?;
        drop(tx);
        let pending = rx.clone();
        let (durable_tx, durable_rx) = flume::bounded(1);
        let counters = Counters::default();
        let result = persist(wal, rx, durable_tx, &pressure_config(), &counters);
        assert!(
            result
                .error
                .unwrap()
                .contains("queue 2 closed or enqueue timed out")
        );
        assert_eq!(result.stats.durable, 2);
        assert_eq!(result.stats.sync_calls, 1);
        assert_eq!(counters.durable.load(Ordering::Relaxed), 2);
        assert_eq!(durable_rx.recv()?.record.seq, 1);
        assert!(durable_rx.try_recv().is_err());
        // No later batch is consumed after the delivery timeout.
        assert_eq!(pending.recv()?.record.seq, 3);
        let mut reader = WalReader::open(&path)?;
        assert_eq!(reader.read_record()?.unwrap().seq, 1);
        assert_eq!(reader.read_record()?.unwrap().seq, 2);
        assert!(reader.read_record()?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn saturated_queue1_times_out_and_marks_sampling_failed() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let (tx, rx) = flume::bounded(1);
        let config = pressure_config();
        let counters = Counters::default();
        let received = AtomicU64::new(0);
        let failure = Notify::new();
        let server = async {
            let (socket, _) = listener.accept().await?;
            let mut socket = tokio_tungstenite::accept_async(socket).await?;
            socket.send(Message::Text("blocked".into())).await?;
            while let Some(message) = socket.next().await {
                if matches!(message?, Message::Close(_)) {
                    break;
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        let (errors, served) = timeout(Duration::from_secs(2), async {
            tokio::join!(
                receive(&endpoint, 900, &tx, &received, &failure, &config, &counters),
                server
            )
        })
        .await?;
        served?;
        assert!(
            errors
                .iter()
                .any(|error| error.contains("queue 1 enqueue timed out"))
        );
        assert_eq!(received.load(Ordering::Relaxed), 2);
        assert_eq!(rx.recv()?.record.seq, 1);
        assert!(rx.try_recv().is_err());
        Ok(())
    }

    #[tokio::test]
    async fn async_hash_matches_the_file_hash_across_chunk_boundaries() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("input");
        for bytes in [vec![], vec![0x5a; 3 * 64 * 1024 + 17]] {
            fs::write(&path, bytes)?;
            assert_eq!(async_file_hash(&path).await?, file_hash(&path)?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn pending_hash_read_can_be_cancelled_without_a_background_hash_loop() {
        use std::{
            pin::Pin,
            sync::atomic::AtomicBool,
            task::{Context, Poll},
        };
        struct PendingReader {
            entered: Option<tokio::sync::oneshot::Sender<()>>,
            dropped: Arc<AtomicBool>,
        }
        impl tokio::io::AsyncRead for PendingReader {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
                _: &mut tokio::io::ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                if let Some(entered) = self.entered.take() {
                    let _ = entered.send(());
                }
                Poll::Pending
            }
        }
        impl Drop for PendingReader {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::Relaxed);
            }
        }
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let reader = PendingReader {
            entered: Some(entered),
            dropped: dropped.clone(),
        };
        tokio::select! {
            result = waiting => result.expect("hash read was entered"),
            result = hash_reader(reader) => panic!("pending hash unexpectedly completed: {result:?}"),
        }
        assert!(dropped.load(Ordering::Relaxed));
    }
}
