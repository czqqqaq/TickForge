use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::watch;

pub struct Counters {
    pub durable: AtomicU64,
    pub processed: AtomicU64,
    pub queue1_peak: AtomicU64,
    pub queue2_peak: AtomicU64,
    pub queue1_enqueue: Mutex<hdrhistogram::Histogram<u64>>,
}
impl Default for Counters {
    fn default() -> Self {
        Self {
            durable: AtomicU64::new(0),
            processed: AtomicU64::new(0),
            queue1_peak: AtomicU64::new(0),
            queue2_peak: AtomicU64::new(0),
            queue1_enqueue: Mutex::new(hdrhistogram::Histogram::new(3).expect("valid histogram")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceSample {
    pub elapsed_ms: u64,
    pub received: u64,
    pub durable: u64,
    pub processed: u64,
    pub received_per_second: f64,
    pub processed_per_second: f64,
    pub queue1: usize,
    pub queue2: usize,
    pub cpu_percent: f64,
    pub rss_bytes: u64,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
}

fn proc_number(path: &str, key: &str) -> u64 {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next()? == key)
                    .then(|| fields.next()?.parse().ok())
                    .flatten()
            })
        })
        .unwrap_or(0)
}

fn cpu_ticks() -> u64 {
    fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|text| {
            let tail = text.rsplit_once(')')?.1;
            let values: Vec<_> = tail.split_whitespace().collect();
            Some(values.get(11)?.parse::<u64>().ok()? + values.get(12)?.parse::<u64>().ok()?)
        })
        .unwrap_or(0)
}

pub async fn monitor<T: Send + 'static>(
    out: &Path,
    input: flume::WeakSender<T>,
    output: Option<flume::WeakSender<T>>,
    received: Arc<AtomicU64>,
    counters: Arc<Counters>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let mut file = BufWriter::new(File::create(out.join("resources.jsonl"))?);
    let ticks_per_second: f64 = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()?
        .stdout
        .split(|b| b.is_ascii_whitespace())
        .find(|part| !part.is_empty())
        .and_then(|part| std::str::from_utf8(part).ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(100.0);
    let origin = Instant::now();
    let mut last = origin;
    let mut last_ticks = cpu_ticks();
    let mut last_received = 0;
    let mut last_processed = 0;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {},
            changed = stop.changed() => { if changed.is_err() { break; } },
        }
        let now = Instant::now();
        let elapsed = now.duration_since(last).as_secs_f64().max(f64::EPSILON);
        let ticks = cpu_ticks();
        let received = received.load(Ordering::Relaxed);
        let processed = counters.processed.load(Ordering::Relaxed);
        let sample = ResourceSample {
            elapsed_ms: origin.elapsed().as_millis() as u64,
            received,
            durable: counters.durable.load(Ordering::Relaxed),
            processed,
            received_per_second: received.saturating_sub(last_received) as f64 / elapsed,
            processed_per_second: processed.saturating_sub(last_processed) as f64 / elapsed,
            queue1: input.upgrade().map_or(0, |tx| tx.len()),
            queue2: output
                .as_ref()
                .and_then(|tx| tx.upgrade())
                .map_or(0, |tx| tx.len()),
            cpu_percent: ticks.saturating_sub(last_ticks) as f64 / ticks_per_second / elapsed
                * 100.0,
            rss_bytes: proc_number("/proc/self/status", "VmRSS:") * 1024,
            disk_read_bytes: proc_number("/proc/self/io", "read_bytes:"),
            disk_write_bytes: proc_number("/proc/self/io", "write_bytes:"),
        };
        serde_json::to_writer(&mut file, &sample)?;
        file.write_all(b"\n")?;
        file.flush()?;
        last = now;
        last_ticks = ticks;
        last_received = received;
        last_processed = processed;
        if *stop.borrow() {
            break;
        }
    }
    file.get_ref().sync_data()?;
    Ok(())
}
