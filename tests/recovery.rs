#![cfg(unix)]

use anyhow::{Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Child, Command, Output, Stdio},
};
use tokio::{
    net::TcpListener,
    time::{Duration, Instant, sleep, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use yino::checkpoint::{latest_verified, read_valid};

struct Process(Option<Child>);

impl Process {
    fn spawn(args: &[&str]) -> Result<Self> {
        Ok(Self(Some(
            Command::new(env!("CARGO_BIN_EXE_yino"))
                .args(args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?,
        )))
    }

    async fn finish(mut self) -> Result<Output> {
        let deadline = Instant::now() + Duration::from_secs(8);
        while self.0.as_mut().unwrap().try_wait()?.is_none() {
            ensure!(Instant::now() < deadline, "child timed out");
            sleep(Duration::from_millis(10)).await;
        }
        Ok(self.0.take().unwrap().wait_with_output()?)
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn path(path: &Path) -> &str {
    path.to_str().unwrap()
}

fn book(id: u64) -> Message {
    Message::Text(json!({"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","u":id,"b":"100","a":"100.01","B":"1","A":"1"}}).to_string().into())
}

fn trade(id: u64) -> Message {
    Message::Text(
        json!({"stream":"btcusdt@trade","data":{"s":"BTCUSDT","t":id,"p":"100","q":"1","m":false}})
            .to_string()
            .into(),
    )
}

async fn recover(input: &Path) -> Result<Value> {
    let output = Process::spawn(&["verify", "--input", path(input), "--recover"])?
        .finish()
        .await?;
    assert_eq!(output.status.code(), Some(1));
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["recovery_only"], true);
    Ok(report)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigkill_proves_only_a_checkpoint_prefix_and_rejects_tampered_signals() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let dir = tempfile::tempdir()?;
    let live = dir.path().join("live");
    let mut child = Process::spawn(&[
        "live",
        "--out",
        path(&live),
        "--endpoint",
        &endpoint,
        "--seconds",
        "900",
    ])?;
    let server = tokio::spawn(async move {
        let (socket, _) = timeout(Duration::from_secs(3), listener.accept()).await??;
        let mut ws = accept_async(socket).await?;
        ws.send(book(1)).await?;
        for id in 1..=1000 {
            ws.send(trade(id)).await?;
        }
        let mut id = 1001;
        loop {
            tokio::select! {
                _ = sleep(Duration::from_millis(50)) => {
                    if ws.send(book(id)).await.is_err() || ws.send(trade(id)).await.is_err() {
                        break;
                    }
                    id += 1;
                }
                message = ws.next() => {
                    if matches!(message, None | Some(Err(_)) | Some(Ok(Message::Close(_)))) {
                        break;
                    }
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    });
    let checkpoints = live.join("checkpoints.jsonl");
    let signals = live.join("signals.jsonl");
    let deadline = Instant::now() + Duration::from_secs(8);
    let observed = loop {
        ensure!(
            Instant::now() < deadline,
            "no durable checkpoint after warmup"
        );
        if let Ok(points) = read_valid(&checkpoints)
            && points.len() >= 2
            && let Some(point) = latest_verified(&checkpoints, &signals)?
            && point.signals > 0
        {
            break point;
        }
        ensure!(
            child.0.as_mut().unwrap().try_wait()?.is_none(),
            "live exited before SIGKILL"
        );
        sleep(Duration::from_millis(20)).await;
    };
    child.0.as_mut().unwrap().kill()?;
    let output = child.finish().await?;
    assert!(output.status.code().is_none());
    timeout(Duration::from_secs(2), server).await???;
    assert!(!live.join("summary.json").exists());

    // A torn checkpoint tail cannot invalidate the last complete checkpoint.
    fs::OpenOptions::new()
        .append(true)
        .open(&checkpoints)?
        .write_all(b"{torn")?;
    let report = recover(&live).await?;
    let prefix = &report["verified_prefix"];
    assert!(prefix.is_object(), "{report}");
    let prefix_seq = prefix["last_seq"].as_u64().unwrap();
    assert!(prefix_seq >= observed.last_seq);
    assert!(report["validated_records"].as_u64().unwrap() >= prefix_seq);
    assert_eq!(prefix["signals"].as_u64(), Some(1));

    let mut bytes = fs::read(&signals)?;
    bytes[0] = b'!';
    fs::write(&signals, bytes)?;
    let report = recover(&live).await?;
    assert!(
        report["verified_prefix"].is_null() || report["verified_prefix"]["signals"] == 0,
        "{report}"
    );
    assert!(report["validated_records"].as_u64().unwrap() >= observed.last_seq);
    Ok(())
}
