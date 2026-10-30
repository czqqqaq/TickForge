#![cfg(unix)]

use anyhow::{Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{
    fs,
    path::Path,
    process::{Child, Command, Output, Stdio},
};
use tokio::{
    net::TcpListener,
    time::{Duration, Instant, sleep, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use yino::{
    run::Summary,
    wal::{Event, WalReader},
};

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

fn live(out: &Path, endpoint: &str, extra: &[&str]) -> Result<Child> {
    Ok(Command::new(env!("CARGO_BIN_EXE_yino"))
        .args(["live", "--out"])
        .arg(out)
        .args(["--endpoint", endpoint])
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?)
}

async fn finished(mut child: Child) -> Result<Output> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            anyhow::bail!("child did not finish within 5 seconds");
        }
        sleep(Duration::from_millis(10)).await;
    }
    Ok(child.wait_with_output()?)
}

async fn stop_signal_drains(signal: &str) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let dir = tempfile::tempdir()?;
    let out = dir.path().join("live");
    let child = live(&out, &endpoint, &["--seconds", "900"])?;
    let (socket, _) = timeout(Duration::from_secs(2), listener.accept()).await??;
    let mut socket = accept_async(socket).await?;
    socket.send(book(1)).await?;
    for id in 1..=1000 {
        socket.send(trade(id)).await?;
    }
    socket.send(Message::Ping(b"ready".to_vec().into())).await?;
    let pong = timeout(Duration::from_secs(2), socket.next())
        .await?
        .unwrap()?;
    ensure!(
        matches!(pong, Message::Pong(_)),
        "missing heartbeat response"
    );
    ensure!(
        Command::new("kill")
            .args([signal, &child.id().to_string()])
            .status()?
            .success(),
        "could not send stop signal"
    );
    let output = finished(child).await?;
    assert_eq!(output.status.code(), Some(1));
    let summary: Summary = serde_json::from_slice(&fs::read(out.join("summary.json"))?)?;
    assert!(!summary.complete);
    assert_eq!(summary.received, summary.durable);
    assert_eq!(summary.durable, summary.stats.records);
    assert_eq!(summary.stats.trades, 1000);
    assert!(summary.errors.iter().any(|e| e.contains("interrupted")));
    let mut reader = WalReader::open(&out.join("raw.wal"))?;
    let mut last = None;
    while let Some(record) = reader.read_record()? {
        last = Some(record);
    }
    assert!(matches!(last.unwrap().event, Event::Disconnected(_)));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_drains_and_preserves_an_incomplete_summary() -> Result<()> {
    stop_signal_drains("-TERM").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigint_drains_and_preserves_an_incomplete_summary() -> Result<()> {
    stop_signal_drains("-INT").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heartbeat_only_connection_times_out_and_reconnects() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let dir = tempfile::tempdir()?;
    let out = dir.path().join("live");
    let child = live(
        &out,
        &endpoint,
        &["--seconds", "1", "--idle-timeout-ms", "100"],
    )?;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let mut socket = accept_async(socket).await?;
        socket.send(book(1)).await?;
        socket.send(trade(1)).await?;
        // Healthy protocol heartbeats must not conceal a stalled market stream.
        loop {
            if socket
                .send(Message::Ping(b"alive".to_vec().into()))
                .await
                .is_err()
            {
                break;
            }
            match timeout(Duration::from_secs(1), socket.next()).await? {
                Some(Ok(Message::Pong(_))) => sleep(Duration::from_millis(20)).await,
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                other => anyhow::bail!("unexpected heartbeat result: {other:?}"),
            }
        }
        let (socket, _) = timeout(Duration::from_secs(2), listener.accept()).await??;
        let mut socket = accept_async(socket).await?;
        for id in 2..100 {
            socket.send(book(id)).await?;
            socket.send(trade(id)).await?;
            if let Ok(Some(message)) = timeout(Duration::from_millis(20), socket.next()).await
                && matches!(message?, Message::Close(_))
            {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    });
    let output = finished(child).await?;
    assert_eq!(output.status.code(), Some(1));
    server.await??;
    let summary: Summary = serde_json::from_slice(&fs::read(out.join("summary.json"))?)?;
    assert!(!summary.complete);
    assert!(summary.errors.iter().any(|e| e.contains("idle timeout")));
    assert_eq!(summary.received, summary.durable);
    assert_eq!(summary.durable, summary.stats.records);
    let mut reader = WalReader::open(&out.join("raw.wal"))?;
    let mut connections = vec![];
    while let Some(record) = reader.read_record()? {
        if matches!(record.event, Event::Connected) {
            connections.push(record.connection);
        }
    }
    assert_eq!(connections, [1, 2]);
    Ok(())
}
