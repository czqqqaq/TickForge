use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{
    fs,
    path::Path,
    process::{Command, Output},
};
use tokio::{
    net::TcpListener,
    time::{Duration, timeout},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use yino::{
    engine::{Engine, Target},
    run::Summary,
    wal::{Event, WalReader},
};

fn book(id: u64) -> Message {
    Message::Text(json!({"stream":"btcusdt@bookTicker", "data":{"s":"BTCUSDT","u":id,"b":"100","a":"100.01","B":"1","A":"1"}}).to_string().into())
}

fn trade(id: u64, sell: bool) -> Message {
    Message::Text(
        json!({"stream":"btcusdt@trade", "data":{"s":"BTCUSDT","t":id,"p":"100","q":"1","m":sell}})
            .to_string()
            .into(),
    )
}

async fn cli(args: Vec<String>) -> Result<Output> {
    Ok(tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_yino")).args(args).output()
    })
    .await??)
}

fn path(path: &Path) -> String {
    path.to_str().unwrap().into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_wal_signals_replay_and_tamper_detection() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let mut ws = accept_async(socket).await?;
        ws.send(Message::Ping(b"heartbeat".to_vec().into())).await?;
        let pong = timeout(Duration::from_secs(2), ws.next()).await?.unwrap()?;
        assert!(matches!(pong, Message::Pong(data) if data.as_ref() == b"heartbeat"));
        ws.send(book(1)).await?;
        for id in 1..=1000 {
            ws.send(trade(id, false)).await?;
        }
        while let Some(message) = ws.next().await {
            if matches!(message?, Message::Close(_)) {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    });
    let dir = tempfile::tempdir()?;
    let live = dir.path().join("live");
    let result = cli(vec![
        "live".into(),
        "--out".into(),
        path(&live),
        "--seconds".into(),
        "1".into(),
        "--endpoint".into(),
        endpoint,
    ])
    .await?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    server.await??;
    let live_summary: Summary = serde_json::from_slice(&fs::read(live.join("summary.json"))?)?;
    assert_eq!(live_summary.received, 1003);
    assert_eq!(live_summary.received, live_summary.durable);
    assert_eq!(live_summary.stats.signals, 1);
    assert_eq!(live_summary.stats.invalid, 0);
    assert!(fs::read_to_string(live.join("system.jsonl"))?.contains("Ping answered"));

    for speed in ["0", "10"] {
        let replay = dir.path().join(format!("replay-{speed}"));
        let result = cli(vec![
            "replay".into(),
            "--input".into(),
            path(&live.join("raw.wal")),
            "--out".into(),
            path(&replay),
            "--speed".into(),
            speed.into(),
        ])
        .await?;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let summary: Summary = serde_json::from_slice(&fs::read(replay.join("summary.json"))?)?;
        assert_eq!(live_summary.stats, summary.stats);
        assert_eq!(
            fs::read(live.join("signals.jsonl"))?,
            fs::read(replay.join("signals.jsonl"))?
        );
    }
    let result = cli(vec!["verify".into(), "--input".into(), path(&live)]).await?;
    assert!(result.status.success());
    fs::write(live.join("signals.jsonl"), b"tampered\n")?;
    let result = cli(vec!["verify".into(), "--input".into(), path(&live)]).await?;
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("signal file differs"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnected_sample_fails_and_preserves_evidence() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        accept_async(socket).await?.close(None).await?;
        Ok::<_, anyhow::Error>(())
    });
    let dir = tempfile::tempdir()?;
    let live = dir.path().join("failed");
    let result = cli(vec![
        "live".into(),
        "--out".into(),
        path(&live),
        "--seconds".into(),
        "1".into(),
        "--endpoint".into(),
        endpoint,
    ])
    .await?;
    assert!(!result.status.success());
    server.await??;
    let summary: Summary = serde_json::from_slice(&fs::read(live.join("summary.json"))?)?;
    assert!(!summary.complete);
    assert!(
        summary
            .errors
            .iter()
            .any(|error| error.contains("disconnected"))
    );
    assert!(live.join("raw.wal").is_file());
    let log = fs::read_to_string(live.join("system.jsonl"))?;
    assert!(log.contains("connection attempt failed"));
    let mut reader = WalReader::open(&live.join("raw.wal"))?;
    assert!(matches!(
        reader.read_record()?.unwrap().event,
        Event::Connected
    ));
    assert!(matches!(
        reader.read_record()?.unwrap().event,
        Event::Disconnected(_)
    ));
    assert!(matches!(
        reader.read_record()?.unwrap().event,
        Event::Finished(_)
    ));
    assert!(reader.read_record()?.is_none());
    assert!(
        !cli(vec!["verify".into(), "--input".into(), path(&live)])
            .await?
            .status
            .success()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconnect_rewarms_preserves_dedup_and_replays_every_connection() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let mut ws = accept_async(socket).await?;
        ws.send(book(100)).await?;
        for id in 1..=1000 {
            ws.send(trade(id, false)).await?;
        }
        ws.close(None).await?;
        drop(ws);

        let (socket, _) = timeout(Duration::from_secs(3), listener.accept()).await??;
        let mut ws = accept_async(socket).await?;
        ws.send(Message::Ping(b"reconnected".to_vec().into()))
            .await?;
        let pong = timeout(Duration::from_secs(2), ws.next()).await?.unwrap()?;
        assert!(matches!(pong, Message::Pong(data) if data.as_ref() == b"reconnected"));
        ws.send(trade(1000, true)).await?; // Prior connection's trade must stay deduplicated.
        ws.send(book(1)).await?; // Quote watermark must reset.
        for id in 1001..=2000 {
            ws.send(trade(id, true)).await?;
        }
        while let Some(message) = timeout(Duration::from_secs(4), ws.next()).await? {
            if matches!(message?, Message::Close(_)) {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    });
    let dir = tempfile::tempdir()?;
    let live = dir.path().join("live");
    let result = cli(vec![
        "live".into(),
        "--out".into(),
        path(&live),
        "--seconds".into(),
        "2".into(),
        "--endpoint".into(),
        endpoint,
    ])
    .await?;
    assert!(!result.status.success());
    server.await??;
    let summary: Summary = serde_json::from_slice(&fs::read(live.join("summary.json"))?)?;
    assert!(!summary.complete);
    assert_eq!(summary.errors.len(), 1, "{:?}", summary.errors);
    assert!(summary.errors[0].contains("connection 1 disconnected"));
    assert_eq!(summary.received, 2007);
    assert_eq!(summary.received, summary.durable);
    assert_eq!(summary.durable, summary.stats.records);
    assert_eq!(summary.stats.controls, 4);
    assert_eq!(summary.stats.trades, 2000);
    assert_eq!(summary.stats.books, 2);
    assert_eq!(summary.stats.duplicates, 1);
    assert_eq!(summary.stats.invalid, 0);
    assert_eq!(summary.stats.signals, 2);
    let log = fs::read_to_string(live.join("system.jsonl"))?;
    assert!(log.contains("WebSocket reconnected"));
    let attempts: Vec<serde_json::Value> = log
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert!(
        attempts
            .iter()
            .any(|line| line["fields"]["connection"] == 2 && line["fields"]["delay_ms"] == 250)
    );

    let mut reader = WalReader::open(&live.join("raw.wal"))?;
    let mut engine = Engine::default();
    let mut signals = vec![];
    let mut disconnected_ns = 0;
    while let Some(record) = reader.read_record()? {
        let outcome = engine.on_event(&record);
        if matches!(record.event, Event::Disconnected(_)) {
            disconnected_ns = record.offset_ns;
            assert_eq!(outcome.state, "DISCONNECTED");
            assert_eq!(outcome.target, Target::Long);
            assert!(outcome.factors.is_none());
            assert!(outcome.signal.is_none());
        }
        if record.connection == 2 && !matches!(record.event, Event::Finished(_)) {
            assert!(record.offset_ns > disconnected_ns);
            if record.seq < 2006 {
                assert_eq!(outcome.state, "WARMING_UP");
                assert_eq!(outcome.target, Target::Long);
                assert!(outcome.signal.is_none());
            } else {
                assert_eq!(outcome.state, "READY");
                assert_eq!(outcome.target, Target::Short);
            }
            if record.seq == 1005 {
                assert_eq!(outcome.class, "duplicate");
            }
            if record.seq == 1006 {
                assert_eq!(outcome.class, "book");
            }
        }
        if let Some(signal) = outcome.signal {
            signals.push(signal.target);
        }
    }
    assert_eq!(signals, [Target::Long, Target::Short]);
    for speed in ["0", "2", "10"] {
        let replay = dir.path().join(format!("replay-{speed}"));
        let result = cli(vec![
            "replay".into(),
            "--input".into(),
            path(&live.join("raw.wal")),
            "--out".into(),
            path(&replay),
            "--speed".into(),
            speed.into(),
        ])
        .await?;
        assert!(!result.status.success()); // Gap evidence survives a full replay.
        let replay_summary: Summary =
            serde_json::from_slice(&fs::read(replay.join("summary.json"))?)?;
        assert!(!replay_summary.complete);
        assert_eq!(replay_summary.stats, summary.stats);
        assert_eq!(replay_summary.errors, summary.errors);
        assert_eq!(
            fs::read(live.join("signals.jsonl"))?,
            fs::read(replay.join("signals.jsonl"))?
        );
    }
    assert!(
        !cli(vec!["verify".into(), "--input".into(), path(&live)])
            .await?
            .status
            .success()
    );
    // A torn tail after multiple connections recovers only the checked prefix.
    let truncated = dir.path().join("truncated.wal");
    let mut bytes = fs::read(live.join("raw.wal"))?;
    bytes.truncate(bytes.len() - 17);
    fs::write(&truncated, bytes)?;
    let recovery = dir.path().join("recovery");
    let result = cli(vec![
        "replay".into(),
        "--input".into(),
        path(&truncated),
        "--out".into(),
        path(&recovery),
        "--recover".into(),
    ])
    .await?;
    assert!(!result.status.success());
    let recovered: Summary = serde_json::from_slice(&fs::read(recovery.join("summary.json"))?)?;
    assert!(!recovered.complete);
    assert_eq!(recovered.stats.records, summary.stats.records - 1);
    assert_eq!(recovered.stats.signal_digest, summary.stats.signal_digest);
    assert!(
        recovered
            .errors
            .iter()
            .any(|error| error.contains("WAL byte offset"))
    );
    let mut forged: serde_json::Value =
        serde_json::from_slice(&fs::read(live.join("summary.json"))?)?;
    forged["complete"] = true.into();
    forged["errors"] = json!([]);
    fs::write(live.join("summary.json"), serde_json::to_vec(&forged)?)?;
    let result = cli(vec!["verify".into(), "--input".into(), path(&live)]).await?;
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("sample contains a connection gap"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_initial_handshake_without_creating_a_phantom_connection() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        drop(socket); // Fail the first handshake before subscription starts.
        let (socket, _) = timeout(Duration::from_secs(2), listener.accept()).await??;
        let mut ws = accept_async(socket).await?;
        ws.send(book(1)).await?;
        ws.send(trade(1, false)).await?;
        while let Some(message) = timeout(Duration::from_secs(3), ws.next()).await? {
            if matches!(message?, Message::Close(_)) {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    });
    let dir = tempfile::tempdir()?;
    let live = dir.path().join("live");
    let result = cli(vec![
        "live".into(),
        "--out".into(),
        path(&live),
        "--seconds".into(),
        "1".into(),
        "--endpoint".into(),
        endpoint,
    ])
    .await?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    server.await??;
    let summary: Summary = serde_json::from_slice(&fs::read(live.join("summary.json"))?)?;
    assert!(summary.complete);
    assert_eq!(summary.received, 4);
    let mut reader = WalReader::open(&live.join("raw.wal"))?;
    while let Some(record) = reader.read_record()? {
        assert_eq!(record.connection, 1);
    }
    assert!(
        cli(vec!["verify".into(), "--input".into(), path(&live)])
            .await?
            .status
            .success()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sampling_deadline_cancels_a_stalled_reconnect_handshake() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await?;
        let mut ws = accept_async(socket).await?;
        ws.send(book(1)).await?;
        ws.send(trade(1, false)).await?;
        drop(ws); // Abrupt transport failure rather than a WebSocket Close.
        let (_socket, _) = timeout(Duration::from_secs(2), listener.accept()).await??;
        // Keep TCP open without completing the new WebSocket handshake.
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok::<_, anyhow::Error>(())
    });
    let dir = tempfile::tempdir()?;
    let live = dir.path().join("live");
    let result = timeout(
        Duration::from_secs(3),
        cli(vec![
            "live".into(),
            "--out".into(),
            path(&live),
            "--seconds".into(),
            "1".into(),
            "--endpoint".into(),
            endpoint,
        ]),
    )
    .await??;
    assert!(!result.status.success());
    let summary: Summary = serde_json::from_slice(&fs::read(live.join("summary.json"))?)?;
    assert!(!summary.complete);
    assert_eq!(summary.received, 5);
    assert_eq!(summary.received, summary.durable);
    assert_eq!(summary.durable, summary.stats.records);
    let mut reader = WalReader::open(&live.join("raw.wal"))?;
    let mut last = None;
    while let Some(record) = reader.read_record()? {
        assert_eq!(record.connection, 1);
        last = Some(record);
    }
    assert!(matches!(last.unwrap().event, Event::Finished(_)));
    server.await??;
    Ok(())
}
