#![cfg(unix)]

use anyhow::{Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
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
use yino::wal::{Event, Record, WalWriter};

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
        let deadline = Instant::now() + Duration::from_secs(15);
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

fn book(id: u64) -> String {
    json!({"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","u":id,"b":"100","a":"100.01","B":"1","A":"1"}}).to_string()
}

fn trade(id: u64) -> String {
    json!({"stream":"btcusdt@trade","data":{"s":"BTCUSDT","t":id,"p":"100","q":"1","m":false}})
        .to_string()
}

fn summary(out: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(
        out.join("summary.json"),
    )?)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persisted_replay_matches_live_and_preserves_the_source_at_two_speeds() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        let (socket, _) = timeout(Duration::from_secs(3), listener.accept()).await??;
        let mut ws = accept_async(socket).await?;
        ws.send(Message::Text(book(1).into())).await?;
        for id in 1..=1000 {
            ws.send(Message::Text(trade(id).into())).await?;
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
    let output = Process::spawn(&[
        "live",
        "--out",
        path(&live),
        "--endpoint",
        &endpoint,
        "--seconds",
        "1",
    ])?
    .finish()
    .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    timeout(Duration::from_secs(2), server).await???;
    let expected = summary(&live)?;
    assert_eq!(expected["complete"], true);
    assert_eq!(expected["stats"]["signals"], 1);
    let source = live.join("raw.wal");
    let source_bytes = fs::read(&source)?;
    for speed in ["0", "2"] {
        let replay = dir.path().join(format!("persist-{speed}"));
        let output = Process::spawn(&[
            "replay",
            "--persist",
            "--input",
            path(&source),
            "--out",
            path(&replay),
            "--speed",
            speed,
        ])?
        .finish()
        .await?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let actual = summary(&replay)?;
        assert_eq!(actual["format_version"], 2);
        assert_eq!(actual["mode"], "replay_persist");
        assert_eq!(actual["complete"], true);
        assert_eq!(actual["stats"], expected["stats"]);
        assert_eq!(actual["received"], actual["durable"]);
        assert_eq!(actual["durable"], actual["stats"]["records"]);
        assert!(actual["sync_calls"].as_u64().unwrap() > 0);
        assert_eq!(fs::read(replay.join("raw.wal"))?, source_bytes);
        assert_eq!(
            fs::read(replay.join("signals.jsonl"))?,
            fs::read(live.join("signals.jsonl"))?
        );
        assert_eq!(fs::read(&source)?, source_bytes);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlimited_replay_cancellation_preserves_summary_with_and_without_persistence() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let source = dir.path().join("source.wal");
    let mut writer = WalWriter::create(&source)?;
    let total = 200_003;
    for seq in 1..=total {
        let event = match seq {
            1 => Event::Connected,
            2 => Event::Message(book(1)),
            200_003 => Event::Finished("duration_elapsed".into()),
            _ => Event::Message(trade(seq - 2)),
        };
        writer.append(&Record {
            seq,
            connection: 1,
            utc_ns: seq,
            offset_ns: seq,
            event,
        })?;
    }
    writer.sync()?;
    drop(writer);
    let original = fs::read(&source)?;
    for persist in [false, true] {
        let out = dir.path().join(if persist { "persist" } else { "compute" });
        let mut args = vec![
            "replay",
            "--input",
            path(&source),
            "--out",
            path(&out),
            "--speed",
            "0",
        ];
        if persist {
            args.push("--persist");
        }
        let mut child = Process::spawn(&args)?;
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            ensure!(
                Instant::now() < deadline,
                "replay produced no initial signal"
            );
            if fs::metadata(out.join("signals.jsonl")).is_ok_and(|metadata| metadata.len() > 0) {
                break;
            }
            ensure!(
                child.0.as_mut().unwrap().try_wait()?.is_none(),
                "replay exited before cancellation"
            );
            sleep(Duration::from_millis(5)).await;
        }
        let pid = child.0.as_ref().unwrap().id().to_string();
        ensure!(
            Command::new("kill")
                .args(["-INT", &pid])
                .status()?
                .success(),
            "SIGINT failed"
        );
        let output = child.finish().await?;
        assert_eq!(
            output.status.code(),
            Some(1),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let actual = summary(&out)?;
        assert_eq!(actual["complete"], false);
        assert!(actual["stats"]["records"].as_u64().unwrap() < total);
        assert!(
            actual["errors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|error| error.as_str().unwrap().contains("interrupted"))
        );
        assert_eq!(fs::read(&source)?, original);
    }
    // The same registered stop request must also interrupt the post-replay hash.
    let out = dir.path().join("final-hash");
    let mut child = Process::spawn(&["replay", "--input", path(&source), "--out", path(&out)])?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        ensure!(Instant::now() < deadline, "final hash never started");
        if fs::read_to_string(out.join("system.jsonl"))
            .is_ok_and(|log| log.contains("\"phase\":\"final\""))
        {
            break;
        }
        ensure!(
            child.0.as_mut().unwrap().try_wait()?.is_none(),
            "replay exited before final hash cancellation"
        );
        sleep(Duration::from_millis(2)).await;
    }
    let pid = child.0.as_ref().unwrap().id().to_string();
    ensure!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()?
            .success(),
        "SIGTERM failed"
    );
    let output = child.finish().await?;
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual = summary(&out)?;
    assert_eq!(actual["complete"], false);
    assert_eq!(actual["stats"]["records"], total);
    assert!(actual["source_wal_sha256"].is_string());
    assert!(
        actual["errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error.as_str().unwrap().contains("final hash verification"))
    );
    assert_eq!(fs::read(&source)?, original);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_hash_cancellation_preserves_zero_record_summary_for_both_signals() -> Result<()> {
    use std::io::Write;
    let dir = tempfile::tempdir()?;
    let source = dir.path().join("large.wal");
    let mut file = fs::File::create(&source)?;
    file.write_all(b"YINOWAL1")?;
    file.set_len(512 * 1024 * 1024)?;
    drop(file);
    for signal in ["-INT", "-TERM"] {
        for persist in [false, true] {
            let out = dir.path().join(format!("startup-{signal}-{persist}"));
            let mut args = vec!["replay", "--input", path(&source), "--out", path(&out)];
            if persist {
                args.push("--persist");
            }
            let mut child = Process::spawn(&args)?;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                ensure!(Instant::now() < deadline, "initial hash never started");
                if fs::read_to_string(out.join("system.jsonl"))
                    .is_ok_and(|log| log.contains("\"phase\":\"initial\""))
                {
                    break;
                }
                ensure!(
                    child.0.as_mut().unwrap().try_wait()?.is_none(),
                    "replay exited before cancellation"
                );
                sleep(Duration::from_millis(2)).await;
            }
            let pid = child.0.as_ref().unwrap().id().to_string();
            ensure!(
                Command::new("kill")
                    .args([signal, &pid])
                    .status()?
                    .success(),
                "stop signal failed"
            );
            let output = child.finish().await?;
            assert_eq!(
                output.status.code(),
                Some(1),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let actual = summary(&out)?;
            assert_eq!(actual["complete"], false);
            assert_eq!(actual["stats"]["records"], 0);
            assert!(actual["source_wal_sha256"].is_null());
            assert_eq!(actual["wal_sha256"], "");
            assert!(
                actual["errors"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|error| error.as_str().unwrap().contains("interrupted"))
            );
            assert_eq!(fs::metadata(&source)?.len(), 512 * 1024 * 1024);
        }
    }
    Ok(())
}
