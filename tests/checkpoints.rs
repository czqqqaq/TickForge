use anyhow::Result;
use sha2::{Digest, Sha256};
use std::{fs, io::Write};
use yino::checkpoint::{
    Checkpoint, CheckpointWriter, latest_verified, latest_verified_through, read_valid,
};

fn checkpoint(last_seq: u64, signals: &[u8], count: u64) -> Checkpoint {
    Checkpoint {
        format_version: 1,
        last_seq,
        records: last_seq,
        signals: count,
        last_signal_seq: if count == 0 { 0 } else { last_seq },
        signal_bytes: signals.len() as u64,
        digest: format!("{:x}", Sha256::digest(last_seq.to_le_bytes())),
        signal_digest: format!("{:x}", Sha256::digest(signals)),
    }
}

#[test]
fn preserves_valid_prefix_before_torn_corrupt_or_regressing_tail() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("checkpoints.jsonl");
    let mut writer = CheckpointWriter::create(&path)?;
    let first = checkpoint(10, b"", 0);
    let second = checkpoint(20, b"{\"seq\":20}\n", 1);
    writer.append(&first)?;
    writer.append(&second)?;
    assert!(writer.append(&first).is_err());
    drop(writer);
    let bytes = fs::read(&path)?;
    assert_eq!(read_valid(&path)?, [first.clone(), second]);

    let mut lines = bytes.split_inclusive(|byte| *byte == b'\n');
    let first_line = lines.next().unwrap();
    let second_line = lines.next().unwrap();
    for tail in [
        second_line[..second_line.len() - 2].to_vec(),
        b"{bad json}\n".to_vec(),
        {
            let mut line = second_line.to_vec();
            let index = line.iter().position(|byte| *byte == b'2').unwrap();
            line[index] = b'3';
            line
        },
        vec![b'x'; 5000],
    ] {
        let mut damaged = first_line.to_vec();
        damaged.extend(tail);
        damaged.extend_from_slice(second_line); // A good suffix must not be skipped to.
        fs::write(&path, damaged)?;
        assert_eq!(read_valid(&path)?, std::slice::from_ref(&first));
    }
    Ok(())
}

#[test]
fn chooses_latest_checkpoint_with_a_complete_matching_signal_prefix() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("checkpoints.jsonl");
    let signals = dir.path().join("signals.jsonl");
    let first_line = b"{\"seq\":10}\n";
    let full = b"{\"seq\":10}\n{\"seq\":20}\n";
    let first = checkpoint(10, first_line, 1);
    let second = checkpoint(20, full, 2);
    let mut writer = CheckpointWriter::create(&path)?;
    writer.append(&first)?;
    writer.append(&second)?;
    drop(writer);
    fs::write(&signals, full)?;
    assert_eq!(
        latest_verified_through(&path, &signals, 15)?,
        Some(first.clone())
    );
    assert!(latest_verified_through(&path, &signals, 9)?.is_none());
    assert_eq!(latest_verified(&path, &signals)?, Some(second));
    fs::write(&signals, &full[..full.len() - 2])?;
    assert_eq!(latest_verified(&path, &signals)?, Some(first.clone()));
    fs::OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(b"{torn")?;
    assert_eq!(latest_verified(&path, &signals)?, Some(first));
    fs::write(&signals, b"tampered\n")?;
    assert!(latest_verified(&path, &signals)?.is_none());
    Ok(())
}

#[test]
fn empty_signals_are_valid_but_missing_evidence_is_not() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("checkpoints.jsonl");
    let signals = dir.path().join("signals.jsonl");
    assert!(latest_verified(&path, &signals)?.is_none());
    let first = checkpoint(5, b"", 0);
    let mut writer = CheckpointWriter::create(&path)?;
    writer.append(&first)?;
    drop(writer);
    assert!(latest_verified(&path, &signals)?.is_none());
    fs::write(&signals, b"")?;
    assert_eq!(latest_verified(&path, &signals)?, Some(first));
    Ok(())
}
