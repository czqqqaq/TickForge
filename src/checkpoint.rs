use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

const MAX_LINE: u64 = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub format_version: u32,
    pub last_seq: u64,
    pub records: u64,
    pub signals: u64,
    pub last_signal_seq: u64,
    pub signal_bytes: u64,
    pub digest: String,
    pub signal_digest: String,
}

impl Checkpoint {
    fn valid_after(&self, previous: Option<&Self>) -> bool {
        let valid_hash = |hash: &str| {
            hash.len() == 64
                && hash
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        self.format_version == 1
            && self.records == self.last_seq
            && self.signals <= self.records
            && self.last_signal_seq <= self.last_seq
            && ((self.signals == 0 && self.last_signal_seq == 0 && self.signal_bytes == 0)
                || (self.signals > 0 && self.last_signal_seq > 0 && self.signal_bytes > 0))
            && valid_hash(&self.digest)
            && valid_hash(&self.signal_digest)
            && previous.is_none_or(|previous| {
                self == previous
                    || (self.last_seq > previous.last_seq
                        && self.signals >= previous.signals
                        && self.signal_bytes >= previous.signal_bytes
                        && self.last_signal_seq >= previous.last_signal_seq)
            })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checked {
    checkpoint: Checkpoint,
    crc32: u32,
}

pub struct CheckpointWriter {
    writer: BufWriter<File>,
    previous: Option<Checkpoint>,
}

impl CheckpointWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let file = File::options().write(true).create_new(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            previous: None,
        })
    }

    /// The caller must flush and sync the matching signals before appending.
    pub fn append(&mut self, checkpoint: &Checkpoint) -> Result<()> {
        ensure!(
            checkpoint.valid_after(self.previous.as_ref()),
            "invalid or regressing checkpoint"
        );
        let crc32 = crc32fast::hash(&serde_json::to_vec(checkpoint)?);
        serde_json::to_writer(
            &mut self.writer,
            &Checked {
                checkpoint: checkpoint.clone(),
                crc32,
            },
        )?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.previous = Some(checkpoint.clone());
        Ok(())
    }
}

/// Return only the consecutive checked prefix; never skip a damaged record.
pub fn read_valid(path: &Path) -> Result<Vec<Checkpoint>> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut checkpoints = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        let length = (&mut reader)
            .take(MAX_LINE + 1)
            .read_until(b'\n', &mut line)?;
        if length == 0 || length as u64 > MAX_LINE || line.last() != Some(&b'\n') {
            break;
        }
        let Ok(checked) = serde_json::from_slice::<Checked>(&line) else {
            break;
        };
        let checkpoint = checked.checkpoint;
        if !checkpoint.valid_after(checkpoints.last())
            || crc32fast::hash(&serde_json::to_vec(&checkpoint)?) != checked.crc32
        {
            break;
        }
        checkpoints.push(checkpoint);
    }
    Ok(checkpoints)
}

/// Find the newest checked record whose complete signal prefix is still present.
/// The caller must additionally replay the WAL and compare the business digest.
pub fn latest_verified(path: &Path, signals: &Path) -> Result<Option<Checkpoint>> {
    latest_verified_through(path, signals, u64::MAX)
}

pub fn latest_verified_through(
    path: &Path,
    signals: &Path,
    max_seq: u64,
) -> Result<Option<Checkpoint>> {
    let checkpoints = match read_valid(path) {
        Ok(checkpoints) => checkpoints,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let file = match File::open(signals) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut reader = BufReader::new(file);
    let mut hash = Sha256::new();
    let mut bytes = 0;
    let mut lines = 0;
    let mut last = None;
    let mut verified = None;
    let mut buffer = [0; 64 * 1024];
    for checkpoint in checkpoints {
        if checkpoint.last_seq > max_seq {
            break;
        }
        while bytes < checkpoint.signal_bytes {
            let length = (checkpoint.signal_bytes - bytes).min(buffer.len() as u64) as usize;
            let count = reader.read(&mut buffer[..length])?;
            if count == 0 {
                return Ok(verified);
            }
            hash.update(&buffer[..count]);
            bytes += count as u64;
            lines += buffer[..count]
                .iter()
                .filter(|&&byte| byte == b'\n')
                .count() as u64;
            last = Some(buffer[count - 1]);
        }
        if lines == checkpoint.signals
            && (bytes == 0 || last == Some(b'\n'))
            && format!("{:x}", hash.clone().finalize()) == checkpoint.signal_digest
        {
            verified = Some(checkpoint);
        }
    }
    Ok(verified)
}
