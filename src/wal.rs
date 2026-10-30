use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
};

const MAGIC: &[u8; 8] = b"YINOWAL1";
pub const MAX_RECORD: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Event {
    Connected,
    Message(String),
    Disconnected(String),
    Finished(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Record {
    pub seq: u64,
    pub connection: u64,
    pub utc_ns: u64,
    pub offset_ns: u64,
    pub event: Event,
}

pub struct WalWriter(pub(crate) BufWriter<File>, Vec<u8>);

impl WalWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let file = File::options().write(true).create_new(true).open(path)?;
        let mut writer = Self(BufWriter::new(file), Vec::new());
        writer.0.write_all(MAGIC)?;
        writer.sync()?;
        Ok(writer)
    }

    pub fn append(&mut self, record: &Record) -> Result<()> {
        self.1.clear();
        serde_json::to_writer(&mut self.1, record)?;
        ensure!(self.1.len() <= MAX_RECORD, "record exceeds size limit");
        self.0.write_all(&(self.1.len() as u32).to_le_bytes())?;
        self.0.write_all(&crc32fast::hash(&self.1).to_le_bytes())?;
        self.0.write_all(&self.1)?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.0.flush()?;
        self.0.get_ref().sync_data().context("WAL sync_data failed")
    }
}

pub struct WalReader {
    reader: BufReader<File>,
    buffer: Vec<u8>,
    pub offset: u64,
    seq: u64,
    time_ns: u64,
    connection: u64,
    connected: bool,
    finished: bool,
}

impl WalReader {
    pub fn open(path: &Path) -> Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut header = [0; 8];
        reader
            .read_exact(&mut header)
            .context("truncated WAL header")?;
        ensure!(&header == MAGIC, "unsupported WAL version");
        Ok(Self {
            reader,
            buffer: Vec::new(),
            offset: 8,
            seq: 0,
            time_ns: 0,
            connection: 0,
            connected: false,
            finished: false,
        })
    }

    pub fn read_record(&mut self) -> Result<Option<Record>> {
        let start = self.offset;
        self.read_inner()
            .with_context(|| format!("WAL byte offset {start}"))
    }

    fn read_inner(&mut self) -> Result<Option<Record>> {
        let mut length = [0; 4];
        if self.reader.read(&mut length[..1])? == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut length[1..])
            .context("truncated record length")?;
        let length = u32::from_le_bytes(length) as usize;
        ensure!(
            length > 0 && length <= MAX_RECORD,
            "invalid record length {length}"
        );
        let mut checksum = [0; 4];
        self.reader
            .read_exact(&mut checksum)
            .context("truncated checksum")?;
        self.buffer.resize(length, 0);
        self.reader
            .read_exact(&mut self.buffer)
            .context("truncated record payload")?;
        if crc32fast::hash(&self.buffer) != u32::from_le_bytes(checksum) {
            bail!("record checksum mismatch");
        }
        let record: Record = serde_json::from_slice(&self.buffer)?;
        ensure!(
            record.seq == self.seq + 1,
            "non-contiguous sequence {}",
            record.seq
        );
        ensure!(record.offset_ns >= self.time_ns, "non-monotonic event time");
        ensure!(!self.finished, "record after Finished event");
        let connection = self.connection + u64::from(matches!(record.event, Event::Connected));
        ensure!(
            record.connection > 0 && record.connection == connection,
            "invalid connection ID"
        );
        match &record.event {
            Event::Connected => {
                ensure!(
                    !self.connected,
                    "Connected event without prior Disconnected"
                );
                self.connected = true;
            }
            Event::Message(_) => ensure!(self.connected, "message while disconnected"),
            Event::Disconnected(_) => {
                ensure!(self.connected, "duplicate Disconnected event");
                self.connected = false;
            }
            Event::Finished(_) => self.finished = true,
        }
        self.connection = connection;
        self.seq = record.seq;
        self.time_ns = record.offset_ns;
        self.offset += 8 + length as u64;
        Ok(Some(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_events(path: &Path, events: Vec<(u64, Event)>) -> Result<()> {
        let mut writer = WalWriter::create(path)?;
        for (index, (connection, event)) in events.into_iter().enumerate() {
            let seq = index as u64 + 1;
            writer.append(&Record {
                seq,
                connection,
                utc_ns: seq,
                offset_ns: seq,
                event,
            })?;
        }
        writer.sync()
    }

    #[test]
    fn reused_buffers_preserve_wire_bytes_for_growing_and_shrinking_records() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("buffers.wal");
        let mut writer = WalWriter::create(&path)?;
        let mut expected = MAGIC.to_vec();
        let events = [
            Event::Connected,
            Event::Message("\"\n中".repeat(4096)),
            Event::Message("x".into()),
            Event::Message("large".repeat(10000)),
            Event::Finished("duration_elapsed".into()),
        ];
        for (index, event) in events.into_iter().enumerate() {
            let seq = index as u64 + 1;
            let record = Record {
                seq,
                connection: 1,
                utc_ns: seq,
                offset_ns: seq,
                event,
            };
            let payload = serde_json::to_vec(&record)?;
            expected.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            expected.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
            expected.extend_from_slice(&payload);
            writer.append(&record)?;
        }
        writer.sync()?;
        assert_eq!(std::fs::read(&path)?, expected);
        let mut reader = WalReader::open(&path)?;
        for seq in 1..=5 {
            assert_eq!(reader.read_record()?.unwrap().seq, seq);
        }
        assert!(reader.read_record()?.is_none());
        assert_eq!(reader.offset, expected.len() as u64);
        Ok(())
    }

    #[test]
    fn reads_multiple_connections_and_a_finish_during_a_gap() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("raw.wal");
        write_events(
            &path,
            vec![
                (1, Event::Connected),
                (1, Event::Message("first".into())),
                (1, Event::Disconnected("test".into())),
                (2, Event::Connected),
                (2, Event::Message("second".into())),
                (2, Event::Disconnected("test".into())),
                (2, Event::Finished("duration_elapsed".into())),
            ],
        )?;
        let mut reader = WalReader::open(&path)?;
        let mut connections = vec![];
        while let Some(record) = reader.read_record()? {
            connections.push(record.connection);
        }
        assert_eq!(connections, [1, 1, 1, 2, 2, 2, 2]);
        assert_eq!(reader.offset, std::fs::metadata(path)?.len());
        Ok(())
    }

    #[test]
    fn rejects_invalid_connection_order_at_the_first_bad_record() -> Result<()> {
        let cases = [
            vec![(0, Event::Connected)],
            vec![(2, Event::Connected)],
            vec![(1, Event::Message("no connection".into()))],
            vec![
                (1, Event::Connected),
                (2, Event::Message("wrong connection".into())),
            ],
            vec![
                (1, Event::Connected),
                (2, Event::Disconnected("wrong connection".into())),
            ],
            vec![(1, Event::Connected), (1, Event::Connected)],
            vec![(1, Event::Connected), (2, Event::Connected)],
            vec![
                (1, Event::Connected),
                (1, Event::Disconnected("test".into())),
                (3, Event::Connected),
            ],
            vec![
                (1, Event::Connected),
                (1, Event::Disconnected("test".into())),
                (1, Event::Connected),
            ],
            vec![
                (1, Event::Connected),
                (1, Event::Disconnected("test".into())),
                (1, Event::Message("disconnected".into())),
            ],
            vec![
                (1, Event::Connected),
                (1, Event::Disconnected("test".into())),
                (1, Event::Disconnected("duplicate".into())),
            ],
            vec![
                (1, Event::Connected),
                (1, Event::Finished("duration_elapsed".into())),
                (1, Event::Message("after finish".into())),
            ],
        ];
        let dir = tempfile::tempdir()?;
        for (index, events) in cases.into_iter().enumerate() {
            let valid_prefix = events.len() - 1;
            let path = dir.path().join(index.to_string());
            write_events(&path, events)?;
            let mut reader = WalReader::open(&path)?;
            for _ in 0..valid_prefix {
                assert!(reader.read_record()?.is_some());
            }
            let offset = reader.offset;
            let error = reader.read_record().unwrap_err();
            assert!(error.to_string().contains(&format!("offset {offset}")));
            assert_eq!(reader.offset, offset);
        }
        Ok(())
    }

    #[test]
    fn rejects_corruption_truncation_and_sequence_gaps() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("raw.wal");
        let mut writer = WalWriter::create(&path)?;
        for seq in 1..=2 {
            writer.append(&Record {
                seq,
                connection: 1,
                utc_ns: seq,
                offset_ns: seq,
                event: if seq == 1 {
                    Event::Connected
                } else {
                    Event::Message("test".into())
                },
            })?;
        }
        writer.sync()?;
        let bytes = std::fs::read(&path)?;
        let mut reader = WalReader::open(&path)?;
        assert_eq!(reader.read_record()?.unwrap().seq, 1);
        let second_start = reader.offset as usize;
        assert_eq!(reader.read_record()?.unwrap().seq, 2);
        assert!(reader.read_record()?.is_none());

        // Every partial second record must preserve the first and reject the tail.
        for end in second_start + 1..bytes.len() {
            std::fs::write(&path, &bytes[..end])?;
            let mut reader = WalReader::open(&path)?;
            assert_eq!(reader.read_record()?.unwrap().seq, 1);
            assert!(
                reader.read_record().is_err(),
                "accepted truncation at {end}"
            );
        }
        let mut corrupt = bytes;
        corrupt[second_start + 8] ^= 1;
        std::fs::write(&path, corrupt)?;
        let mut reader = WalReader::open(&path)?;
        assert!(reader.read_record()?.is_some());
        assert!(
            reader
                .read_record()
                .unwrap_err()
                .to_string()
                .contains("offset")
        );

        let gap = dir.path().join("gap.wal");
        let mut writer = WalWriter::create(&gap)?;
        writer.append(&Record {
            seq: 2,
            connection: 1,
            utc_ns: 0,
            offset_ns: 0,
            event: Event::Connected,
        })?;
        writer.sync()?;
        assert!(WalReader::open(&gap)?.read_record().is_err());
        Ok(())
    }
}
