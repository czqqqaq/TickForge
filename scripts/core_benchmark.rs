#[path = "ENGINE"]
mod engine;
#[path = "WAL"]
mod wal;
use anyhow::Result;
use sha2::{Digest, Sha256};
use std::{fs, hint::black_box, path::Path, time::Instant};

fn hash_records(records: &[wal::Record]) -> Result<String> {
    let mut engine = engine::Engine::default();
    let mut hash = Sha256::new();
    for record in records {
        let bytes = serde_json::to_vec(&engine.on_event(record))?;
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let input = Path::new(&args[1]);
    let output = Path::new(&args[2]);
    let mut reader = wal::WalReader::open(input)?;
    let mut records = vec![];
    while let Some(record) = reader.read_record()? {
        records.push(record);
    }
    let mut read_us = vec![];
    let mut engine_us = vec![];
    let mut write_us = vec![];
    for sample in 0..9 {
        let start = Instant::now();
        for _ in 0..25 {
            let mut engine = engine::Engine::default();
            for record in &records {
                black_box(engine.on_event(black_box(record)));
            }
        }
        engine_us.push(start.elapsed().as_micros());
        let start = Instant::now();
        for _ in 0..25 {
            let mut reader = wal::WalReader::open(input)?;
            while let Some(record) = reader.read_record()? {
                black_box(record);
            }
        }
        read_us.push(start.elapsed().as_micros());
        let start = Instant::now();
        for pass in 0..25 {
            let path = output.join(format!("write-{sample}-{pass}.wal"));
            let mut writer = wal::WalWriter::create(&path)?;
            for record in &records {
                writer.append(black_box(record))?;
            }
            writer.sync()?;
            black_box(fs::metadata(&path)?.len());
            fs::remove_file(path)?;
        }
        write_us.push(start.elapsed().as_micros());
    }
    println!(
        "{}",
        serde_json::json!({
            "records_per_round":records.len(), "rounds_per_sample":25,
            "engine_us":engine_us, "wal_read_us":read_us, "wal_write_us":write_us,
            "digest":hash_records(&records)?
        })
    );
    Ok(())
}
