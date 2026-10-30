#!/usr/bin/env python3
"""Isolated engine/WAL benchmark; requires cargo build --release first."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def check_digest(results):
    if results["before"]["digest"] != results["after"]["digest"]:
        raise RuntimeError("Business digest changed")


def benchmark(args, root=ROOT):
    args.out.mkdir(parents=True)
    fixed_input = args.out / "input.wal"
    input_hash = sha(args.input)
    shutil.copyfile(args.input, fixed_input)
    if sha(fixed_input) != input_hash:
        raise RuntimeError("Input WAL changed while copying")
    deps = root / "target/release/deps"
    compiler = ["rustc", "+1.95.0", "--edition=2024", "-O", "-L", f"dependency={deps}"]
    for name in ("anyhow", "serde", "serde_json", "crc32fast", "sha2", "tracing"):
        paths = list(deps.glob(f"lib{name}-*.rlib"))
        if not paths:
            raise RuntimeError("Run cargo build --release before benchmarking")
        compiler += ["--extern", f"{name}={max(paths, key=lambda p: p.stat().st_mtime)}"]
    template = (root / "scripts/core_benchmark.rs").read_text()
    results = {}
    source_hashes = {}
    for name, source in (("before", args.before), ("after", args.after)):
        snapshot = args.out / name
        snapshot.mkdir()
        source_hashes[name] = {}
        for filename in ("engine.rs", "wal.rs"):
            shutil.copyfile(source / filename, snapshot / filename)
            source_hashes[name][filename] = sha(snapshot / filename)
        harness = snapshot / "bench.rs"
        harness.write_text(template.replace("ENGINE", str((snapshot / "engine.rs").resolve())).replace("WAL", str((snapshot / "wal.rs").resolve())))
        binary = (snapshot / "bench").resolve()
        subprocess.run(compiler + ["--crate-name", f"core_bench_{name}", str(harness), "-o", str(binary)], check=True)
        completed = subprocess.run([str(binary), str(fixed_input.resolve()), str(snapshot.resolve())], text=True, capture_output=True, check=True)
        results[name] = json.loads(completed.stdout)
        (snapshot / "measurements.json").write_text(json.dumps(results[name], indent=2) + "\n")
    check_digest(results)
    if sha(fixed_input) != input_hash or sha(args.input) != input_hash:
        raise RuntimeError("Input WAL changed during benchmarking")
    report = {
        "scope": "isolated microbenchmark; excludes queues, network and live pipeline",
        "input": "input.wal",
        "input_source": str(args.input.resolve()),
        "wal_sha256": input_hash,
        "machine": list(os.uname()),
        "source_hashes": source_hashes,
        "digest": results["after"]["digest"],
        "measurements": results,
        "medians": {},
    }
    for metric in ("engine_us", "wal_read_us", "wal_write_us"):
        before, after = (statistics.median(results[name][metric]) for name in ("before", "after"))
        report["medians"][metric] = {"before_us": before, "after_us": after, "reduction_pct": round((before - after) / before * 100, 2)}
    (args.out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True, help="Directory containing baseline engine.rs and wal.rs")
    parser.add_argument("--after", type=Path, default=ROOT / "src")
    parser.add_argument("--input", type=Path, required=True, help="Fixed WAL, copied into the evidence directory")
    parser.add_argument("--out", type=Path, required=True, help="New self-contained evidence directory")
    report = benchmark(parser.parse_args())
    print(json.dumps(report["medians"], indent=2))


if __name__ == "__main__":
    main()
