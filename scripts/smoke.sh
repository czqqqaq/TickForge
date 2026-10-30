#!/usr/bin/env bash
set -euo pipefail

run_dir="${1:-data/smoke-$(date -u +%Y%m%dT%H%M%SZ)}"
sample_seconds="${2:-60}"
yino_bin="${YINO_BIN:-./target/release/yino}"

"$yino_bin" live --out "$run_dir/live" --seconds "$sample_seconds"
"$yino_bin" replay --input "$run_dir/live/raw.wal" --out "$run_dir/replay" --speed 0
"$yino_bin" verify --input "$run_dir/live"
cmp "$run_dir/live/signals.jsonl" "$run_dir/replay/signals.jsonl"
