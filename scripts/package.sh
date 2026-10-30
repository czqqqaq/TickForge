#!/usr/bin/env bash
set -euo pipefail

if (( $# == 0 )); then
    echo "Usage: bash scripts/package.sh NEW_OUTPUT_DIR [SAMPLE_DIR ...]" >&2
    exit 2
fi

project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
package_dir="$(realpath -m -- "$1")"
shift
for source_dir in src tests scripts docs; do
    case "$package_dir/" in
        "$project_dir/$source_dir/"*) echo "Output must be outside source directories" >&2; exit 2 ;;
    esac
done
sample_dirs=()
for sample_dir in "$@"; do
    sample_dir="$(realpath -e -- "$sample_dir")"
    test -d "$sample_dir"
    case "$package_dir/" in
        "$sample_dir/"*) echo "Output must be outside each sample directory" >&2; exit 2 ;;
    esac
    sample_dirs+=("$sample_dir")
done
mkdir -- "$package_dir"

source_items=(Cargo.toml Cargo.lock Dockerfile rust-toolchain.toml .gitignore .dockerignore README.md PLAN.MD TASKS.md src tests scripts docs)
(
    cd -- "$project_dir"
    mapfile -d '' source_files < <(find "${source_items[@]}" -type f ! -name '*.pyc' ! -path '*/__pycache__/*' -print0 | sort -z)
    sha256sum -- "${source_files[@]}" > "$package_dir/source-files.sha256"
    printf '%s\0' "${source_files[@]}" | tar --null -T - -czf "$package_dir/source.tar.gz"
)

sample_index=0
for sample_dir in "${sample_dirs[@]}"; do
    sample_index=$((sample_index + 1))
    sample_name="sample-$sample_index"
    printf '%s\t%s\n' "$sample_name.tar.gz" "$(basename -- "$sample_dir")" >> "$package_dir/samples.tsv"
    (
        cd -- "$sample_dir"
        find . -type f -print0 | sort -z | xargs -0 sha256sum > "$package_dir/$sample_name-files.sha256"
        tar --sort=name -czf "$package_dir/$sample_name.tar.gz" .
    )
done
(
    cd -- "$package_dir"
    sha256sum -- *.tar.gz *-files.sha256 > SHA256SUMS
    sha256sum -c SHA256SUMS
)
