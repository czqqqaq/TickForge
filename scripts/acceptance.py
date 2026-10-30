#!/usr/bin/env python3
"""Record a real sample and compare compute/persisted replays without overwriting data."""
import argparse
import hashlib
import json
import os
import platform
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


POLL_SECONDS = 0.02


def utc():
    return datetime.now(timezone.utc).isoformat()


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--bin', type=Path, default=Path('target/release/yino'))
    parser.add_argument('--seconds', type=int, default=900)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    binary = args.bin.resolve()
    receipts = []
    jobs = []
    state = {'started_at': utc(), 'commands': receipts, 'complete': False,
             'exit_poll_interval_ms': int(POLL_SECONDS * 1000)}
    status = args.out / 'acceptance.json'
    stopped_by = None

    def request_stop(number, _frame):
        nonlocal stopped_by
        stopped_by = number

    handlers = {number: signal.signal(number, request_stop)
                for number in (signal.SIGINT, signal.SIGTERM)}

    def check_stop():
        if stopped_by is not None:
            raise RuntimeError(f'acceptance interrupted by {signal.Signals(stopped_by).name}')

    def save():
        temporary = status.with_suffix('.tmp')
        temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + '\n')
        temporary.replace(status)

    def refresh(persist=True):
        changed = False
        for process, record in jobs:
            if record['returncode'] is None and process.poll() is not None:
                record['returncode'] = process.returncode
                # This is when the parent observes exit, within the poll interval.
                record['finished_at'] = utc()
                changed = True
        if changed and persist:
            save()

    def launch(name, *command):
        check_stop()
        record = {'name': name, 'args': [str(value) for value in command],
                  'started_at': utc(), 'returncode': None}
        receipts.append(record)
        with (args.out / f'{name}.stdout').open('w') as stdout, \
                (args.out / f'{name}.stderr').open('w') as stderr:
            process = subprocess.Popen([str(binary), *map(str, command)],
                                       stdout=stdout, stderr=stderr, start_new_session=True)
            # Register before closing handles or performing any further I/O.
            # The signal handler only sets a flag during this critical section.
            jobs.append((process, record))
        record['pid'] = process.pid
        save()
        check_stop()
        return record

    def wait(record):
        while True:
            check_stop()
            refresh()
            for _, finished in jobs:
                if finished['returncode'] not in (None, 0):
                    raise RuntimeError(f'{finished["name"]} failed; inspect its stderr and sample summary')
            if record['returncode'] is not None:
                return
            time.sleep(POLL_SECONDS)

    def stop_children():
        for process, _ in jobs:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, stopped_by or signal.SIGTERM)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 10
        while any(process.poll() is None for process, _ in jobs) and time.monotonic() < deadline:
            refresh(persist=False)
            time.sleep(POLL_SECONDS)
        for process, _ in jobs:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.wait()
        refresh(persist=False)

    try:
        save()
        files = sorted([*Path('src').rglob('*.rs'), Path('Cargo.toml'), Path('Cargo.lock'),
                        Path('Dockerfile'), Path('rust-toolchain.toml')])
        state['source_sha256'] = {str(path): sha(path) for path in files}
        state['binary_sha256'] = sha(binary)
        git = subprocess.run(['git', 'rev-parse', '--verify', 'HEAD'], capture_output=True, text=True)
        state['environment'] = {'platform': platform.platform(), 'cpu_count': os.cpu_count(),
                                'git_commit': git.stdout.strip() if git.returncode == 0 else None}
        state['parallel_replay_note'] = '1x, 2x and unlimited persisted replays run concurrently as a shared-host pressure test; these latency measurements are not isolated benchmarks'
        check_stop()
        save()
        live = args.out / 'live'
        wait(launch('live', 'live', '--out', live, '--seconds', args.seconds))
        wait(launch('verify', 'verify', '--input', live))
        wait(launch('live-report', 'report', '--input', live))
        compute = args.out / 'compute'
        wait(launch('compute', 'replay', '--input', live / 'raw.wal', '--out', compute))
        parallel = [launch(f'persist-{speed}', 'replay', '--input', live / 'raw.wal',
                           '--out', args.out / f'persist-{speed}', '--persist', '--speed', speed)
                    for speed in (1, 2, 0)]
        for record in parallel:
            wait(record)
        expected = json.loads((live / 'summary.json').read_text())
        comparisons = []
        for name in ('compute', 'persist-1', 'persist-2', 'persist-0'):
            check_stop()
            out = args.out / name
            actual = json.loads((out / 'summary.json').read_text())
            require(actual['stats'] == expected['stats'], f'{name}: business results differ')
            require(sha(out / 'signals.jsonl') == sha(live / 'signals.jsonl'), f'{name}: signals differ')
            if name.startswith('persist'):
                require(sha(out / 'raw.wal') == sha(live / 'raw.wal'), f'{name}: persisted WAL differs')
                require(actual['received'] == actual['durable'] == actual['stats']['records'],
                        f'{name}: received/durable/processed counts differ')
            comparisons.append({'name': name, 'matched': True, 'records': actual['stats']['records'],
                                'signals': actual['stats']['signals'], 'elapsed_ms': actual['elapsed_ms'],
                                'event_age_max_us': actual['event_age']['max_us']})
            wait(launch(name + '-report', 'report', '--input', out))
        require({str(path): sha(path) for path in files} == state['source_sha256'],
                'source changed after sample began')
        require(sha(binary) == state['binary_sha256'], 'binary changed after sample began')
        state['comparisons'] = comparisons
        state['sample_seconds'] = expected['stats']['activity']['duration_ns'] / 1e9
        state['formal_sample'] = args.seconds >= 900 and state['sample_seconds'] >= 899 and expected['stats']['activity']['ready_ns'] > 0
        state['files_sha256'] = {str(path.relative_to(args.out)): sha(path) for path in sorted(args.out.rglob('*')) if path.is_file() and path != status}
        check_stop()
        state['complete'] = True
        state['finished_at'] = utc()
        save()
    except BaseException as error:
        # Covers unexpected Python exceptions as well as operator interruption.
        # All phases share one task table, including live, verify and reports.
        stop_children()
        state['complete'] = False
        state['error'] = str(error)
        state['finished_at'] = utc()
        save()
        raise
    finally:
        for number, handler in handlers.items():
            signal.signal(number, handler)


if __name__ == '__main__':
    main()
