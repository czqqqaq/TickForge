"""Process-level regression tests for the acceptance runner (standard library only)."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime


RUNNER = Path(__file__).resolve().parents[1] / 'scripts' / 'acceptance.py'
FAKE = r'''#!/usr/bin/env python3
import json, os, signal, sys, time
from pathlib import Path

command = sys.argv[1]
args = sys.argv[2:]
scenario = os.environ.get('ACCEPTANCE_TEST_SCENARIO', '')
if command in ('live', 'replay'):
    out = Path(args[args.index('--out') + 1])
    out.mkdir(parents=True)
    if scenario.startswith('sleep-') and scenario[6:] == command:
        time.sleep(60)
    if '--persist' in args:
        speed = args[args.index('--speed') + 1]
        if scenario == 'parallel-sleep':
            time.sleep(60)
        if scenario == 'timing':
            time.sleep({'1': .8, '2': .35, '0': .05}[speed])
    records = 2 if scenario == 'stats' and command == 'replay' else 1
    summary = {'stats': {'records': records, 'signals': 0,
                        'activity': {'duration_ns': 900000000000, 'ready_ns': 1}},
               'received': 1, 'durable': 1, 'elapsed_ms': 1,
               'event_age': {'max_us': 1}}
    if scenario == 'counts' and '--persist' in args:
        summary['durable'] = 2
    (out / 'summary.json').write_text(json.dumps(summary))
    (out / 'signals.jsonl').write_text('bad' if scenario == 'signals' and command == 'replay' else '')
    if command == 'live' or '--persist' in args:
        (out / 'raw.wal').write_text('bad' if scenario == 'wal' and command == 'replay' else 'wal')
    if scenario == 'source' and '--persist' in args and speed == '0':
        Path('src/lib.rs').write_text('changed')
    if scenario == 'binary' and '--persist' in args and speed == '0':
        with Path(sys.argv[0]).open('a') as handle:
            handle.write('\n# changed\n')
elif command == 'verify' and scenario == 'sleep-verify':
    time.sleep(60)
elif command == 'report' and scenario == 'sleep-report':
    time.sleep(60)
'''


class AcceptanceRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='yino-acceptance-test-')
        self.root = Path(self.temporary.name)
        (self.root / 'src').mkdir()
        (self.root / 'src/lib.rs').write_text('// fixture\n')
        for name in ('Cargo.toml', 'Cargo.lock', 'Dockerfile', 'rust-toolchain.toml'):
            (self.root / name).write_text('fixture\n')
        self.binary = self.root / 'fake-yino'
        self.binary.write_text(FAKE)
        self.binary.chmod(0o755)
        subprocess.run(['git', 'init', '--quiet'], cwd=self.root, check=True)
        self.children = []

    def tearDown(self):
        for process in self.children:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        self.temporary.cleanup()

    def start(self, scenario='', optimized=False):
        self.binary.write_text(FAKE)
        (self.root / 'src/lib.rs').write_text('// fixture\n')
        out = self.root / f'run-{len(self.children)}'
        command = [sys.executable, *(['-O'] if optimized else []), str(RUNNER),
                   '--out', str(out), '--bin', str(self.binary)]
        environment = dict(os.environ, ACCEPTANCE_TEST_SCENARIO=scenario)
        process = subprocess.Popen(command, cwd=self.root, env=environment,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.children.append(process)
        return process, out

    def complete(self, process, out):
        stdout, stderr = process.communicate(timeout=15)
        return process.returncode, json.loads((out / 'acceptance.json').read_text()), stderr.decode()

    def until(self, function):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            result = function()
            if result:
                return result
            time.sleep(.01)
        self.fail('condition did not become true before timeout')

    def running(self, out, expected):
        try:
            state = json.loads((out / 'acceptance.json').read_text())
        except FileNotFoundError:
            return None
        records = [record for record in state['commands']
                   if record['name'].startswith(expected) and 'pid' in record]
        return records or None

    def test_consistency_checks_are_active_with_and_without_optimization(self):
        for scenario, message in (
            ('stats', 'business results differ'), ('signals', 'signals differ'),
            ('wal', 'persisted WAL differs'), ('counts', 'counts differ'),
            ('source', 'source changed'), ('binary', 'binary changed'),
        ):
            for optimized in (False, True):
                with self.subTest(scenario=scenario, optimized=optimized):
                    process, out = self.start(scenario, optimized)
                    code, state, stderr = self.complete(process, out)
                    self.assertNotEqual(code, 0, stderr)
                    self.assertFalse(state['complete'])
                    self.assertIn(message, state['error'])
                    self.assertIn('finished_at', state)

    def test_signals_reap_children_in_every_phase(self):
        for phase, expected in (('live', 'live'), ('verify', 'verify'),
                                ('report', 'live-report'), ('replay', 'compute')):
            for number in (signal.SIGINT, signal.SIGTERM):
                with self.subTest(phase=phase, signal=number):
                    process, out = self.start(f'sleep-{phase}')
                    records = self.until(lambda: self.running(out, expected))
                    process.send_signal(number)
                    code, state, stderr = self.complete(process, out)
                    self.assertEqual(code, 1, stderr)
                    self.assertFalse(state['complete'])
                    self.assertIn(signal.Signals(number).name, state['error'])
                    for record in records:
                        with self.assertRaises(ProcessLookupError):
                            os.kill(record['pid'], 0)
                    for record in state['commands']:
                        self.assertIsNotNone(record['returncode'])
                        self.assertIn('finished_at', record)

    def test_signal_reaps_every_parallel_replay(self):
        for number in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=number):
                process, out = self.start('parallel-sleep')
                def three_replays():
                    records = self.running(out, 'persist-')
                    return records if records and len(records) == 3 else None
                records = self.until(three_replays)
                process.send_signal(number)
                code, state, stderr = self.complete(process, out)
                self.assertEqual(code, 1, stderr)
                self.assertFalse(state['complete'])
                for record in records:
                    with self.assertRaises(ProcessLookupError):
                        os.kill(record['pid'], 0)

    def test_parallel_exit_timestamps_and_unborn_git_identity(self):
        process, out = self.start('timing')
        code, state, stderr = self.complete(process, out)
        self.assertEqual(code, 0, stderr)
        self.assertTrue(state['complete'])
        self.assertIsNone(state['environment']['git_commit'])
        records = {record['name']: record for record in state['commands']}
        finished = {speed: datetime.fromisoformat(records[f'persist-{speed}']['finished_at'])
                    for speed in (1, 2, 0)}
        self.assertLess(finished[0], finished[2])
        self.assertLess(finished[2], finished[1])
        self.assertGreater((finished[1] - finished[0]).total_seconds(), .5)


if __name__ == '__main__':
    unittest.main()
