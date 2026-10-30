#!/usr/bin/env python3
"""Summarize recorded metrics; never alter sample files or acceptance decisions."""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    args = parser.parse_args()
    result = {}
    for path in sorted(args.input.glob('*/summary.json')):
        summary = json.loads(path.read_text())
        resources = [json.loads(line) for line in (path.parent / 'resources.jsonl').read_text().splitlines()]
        resource = {}
        if resources:
            resource = {
                'peak_cpu_percent': max(row['cpu_percent'] for row in resources),
                'peak_rss_bytes': max(row['rss_bytes'] for row in resources),
                'peak_received_per_second': max(row['received_per_second'] for row in resources),
                'peak_processed_per_second': max(row['processed_per_second'] for row in resources),
                'sampled_queue1_max': max(row['queue1'] for row in resources),
                'sampled_queue2_max': max(row['queue2'] for row in resources),
                'final_received': resources[-1]['received'],
                'final_durable': resources[-1]['durable'],
                'final_processed': resources[-1]['processed'],
                'disk_write_bytes': resources[-1]['disk_write_bytes'],
            }
            throughputs = [
                (after['disk_write_bytes'] - before['disk_write_bytes']) * 1000 / (after['elapsed_ms'] - before['elapsed_ms'])
                for before, after in zip(resources, resources[1:])
                if after['elapsed_ms'] > before['elapsed_ms']
            ]
            resource['peak_disk_write_bytes_per_second'] = max(throughputs, default=0)
        result[path.parent.name] = {
            'complete': summary['complete'], 'errors': summary['errors'],
            'mode': summary['mode'], 'elapsed_ms': summary['elapsed_ms'],
            'records': summary['stats']['records'], 'signals': summary['stats']['signals'],
            'digest': summary['stats']['digest'], 'signal_digest': summary['stats']['signal_digest'],
            'drain_ms': summary['drain_ms'], 'backlog_at_input_end': summary['backlog_at_input_end'],
            'sync_calls': summary['sync_calls'],
            'latencies': {key: summary[key] for key in ('persistence', 'sync', 'processing', 'event_age', 'receive_to_decision')},
            'stages': summary['stages'], 'resources': resource,
            'ready_seconds': summary['stats']['activity']['ready_ns'] / 1e9,
            'max_trade_gap_seconds': summary['stats']['activity']['max_trade_gap_ns'] / 1e9,
            'max_book_gap_seconds': summary['stats']['activity']['max_book_gap_ns'] / 1e9,
        }
    text = json.dumps(result, ensure_ascii=False, indent=2) + '\n'
    (args.input / 'metrics.json').write_text(text)
    print(text)


if __name__ == '__main__':
    main()
