#!/usr/bin/env python3
"""Summarize SEEKDB_VECTOR_PROFILE records without double-counting snapshots.

Use one observer log per dataset/run. Nested spans are deliberately not added
into a grand total: CPU time can overlap across threads and with GPU work.
"""
import argparse
import csv
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path

PREFIX = '[seekdb_profile] '


def percentile(values, fraction):
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lo, hi = math.floor(position), math.ceil(position)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def parse_records(paths):
    spans, counters, invalid, line_count = [], {}, [], 0
    for path in paths:
        with path.open(errors='replace') as stream:
            for number, line in enumerate(stream, 1):
                line_count += 1
                if PREFIX not in line:
                    continue
                try:
                    record = json.loads(line.split(PREFIX, 1)[1])
                    if record['version'] != 1 or record['kind'] not in ('span', 'counter'):
                        raise ValueError('unsupported record version/kind')
                    if not isinstance(record['stage'], str) or not math.isfinite(record['ms']) or record['ms'] < 0:
                        raise ValueError('invalid stage/duration')
                    for key in ('pid', 'tid', 'start_ns', 'rows', 'dim', 'nq', 'topk', 'rc', 'calls'):
                        if not isinstance(record[key], int):
                            raise ValueError('invalid ' + key)
                    if record['kind'] == 'counter':
                        key = (record['pid'], record['key'], record['stage'], record['start_ns'])
                        # Keep one cumulative snapshot per counter lifetime.
                        previous = counters.get(key)
                        if previous is None or (record['calls'], record['ms']) > (previous['calls'], previous['ms']):
                            counters[key] = record
                    else:
                        spans.append(record)
                except (ValueError, KeyError, TypeError) as error:
                    invalid.append(dict(file=str(path), line=number, error=str(error)))
    return spans + list(counters.values()), invalid, line_count


def summarize(records):
    groups = defaultdict(list)
    for record in records:
        key = (record['stage'], record['kind'], record['rows'] if record['kind'] == 'span' else 0,
               record['dim'], record['nq'], record['topk'])
        groups[key].append(record)
    output = []
    for (stage, kind, rows, dim, nq, topk), group in sorted(groups.items()):
        times = [record['ms'] for record in group]
        output.append(dict(stage=stage, kind=kind, rows_per_span=rows, dim=dim, nq=nq, topk=topk,
                           samples=len(group), calls=sum(x['calls'] for x in group),
                           rows_total=sum(x['rows'] for x in group),
                           failures=sum(x['rc'] != 0 for x in group), total_ms=sum(times),
                           p50_ms=percentile(times, .5), p95_ms=percentile(times, .95),
                           max_ms=max(times)))
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('logs', nargs='+', type=Path)
    parser.add_argument('--output', type=Path, required=True, help='new output directory')
    parser.add_argument('--label', default='unlabelled')
    parser.add_argument('--commit', default='unknown')
    parser.add_argument('--dataset', default='unknown')
    parser.add_argument('--workload', type=Path, help='workload.json for per-case GPU observations')
    parser.add_argument('--bridge', type=Path, help='optional bridge binary for SHA-256 fingerprint')
    args = parser.parse_args()
    if args.output.exists():
        parser.error('output directory already exists; choose a new run directory')
    records, invalid, lines = parse_records(args.logs)
    if not records:
        parser.error('no valid profile records: enable SEEKDB_VECTOR_PROFILE=1 BEFORE starting the instrumented observer and capture stderr')
    phases = summarize(records)
    bridge_hash = None
    if args.bridge:
        digest = hashlib.sha256()
        with args.bridge.open('rb') as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(block)
        bridge_hash = digest.hexdigest()
    report = dict(version=1, label=args.label, commit=args.commit, dataset=args.dataset,
                  bridge_sha256=bridge_hash, inputs=[str(x) for x in args.logs], input_lines=lines,
                  retained_records=len(records), malformed_records=invalid,
                  caveats=['Nested spans overlap: do not sum stages to obtain end-to-end latency.',
                           'Counter times are accumulated across calls/threads; p50/p95 describe counter lifetimes, not individual rows.',
                           'Diagnostic GPU synchronization changes scheduling; measure performance separately with profiling disabled.',
                           'Recall and SQL client wall time must be measured by the workload; they are not inferred from these logs.',
                           'Missing GPU stages mean unobserved GPU execution, not zero GPU time.'], phases=phases)
    snapshots = defaultdict(list)
    for record in records:
        if 'process_peak_rss_kib' in record:
            snapshots[record['pid']].append(record)
    report['process_resources'] = [dict(pid=pid,
        peak_rss_kib=max(x['process_peak_rss_kib'] for x in group),
        observed_user_cpu_ms=max(x['process_user_ms'] for x in group)-min(x['process_user_ms'] for x in group),
        observed_system_cpu_ms=max(x['process_system_ms'] for x in group)-min(x['process_system_ms'] for x in group),
        note='process-wide snapshots over retained records; not exclusive per-stage usage')
        for pid, group in sorted(snapshots.items())]
    if args.workload:
        workload = json.loads(args.workload.read_text())
        observations = []
        for case in workload.get('cases', []):
            begin, end = case.get('build_start_ns', 0), case.get('build_end_ns', 0)
            build_records = [r for r in records if begin <= r['start_ns'] <= end and end > begin]
            begin, end = case.get('query_start_ns', 0), case.get('query_end_ns', 0)
            query_records = [r for r in records if begin <= r['start_ns'] <= end and end > begin]
            observations.append(dict(round=case['round'], lib=case['lib'], table=case['table'],
                gpu_builds_observed=sum(r['stage']=='gpu.cagra_build' and r['rc']==0 for r in build_records),
                first_query_gpu_serves_observed=sum(r['stage']=='route.gpu_served' for r in build_records),
                subsequent_gpu_serves_observed=sum(r['stage']=='route.gpu_served' for r in query_records),
                cpu_fallbacks_observed=sum(r['stage']=='route.cpu_fallback' for r in build_records+query_records)))
        report['workload'] = workload
        report['gpu_observations'] = observations
    args.output.mkdir(parents=True)
    (args.output / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    with (args.output / 'phases.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(phases[0]))
        writer.writeheader()
        writer.writerows(phases)
    with (args.output / 'events.jsonl').open('w') as stream:
        for record in records:
            stream.write(json.dumps(record) + '\n')
    print(f'{"stage":32} {"kind":7} {"calls":>8} {"total_ms":>12} {"p50_ms":>12} {"p95_ms":>12} {"fail":>5}')
    for phase in phases:
        print(f'{phase["stage"]:32} {phase["kind"]:7} {phase["calls"]:8d} {phase["total_ms"]:12.3f} {phase["p50_ms"]:12.3f} {phase["p95_ms"]:12.3f} {phase["failures"]:5d}')
    print('Nested stages are not additive. Read summary.json caveats.')
    if invalid:
        print(f'WARNING: {len(invalid)} malformed records; details in summary.json')
    print(args.output / 'summary.json')


if __name__ == '__main__':
    main()
