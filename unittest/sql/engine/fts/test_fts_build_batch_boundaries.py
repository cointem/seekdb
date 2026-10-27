#!/usr/bin/env python3
"""Verify cross-document FTS DDL batches against explicit IDs and a DDL golden.

Covers batches spanning many short documents, NULL/empty documents, a document
longer than a batch, its reverse, and the final partial batch. No timing or
baseline file is required for membership checks. Use --snapshot on the control
server and --compare on the candidate to compare DDL BM25 scores as well.
"""
import argparse
import math
import json
from pathlib import Path
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--parser', choices=('space', 'beng', 'ik'), default='beng')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--snapshot', type=Path)
    mode.add_argument('--compare', type=Path)
    args = parser.parse_args()
    command = [args.client, '--no-defaults', '-h127.0.0.1', f'-P{args.port}',
               '-uroot', '-N', '-s', '--default-character-set=utf8mb4']
    database = 'fts_batch_' + uuid.uuid4().hex[:12]

    def sql(statement):
        result = subprocess.run(command, input='SET ob_query_timeout=1000000000;\n' + statement,
                                text=True, capture_output=True, timeout=180)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()

    def literal(value):
        return 'NULL' if value is None else "'" + value.replace("'", "''") + "'"

    words = ['bulk' + ''.join(chr(97 + (i // (26 ** j)) % 26) for j in range(3))
             + ('x' * (i % 5)) for i in range(2051)]
    documents = {1: 'alpha ' + ' '.join(words)}
    for row_id in range(2, 1106):
        documents[row_id] = None if row_id % 17 == 0 else '' if row_id % 19 == 0 else 'alpha shortmarker'
    documents.update({1106: 'alpha ' + ' '.join(reversed(words)),
                      1107: '', 1108: 'lastmarker beta', 1109: None})
    ddl_results = {}
    try:
        sql(f'CREATE DATABASE {database};'
            f'CREATE TABLE {database}.built(id BIGINT PRIMARY KEY, body LONGTEXT);'
            f'CREATE TABLE {database}.reference(id BIGINT PRIMARY KEY, body LONGTEXT, '
            f'FULLTEXT INDEX ft(body) WITH PARSER {args.parser});')
        items = list(documents.items())
        for start in range(0, len(items), 100):
            values = ','.join(f'({row_id},{literal(body)})' for row_id, body in items[start:start + 100])
            # The reference receives every posting through ordinary index DML.
            sql(f'INSERT INTO {database}.built VALUES {values};'
                f'INSERT INTO {database}.reference VALUES {values};')
        sql(f'CREATE FULLTEXT INDEX ft ON {database}.built(body) WITH PARSER {args.parser};')
        expected = {
            'alpha': [i for i, body in documents.items() if body and 'alpha' in body.split()],
            'shortmarker': [i for i, body in documents.items() if body == 'alpha shortmarker'],
            words[0]: [1, 1106], words[1023]: [1, 1106], words[1024]: [1, 1106],
            words[2047]: [1, 1106], words[-1]: [1, 1106],
            'lastmarker': [1108], 'beta': [1108], 'absentmarker': [],
        }
        for query, expected_ids in expected.items():
            matched = {}
            for table in ('built', 'reference'):
                match = f'MATCH(body) AGAINST({literal(query)})'
                output = sql(f'SELECT id,{match} FROM {database}.{table} WHERE {match}>0 ORDER BY id;')
                matched[table] = [(int(i), float(score)) for i, score in
                                  (line.split('\t') for line in output.splitlines())]
                assert [i for i, _ in matched[table]] == sorted(expected_ids), (query, table, matched[table])
            # DML and populated-DDL builds can have different major-only BM25
            # average-length statistics. DML is a membership oracle only.
            ddl_results[query] = matched['built']
        golden = {'parser': args.parser, 'queries': ddl_results}
        if args.snapshot:
            args.snapshot.write_text(json.dumps(golden, sort_keys=True) + '\n')
        elif args.compare:
            reference = json.loads(args.compare.read_text())
            assert reference['parser'] == args.parser, ('parser mismatch', reference['parser'], args.parser)
            assert reference['queries'].keys() == ddl_results.keys()
            for query, actual in ddl_results.items():
                expected_rows = reference['queries'][query]
                assert [i for i, _ in actual] == [i for i, _ in expected_rows], (query, 'golden membership')
                for (row_id, score), (_, expected_score) in zip(actual, expected_rows):
                    assert math.isclose(score, expected_score, rel_tol=1e-10, abs_tol=1e-10), \
                        (query, row_id, score, expected_score)
        print(f'PASS {args.parser}: short/NULL/empty/long/final FTS batch membership'
              + (' and golden DDL scores' if args.compare else ''))
    finally:
        sql(f'DROP DATABASE IF EXISTS {database};')


if __name__ == '__main__':
    main()
