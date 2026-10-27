#!/usr/bin/env python3
"""Differential DDL/DML checks for reusable full-text parser state.

Uses a unique disposable database. Capture --snapshot on the control binary,
then --compare on the candidate. Compare exact memberships and per-document
scores after building indexes over existing mixed short/long/empty documents.
"""
import argparse
import json
import math
from pathlib import Path
import random
import subprocess
import uuid


def build_corpus():
    rng = random.Random(73119)
    atoms = ['中华人民共和国', '数据库', '索引', '三公斤', '两百万元', '北京大学',
             'alpha', 'ALPHA', 'beta', 'the', 'ABC123', 'ß', 'İ', 'é', 'e\u0301',
             'カタカナ', '한국어', '𠀀', '😀', '，', '123.45', 'Ｅ１２']
    documents = [None, '', ' ', 'the and', 'alpha', '数据库',
                 ('数据库 三公斤 alpha beta 😀 ' * 1800), 'END', '北京大学',
                 ('ALPHA beta ' * 1800), 'short']
    # Force the node pool to grow beyond its retained page, then immediately
    # reuse it for a short document. Letter-only suffixes survive both parsers.
    unique_words = ['pool' + ''.join(chr(97 + (i // (26 ** j)) % 26)
                                     for j in range(3)) for i in range(2048)]
    # Cross several output batch boundaries with unequal token byte lengths,
    # then switch document IDs and immediately empty the next document.
    boundary_words = ['batch' + ''.join(chr(97 + (i // (26 ** j)) % 26)
                                       for j in range(3)) + ('x' * (i % 7))
                      for i in range(2051)]
    documents += [' '.join(boundary_words), '', 'batchtail alpha',
                  ' '.join(unique_words), 'alpha tail',
                  ' '.join(reversed(unique_words)), 'beta']
    documents += [' '.join(rng.choices(atoms, k=rng.randrange(1, 65))) for _ in range(300)]
    documents *= 3
    configs = [('ik_smart', 'ik', 'PARSER_PROPERTIES=(ik_mode="smart")'),
               ('ik_max', 'ik', 'PARSER_PROPERTIES=(ik_mode="max_word")'),
               ('beng', 'beng', ''),
               ('beng_custom', 'beng', 'PARSER_PROPERTIES=(min_token_size=2,max_token_size=20)')]
    queries = ['数据库', '全文索引', '三公斤', '北京大学', '中华人民共和国',
               'alpha', 'ALPHA beta', 'ABC123', '数据库 alpha', 'é', 'absentword']
    queries += [unique_words[0], unique_words[1024], unique_words[-1],
                boundary_words[0], boundary_words[1023], boundary_words[1024],
                boundary_words[2047], boundary_words[-1], 'batchtail']
    return documents, configs, queries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', required=True)
    parser.add_argument('--port', type=int, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--snapshot', type=Path)
    mode.add_argument('--compare', type=Path)
    args = parser.parse_args()
    command = [args.client, '--no-defaults', '-h127.0.0.1', f'-P{args.port}',
               '-uroot', '-N', '-s', '--default-character-set=utf8mb4']
    db = 'fts_reuse_' + uuid.uuid4().hex[:10]

    def sql(statement):
        p = subprocess.run(command, input="SET NAMES utf8mb4; SET sql_mode='NO_AUTO_VALUE_ON_ZERO'; SET ob_query_timeout=1000000000;\n" + statement,
                           text=True, capture_output=True)
        if p.returncode:
            raise RuntimeError(p.stderr)
        return p.stdout

    def literal(value):
        return 'NULL' if value is None else "CONVERT(X'" + value.encode().hex() + "' USING utf8mb4)"

    documents, configs, queries = build_corpus()
    results = {}
    sql(f'CREATE DATABASE {db};')
    try:
        for name, analyzer, props in configs:
            table = f'{db}.{name}'
            sql(f'CREATE TABLE {table}(id BIGINT AUTO_INCREMENT PRIMARY KEY, body LONGTEXT);')
            for start in range(0, len(documents), 100):
                values = ','.join(f'({i},{literal(documents[i])})'
                                  for i in range(start, min(start + 100, len(documents))))
                sql(f'INSERT INTO {table} VALUES {values};')
            for cycle in range(2):
                sql(f'CREATE FULLTEXT INDEX ft ON {table}(body) WITH PARSER {analyzer} {props};')
                statements = []
                for q in queries:
                    match = f'MATCH(body) AGAINST({literal(q)})'
                    statements.append(f'SELECT id, {match} FROM {table} WHERE {match}>0 ORDER BY id; SELECT "END";')
                chunks = sql('\n'.join(statements)).split('END\n')[:-1]
                assert len(chunks) == len(queries)
                for q, chunk in zip(queries, chunks):
                    results[f'{name}/{cycle}/{q}'] = [[int(i), float(score)] for i, score in
                                                     (line.split('\t') for line in chunk.splitlines())]
                if cycle == 0:
                    # A second DDL rebuild sees both updates and deletions, plus
                    # a short document immediately after a very long one.
                    sql(f'UPDATE {table} SET body="new alpha 数据库" WHERE id%17=0;'
                        f'DELETE FROM {table} WHERE id%19=0;'
                        f'INSERT INTO {table} VALUES (10000,"tail beta 北京大学");'
                        f'DROP INDEX ft ON {table};')
            print(f'PASS {name}: two DDL builds over {len(documents)} documents', flush=True)
        # Expanding one source row must preserve its tablet routing on every
        # emitted term. Both partitions contain matching rows; misplaced posting
        # docIDs fail the subsequent docID-to-rowkey lookup (error 4377).
        layouts = ["PARTITION BY HASH(id) PARTITIONS 2",
                   "PARTITION BY RANGE(id) SUBPARTITION BY HASH(id) SUBPARTITIONS 2 "
                   "(PARTITION p0 VALUES LESS THAN(3), PARTITION p1 VALUES LESS THAN(MAXVALUE))"]
        for layout_id, layout in enumerate(layouts):
            table = f'{db}.partitioned_{layout_id}'
            sql(f'CREATE TABLE {table}(id INT PRIMARY KEY, body VARCHAR(4096)) {layout};'
                f"INSERT INTO {table} VALUES (1,'test1'),(2,'test1 test2'),"
                "(3,'test1 test2 test3'),(4,'test1 test2 test3 test4'),"
                "(5,'test1 test2 test3 test4 test5');"
                f'CREATE FULLTEXT INDEX ft ON {table}(body);')
            match = "MATCH(body) AGAINST('test1 test2')"
            actual_ids = sql(f'SELECT id FROM {table} WHERE {match} ORDER BY id;').splitlines()
            assert actual_ids == ['1', '2', '3', '4', '5'], (layout, actual_ids)
            actual_count = sql(f'SELECT COUNT(*) FROM {table} WHERE {match};').strip()
            assert actual_count == '5', (layout, actual_count)
        print('PASS hash/subpartition FTS DDL tablet routing', flush=True)
        if args.snapshot:
            args.snapshot.write_text(json.dumps(results, ensure_ascii=False, sort_keys=True) + '\n')
        else:
            expected = json.loads(args.compare.read_text())
            assert results.keys() == expected.keys()
            for key, actual in results.items():
                reference = expected[key]
                assert [r[0] for r in actual] == [r[0] for r in reference], (key, 'membership')
                for (i, score), (_, ref) in zip(actual, reference):
                    assert math.isclose(score, ref, rel_tol=1e-10, abs_tol=1e-10), (key, i, score, ref)
        print(f'PASS {len(results)} exact membership/score comparisons', flush=True)
    finally:
        sql(f'DROP DATABASE {db};')


if __name__ == '__main__':
    main()
