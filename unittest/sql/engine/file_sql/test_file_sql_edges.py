#!/usr/bin/env python3
"""File SQL lifecycle and relational-oracle regression matrix."""
import argparse
import base64
import csv
import json
import os
from pathlib import Path
import select
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser()
parser.add_argument('--client', required=True)
parser.add_argument('--port', type=int, required=True)
parser.add_argument('--data-dir', required=True)
args = parser.parse_args()
client = [args.client, '--no-defaults', '-h127.0.0.1', f'-P{args.port}', '-uroot', '-Dtest', '-NB']

def literal(value):
    return "'" + str(value).replace("'", "''") + "'"

def sql(query, error=None):
    result = subprocess.run(client + ['-e', 'SET enable_file_sql=ON;' + query], text=True, capture_output=True, timeout=20)
    if error is None:
        assert result.returncode == 0, (query, result.stderr)
    else:
        assert result.returncode and f'ERROR {error} ' in result.stderr, (query, result.stdout, result.stderr)
    return result.stdout.strip()

class Session:
    def __init__(self):
        self.errors = tempfile.TemporaryFile()
        self.process = subprocess.Popen(client + ['--force', '--unbuffered'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.errors)
        self.pending = b''

    def execute(self, query, error=None):
        marker = b'FILE_SQL_AUDIT_END\n'
        offset = self.errors.seek(0, os.SEEK_END)
        self.process.stdin.write((query + ";SELECT 'FILE_SQL_AUDIT_END';\n").encode())
        self.process.stdin.flush()
        deadline = time.monotonic() + 15
        while marker not in self.pending:
            remaining = deadline - time.monotonic()
            assert remaining > 0 and select.select([self.process.stdout], [], [], remaining)[0], 'client response timed out'
            chunk = os.read(self.process.stdout.fileno(), 65536)
            assert chunk, 'client exited before response marker'
            self.pending += chunk
        output, self.pending = self.pending.split(marker, 1)
        self.errors.seek(offset)
        errors = self.errors.read().decode()
        if error is None:
            assert not errors, (query, errors)
        else:
            assert f'ERROR {error} ' in errors, (query, errors)
        return output.decode().strip()

    def close(self):
        self.process.stdin.close()
        self.process.wait(timeout=10)
        self.process.stdout.close()
        self.errors.close()

with tempfile.TemporaryDirectory(prefix='file-sql-edges-', dir=args.data_dir) as temporary:
    root = Path(temporary)
    rows = [(1, 'a', 10.5), (1, 'b', None), (2, 'a', -2), (3, 'b', 0), (None, 'c', 5)]
    other = [(1, 'x'), (1, 'y'), (3, 'z'), (4, 'w'), (None, 'n')]
    a_path, b_path = root / 'a.csv', root / 'b.jsonl'
    with a_path.open('w', newline='') as output:
        writer = csv.writer(output)
        writer.writerow(['id', 'grp', 'score'])
        writer.writerows(rows)
    b_path.write_text(''.join(json.dumps(dict(id=key, label=label)) + '\n' for key, label in other))
    sql('DROP TABLE IF EXISTS file_sql_oracle_a,file_sql_oracle_b;'
        'CREATE TABLE file_sql_oracle_a(id BIGINT,grp VARCHAR(20) COLLATE utf8mb4_bin,score DOUBLE);'
        'CREATE TABLE file_sql_oracle_b(id BIGINT,label VARCHAR(20) COLLATE utf8mb4_bin);'
        "INSERT INTO file_sql_oracle_a VALUES(1,'a',10.5),(1,'b',NULL),(2,'a',-2),(3,'b',0),(NULL,'c',5);"
        "INSERT INTO file_sql_oracle_b VALUES(1,'x'),(1,'y'),(3,'z'),(4,'w'),(NULL,'n')")
    a, b = f'read_csv({literal(a_path)})', f'read_ndjson({literal(b_path)})'
    templates = [
        'SELECT COUNT(*),COUNT(id),COUNT(score),SUM(score),ROUND(AVG(score),6),MIN(score),MAX(score) FROM {a}',
        'SELECT grp,COUNT(*),SUM(score) FROM {a} GROUP BY grp HAVING COUNT(*)>1 ORDER BY grp',
        'SELECT id,grp,score*2 AS doubled FROM {a} WHERE score IS NOT NULL ORDER BY id,grp LIMIT 3',
        'SELECT DISTINCT id FROM {a} ORDER BY id',
        'SELECT a.id,a.grp,b.label FROM {a} a JOIN {b} b ON a.id=b.id ORDER BY a.id,a.grp,b.label',
        'SELECT a.id,a.grp,b.label FROM {a} a LEFT JOIN {b} b ON a.id=b.id ORDER BY a.id,a.grp,b.label',
        'SELECT a.id,a.grp FROM {a} a LEFT JOIN {b} b ON a.id=b.id WHERE b.id IS NULL ORDER BY a.id,a.grp',
        'SELECT COUNT(*) FROM {a} a CROSS JOIN {b} b',
        'SELECT a.id,COUNT(*) FROM {a} a JOIN {a} b ON a.id=b.id GROUP BY a.id ORDER BY a.id',
        'SELECT a.id,b.label FROM {a} a JOIN {b} b ON a.id=b.id WHERE a.score>0 ORDER BY a.id,b.label',
        'SELECT id FROM {a} WHERE id IN (SELECT id FROM {b}) ORDER BY id',
        'SELECT COUNT(*) FROM {a} a WHERE EXISTS(SELECT 1 FROM {b} b WHERE b.id=a.id)',
        'WITH q AS (SELECT id,score FROM {a}) SELECT id,SUM(score) FROM q GROUP BY id ORDER BY id',
        'SELECT id FROM {a} UNION ALL SELECT id FROM {b} ORDER BY id',
        'SELECT score,id FROM {a} ORDER BY id,score',
        'SELECT a.grp,COUNT(*) FROM {a} a LEFT JOIN {b} b ON a.id=b.id OR a.id IS NULL GROUP BY a.grp ORDER BY a.grp',
    ]
    try:
        for template in templates:
            expected = sql(template.format(a='file_sql_oracle_a', b='file_sql_oracle_b'))
            actual = sql(template.format(a=a, b=b))
            assert actual == expected, (template, actual, expected)
        assert sql(f'SELECT COUNT(*) FROM file_sql_oracle_a a JOIN {b} b ON a.id=b.id') == '5'
        parquet_path = root / 'matrix.parquet'
        parquet_path.write_bytes(base64.b64decode((Path(__file__).parent / 'data/matrix.parquet.b64').read_text()))
        parquet = f'read_parquet({literal(parquet_path)})'
        assert sql(f'SELECT COUNT(*),COUNT(score),SUM(score),SUM(flag) FROM {parquet}') == '6\t4\t1.5\t2'
        assert sql(f'SELECT id,HEX(text),day,stamp,flag FROM {parquet} WHERE id IN(1,2,3) ORDER BY id') == (
            '1\t' + '2026-01-01'.encode().hex().upper() + '\t1970-01-01\t1970-01-01 00:00:01.123456\t1\n'
            '2\tNULL\tNULL\tNULL\tNULL\n'
            '3\t' + '中文\n"text"'.encode().hex().upper() + '\t1970-01-02\t1969-12-31 23:59:59.999999\t0')
        assert sql(f'SELECT p.id,b.label FROM {parquet} p JOIN {b} b ON p.id=b.id ORDER BY p.id,b.label') == '1\tx\n1\ty\n3\tz\n4\tw'
        session = Session()
        try:
            prepared = root / 'prepared.csv'
            prepared.write_text('id\n1\n')
            query = f'SELECT * FROM read_csv({literal(prepared)})'
            session.execute('SET enable_file_sql=ON;PREPARE file_scan FROM ' + literal(query))
            assert session.execute('EXECUTE file_scan;EXECUTE file_scan') == '1\n1'
            prepared.write_text('id\nupdated\n')
            assert session.execute('EXECUTE file_scan') == 'updated'
            session.execute('SET enable_file_sql=OFF;EXECUTE file_scan', 1235)
            session.execute('SET enable_file_sql=ON;DEALLOCATE PREPARE file_scan')
        finally:
            session.close()
        for index, options in enumerate(["DELIM ',',DELIMITER ';'", 'HEADER,header false', 'FORMAT CSV,FORMAT CSV']):
            target = root / f'bad-options-{index}.csv'
            before = set(root.iterdir())
            sql(f'COPY (SELECT 1 AS id) TO {literal(target)} ({options})', 1210)
            assert set(root.iterdir()) == before
        for index in range(2):
            target = root / f'distinct-target-{index}.csv'
            sql(f'COPY (SELECT {index} AS id) TO {literal(target)} (HEADER)')
            assert sql(f'SELECT id FROM read_csv({literal(target)})') == str(index)
        malformed = root / 'panic-footer.parquet'
        malformed.write_bytes(b'PAR1\x07\xc6\x01\x03\x00\x00\x00PAR1')
        sql(f'SELECT * FROM read_parquet({literal(malformed)})', 1210)
        assert sql('SELECT 1') == '1', 'server must survive malformed Footer'
        wide = root / 'timeout.csv'
        wide.write_text(','.join(f'c{i}' for i in range(128)) + '\n' + (','.join(['1'] * 128) + '\n') * 20000)
        start = time.monotonic()
        sql(f'SET ob_query_timeout=10000;SELECT COUNT(*) FROM read_csv({literal(wide)})', 4012)
        assert time.monotonic() - start < 3, 'schema inference ignored query cancellation'
        assert sql('SELECT 1') == '1'
    finally:
        sql('DROP TABLE IF EXISTS file_sql_oracle_a,file_sql_oracle_b')
print(f'File SQL edge matrix passed: {len(templates)} relational comparisons plus lifecycle/error cases')
