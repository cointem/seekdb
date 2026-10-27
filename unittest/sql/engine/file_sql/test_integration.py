#!/usr/bin/env python3
"""Native SQL regression coverage; run against a local seekdb with fixture access."""
import argparse
import base64
import pathlib
import subprocess
import tempfile

p = argparse.ArgumentParser()
p.add_argument('--client', required=True)
p.add_argument('--port', type=int, default=12883)
p.add_argument('--data-dir', required=True, help='existing directory within secure_file_priv')
a = p.parse_args()
client = [a.client, '--no-defaults', '-h127.0.0.1', f'-P{a.port}', '-uroot', '-Dtest', '-NB']
def sql(query, error=None, enabled=True):
    result = subprocess.run(client + ['-e', ('SET enable_file_sql=ON;' if enabled else '') + query], text=True, capture_output=True)
    if error is not None:
        assert result.returncode and f'ERROR {error} ' in result.stderr, (query, result.stdout, result.stderr)
    else:
        assert result.returncode == 0, (query, result.stderr)
    return result.stdout.strip()
def lit(text):
    return "'" + str(text).replace("'", "''") + "'"
fixtures = pathlib.Path(__file__).resolve().parents[4] / 'tools/deploy/mysql_test/test_suite/file_sql/data'
with tempfile.TemporaryDirectory(prefix='file-sql-', dir=a.data_dir) as temp:
    root = pathlib.Path(temp)
    for name in ['basic.csv', 'basic.jsonl', 'invalid.txt']:
        (root / name).write_bytes((fixtures / name).read_bytes())
    (root / 'simple.parquet').write_bytes(base64.b64decode((fixtures / 'simple.parquet.b64').read_bytes()))
    csv = f'read_csv({lit(root / "basic.csv")})'
    json = f'read_ndjson({lit(root / "basic.jsonl")})'
    parquet = f'read_parquet({lit(root / "simple.parquet")})'
    assert sql('SELECT @@enable_file_sql', enabled=False) == '0'
    sql(f'SELECT * FROM {csv}', 1235, enabled=False)
    assert sql(f'SELECT COUNT(*),SUM(id),ROUND(AVG(score),2),SUM(active) FROM {csv}') == '3\t6\t91.92\t2'
    assert sql(f'SELECT id,name FROM {csv} WHERE score>90 ORDER BY id DESC LIMIT 1') == '3\tChen'
    assert sql(f'SELECT c.id,c.name FROM {csv} c JOIN {json} j ON c.id=j.id WHERE j.active ORDER BY c.id') == '1\tAlice\n3\tChen'
    assert sql(f'SELECT COUNT(*),MIN(c1),MAX(c1),SUM(c1) FROM {parquet}') == '7\t0\t6\t21'
    assert sql(f'SELECT COUNT(*) FROM {parquet}') == '7'
    assert sql(f'SELECT COUNT(*) FROM {csv} a CROSS JOIN {csv} b') == '9'
    sql(f'SELECT missing_column FROM {csv}', 1054)
    schema = sql(f'DESCRIBE SELECT * FROM {csv}').splitlines()
    assert schema[0] == 'id\tBIGINT\tNO\tNULL\tNULL\tNULL', schema
    assert schema[3] == 'active\tBOOLEAN\tNO\tNULL\tNULL\tNULL', schema
    assert schema[4] == 'day\tDATE\tYES\tNULL\tNULL\tNULL', schema
    sql(f'SELECT * FROM read_csv({lit(root / "basic.csv")},sample_size=10)', 1235)
    sql(f'SELECT * FROM read_csv({lit(root / "basic.csv")},\'xml\')', 1064)
    # Binding must inspect the full file even when sample_size is omitted.
    late_csv = root / 'late.csv'
    late_csv.write_text('id\n' + '1\n' * 2048 + 'tail\n')
    late = f'read_csv({lit(late_csv)})'
    assert sql(f'SELECT COUNT(*) FROM {late}') == '2049'
    assert sql(f'SELECT id FROM {late} WHERE id=\'tail\'') == 'tail'
    assert sql(f'DESCRIBE SELECT * FROM {late}') == sql(f'DESCRIBE SELECT * FROM read_csv({lit(late_csv)},sample_size=-1)')
    late_json = root / 'late.jsonl'
    late_json.write_text('{"z":1}\n' * 2048 + '{"a":true,"z":null}\n')
    assert sql(f'SELECT a FROM read_ndjson({lit(late_json)}) WHERE a IS NOT NULL') == '1'
    assert sql(f'DESCRIBE SELECT * FROM read_ndjson({lit(late_json)})').splitlines()[0].startswith('a\tBOOLEAN\tYES\t')
    # A CSV integer beyond i64 must retain its exact text rather than become DOUBLE.
    bounds = root / 'bounds.csv'
    bounds.write_text('id,stamp\n9223372036854775808,2026-01-01 12:13:14.120000\n')
    assert sql(f'SELECT id,stamp FROM read_csv({lit(bounds)})') == '9223372036854775808\t2026-01-01 12:13:14.120000'
    # Reject duplicate aliases for one parameter and parameters for another format.
    for options in ['header=true,HEADER=false', "delim=',',delimiter=';'", 'unknown=true', "path='/other.csv'"]:
        sql(f'SELECT * FROM read_csv({lit(bounds)},{options})', 1210)
    sql(f'SELECT * FROM read_parquet({lit(root / "simple.parquet")},header=true)', 1210)
    invalid_json = root / 'invalid.jsonl'
    for contents in ['{"id":1,"id":2}\n', '{"id":9223372036854775808}\n', '{"id":-9223372036854775809}\n']:
        invalid_json.write_text(contents)
        sql(f'SELECT * FROM read_ndjson({lit(invalid_json)})', 1210)
    # An outer join must keep unmatched rows and null-extend non-null source fields.
    right_json = root / 'right.jsonl'
    right_json.write_text('{"id":1,"label":"matched"}\n')
    right = f'read_ndjson({lit(right_json)})'
    assert sql(f'SELECT c.id FROM {csv} c LEFT JOIN {right} r ON c.id=r.id WHERE r.id IS NULL ORDER BY c.id') == '2\n3'
    assert sql(f'SELECT COUNT(*) FROM {csv} c JOIN {parquet} p ON c.id=p.c1') == '3'
    assert sql(f'SELECT COUNT(*) FROM glob({lit(root / "basic.csv")})') == '1'
    assert sql(f'SELECT COUNT(*) FROM glob({lit(root / "no-match-*.csv")})') == '0'
    assert sql(f'SELECT COUNT(*) FROM glob({lit(root / "missing-dir/*.csv")})') == '0'
    # File-dependent bindings must not survive into a new statement's cached plan.
    changing_glob = f'SELECT COUNT(*) FROM glob({lit(root / "fresh-*.csv")})'
    assert sql(changing_glob) == '0'
    assert sql(changing_glob) == '0'
    changing_file = root / 'fresh-data.csv'
    changing_file.write_text('id\n1\n')
    assert sql(changing_glob) == '1', 'glob reused a stale file list'
    changing_query = f'SELECT * FROM read_csv({lit(changing_file)})'
    assert sql(changing_query) == '1'
    assert sql(changing_query) == '1'
    changing_file.write_text('id\nupdated\n')
    assert sql(changing_query) == 'updated', 'new statement reused a stale schema'
    assert sql('SELECT 1+1') == '2'
    output = root / 'export.csv'
    sql(f'COPY (SELECT id,name,score,note FROM {csv} ORDER BY id) TO {lit(output)} (FORMAT csv,HEADER,NULLSTR \'NULL\')')
    assert sql(f'SELECT id,name,score FROM read_csv({lit(output)}) ORDER BY id') == '1\tAlice\t98.5\n2\tBob\t87\n3\tChen\t90.25'
    before = output.read_bytes()
    sql(f'COPY (SELECT 1) TO {lit(output)} (FORMAT csv)', 1210)
    assert output.read_bytes() == before
    failed_output = root / 'cancelled.csv'
    before_files = set(root.iterdir())
    sql(f'SET ob_query_timeout=100000;COPY (SELECT id,SLEEP(1) AS delay FROM {csv}) TO {lit(failed_output)} (HEADER)', 4012)
    assert set(root.iterdir()) == before_files, 'cancelled COPY left output or temporary files'
    sql('DROP TABLE IF EXISTS file_sql_copy_test;CREATE TABLE file_sql_copy_test(id BIGINT, name VARCHAR(50));INSERT INTO file_sql_copy_test VALUES(1,\'x\'),(2,\'y\')')
    table_output = root / 'table.csv'
    sql(f'COPY file_sql_copy_test TO {lit(table_output)} (HEADER)')
    assert sql(f'SELECT SUM(id) FROM read_csv({lit(table_output)})') == '3'
    sql('DROP TABLE file_sql_copy_test')
print('file SQL integration checks passed')
