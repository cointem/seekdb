#!/usr/bin/env python3
"""Compare the shared file SQL mysqltest case on a local seekdb instance."""

import argparse
import base64
import pathlib
import shutil
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mysqltest', required=True, type=pathlib.Path)
    parser.add_argument('--data-dir', required=True, type=pathlib.Path,
                        help='existing local directory allowed by secure_file_priv')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=12883)
    parser.add_argument('--user', default='root')
    parser.add_argument('--database', default='test')
    parser.add_argument('--log-dir', type=pathlib.Path,
                        help='preserve mysqltest logs and rejection files here')
    args = parser.parse_args()
    repo = pathlib.Path(__file__).resolve().parents[4]
    deploy = repo / 'tools/deploy'
    suite = deploy / 'mysql_test/test_suite/file_sql'
    mysqltest = args.mysqltest.resolve(strict=True)
    data_root = args.data_dir.resolve(strict=True)
    if not data_root.is_dir():
        parser.error('--data-dir must be a directory')
    # The fixture path is substituted into single-quoted SQL by mysqltest eval.
    if any(char in str(data_root) for char in "'\\$;\r\n"):
        parser.error('--data-dir cannot contain quotes, backslashes, $, ; or newlines')
    with tempfile.TemporaryDirectory(prefix='file-sql-mysqltest-', dir=data_root) as temp:
        work = pathlib.Path(temp)
        fixtures = work / 'data'
        fixtures.mkdir()
        for name in ('basic.csv', 'basic.jsonl', 'invalid.txt'):
            shutil.copyfile(suite / 'data' / name, fixtures / name)
        (fixtures / 'simple.parquet').write_bytes(base64.b64decode(
            b''.join((suite / 'data/simple.parquet.b64').read_bytes().split()),
            validate=True))
        logs = args.log_dir.resolve() if args.log_dir else work / 'logs'
        logs.mkdir(parents=True, exist_ok=True)
        test_file = work / 'file_sql_basic.test'
        test_file.write_text(
            f'let $FILE_SQL_DATA_DIR = {fixtures};\n'
            '--source mysql_test/test_suite/file_sql/include/file_sql_queries.inc\n',
            encoding='utf-8')
        command = [
            str(mysqltest), '--no-defaults', f'--host={args.host}',
            f'--port={args.port}', f'--user={args.user}',
            f'--database={args.database}', '--default-character-set=utf8mb4',
            '--max-connect-retries=1', '--connect-timeout=5', '--silent',
            f'--test-file={test_file}',
            f'--result-file={suite / "r/mysql/file_sql_basic.result"}',
            f'--logdir={logs}', f'--tmpdir={work}',
        ]
        result = subprocess.run(command, cwd=deploy, check=False)
        if result.returncode == 0:
            print('file SQL mysqltest comparison passed', flush=True)
        else:
            print(f'file SQL mysqltest failed (exit={result.returncode})', flush=True)
            if not args.log_dir:
                for reject in logs.glob('*.reject'):
                    print(reject.read_text(encoding='utf-8', errors='replace'), flush=True)
        return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
