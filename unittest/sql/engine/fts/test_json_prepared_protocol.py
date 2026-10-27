#!/usr/bin/env python3
"""Validate JSON wire values through mysqltest's prepared-statement protocol.

Read-only SQL; use an independent Python JSON oracle, including escaped/large
payloads. Official text-protocol table-format goldens cannot validate PS output.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def literal(text):
    return "CONVERT(X'" + text.encode().hex() + "' USING utf8mb4)"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    mysqltest = Path(args.client).with_name("mysqltest")
    values = [None, True, False, 42, -3.5, "", [], {},
              'quote" slash\\ newline\n tab\t 数据库 😀',
              ["x" * 1024, {"nested": [1, False, None]}],
              {"large": "汉字" * 22000, "tail": "kept"}]
    statements = ["SELECT CAST(" + literal(json.dumps(value, ensure_ascii=False))
                  + " AS JSON);" for value in values]
    for name in ("space", "beng", "ik"):
        statements.append("SELECT TOKENIZE('alpha beta alpha', '" + name + "');")
    script = "--disable_query_log\n--disable_column_names\nSET NAMES utf8mb4;\n" + "\n".join(statements) + "\n"
    with tempfile.TemporaryDirectory(prefix="fts-json-ps-") as directory:
        result = Path(directory) / "json.result"
        process = subprocess.run([str(mysqltest), "--no-defaults", "--ps-protocol",
            "-h127.0.0.1", f"-P{args.port}", "-uroot", "--default-character-set=utf8mb4",
            "--record", "--result-file=" + str(result), "--logdir=" + directory],
            input=script, text=True, capture_output=True, timeout=180)
        if process.returncode:
            raise AssertionError(process.stdout + process.stderr)
        actual = [json.loads(line) for line in result.read_text().splitlines() if line]
    assert len(actual) == len(values) + 3, len(actual)
    assert actual[:len(values)] == values, "prepared JSON wire payload changed"
    for tokens in actual[len(values):]:
        assert sorted(tokens) == ["alpha", "beta"], tokens
    print(f"PASS: {len(actual)} prepared JSON values, including escaped/large payloads and TOKENIZE")


if __name__ == "__main__":
    main()
