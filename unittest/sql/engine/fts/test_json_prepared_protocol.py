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
    # Flat string arrays use a dedicated wire serializer. Check every control
    # escape, UTF-8, local-buffer (64 bytes) edges and descriptor/big-payload tails.
    values += [[chr(i) for i in range(32)] + ['"', '\\', '/', '\x7f', '数据库 é 😀 𠀀'],
               ['', ''], ['a'] * 15, ['b'] * 16, ['c'] * 17,
               ['汉字' * 22000], ['\x00' * 1024]]
    values += [['x' * n] for n in (59, 60, 61, 127, 128, 255, 256, 65535, 65536)]
    values += [['prefix'] * 17 + [{'nested': ['tail', None]}]]
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
        raw = [line for line in result.read_text().splitlines() if line]
        actual = [json.loads(line) for line in raw]
    assert len(actual) == len(values) + 3, len(actual)
    assert actual[:len(values)] == values, "prepared JSON wire payload changed"
    for index, value in enumerate(values):
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            assert raw[index] == json.dumps(value, ensure_ascii=False), ("flat JSON wire bytes", index)
    for tokens in actual[len(values):]:
        assert sorted(tokens) == ["alpha", "beta"], tokens
    print(f"PASS: {len(actual)} prepared JSON values, including escaped/large payloads and TOKENIZE")


if __name__ == "__main__":
    main()
