#!/usr/bin/env python3
"""Check direct TOKENIZE binary JSON against the generic tree serializer.

Read-only SQL against an existing server; starts no instances and builds nothing.
--snapshot/--compare additionally compare JSON array order against an older build.
The nonconstant parser assignment selects the original, unpooled tree path.
"""
import argparse
import concurrent.futures
import json
from pathlib import Path
import random
import subprocess


def literal(text):
    if text is None:
        return "CAST(NULL AS CHAR CHARACTER SET utf8mb4)"
    return "CONVERT(X'" + text.encode().hex() + "' USING utf8mb4)"


def term(index, length=12):
    # Alphabet-only distinct terms survive all five parsers' normalizers.
    prefix = "term" + "".join(chr(97 + (index // 26**j) % 26) for j in range(4))
    return prefix + "z" * (length - len(prefix))


def cases():
    result = []
    for parser in ("ik", "beng", "space", "ngram", "ngram2"):
        for text in (None, "", " ", "alpha", "alpha ALPHA beta alpha",
                     'alpha"quote beta\\slash gamma\x00nul delta\x01control',
                     "数据库 北京大学 𠀀 カタカナ 한국어 é ß İ 😀",
                     "abc\tdef\ndef\rghi\x1fjkl"):
            result.append((text, parser, "[]", "utf8mb4_bin"))
        # Exercise the generic ALL fallback alongside DEFAULT after each switch.
        result.append(("Alpha alpha 数据库", parser, '[{"output":"all"}]',
                       "utf8mb4_general_ci"))
    for count in (1, 63, 64, 65, 127, 128, 255, 256, 257, 512, 2048):
        text = " ".join(term(i) for i in range(count))
        result.extend(((text, "space", "[]", "utf8mb4_bin"),
                       ("tiny", "space", "[]", "utf8mb4_bin")))
    # Container size/offset fields widen at 256 and 65536. Construct payloads
    # immediately around those boundaries at the previous field width. Widening
    # itself grows every entry; the resulting binary may jump over the boundary.
    for count, width, boundary in ((3, 1, 256), (1000, 2, 65536)):
        count_width = 1 if count < 256 else 2
        for delta in (-2, -1, 0, 1):
            size = boundary + delta
            overhead = 2 + count_width + width + count * (width + 1) + 2 * count
            total_string_bytes = size - overhead
            base, extra = divmod(total_string_bytes, count)
            assert 8 <= base <= 83
            text = " ".join(term(i, base + (i < extra)) for i in range(count))
            result.append((text, "space", "[]", "utf8mb4_bin"))
    # String vi64 length boundaries. IK has no 84-character word-length filter.
    for size in (63, 64, 127, 128, 129, 255, 256):
        result.append(("q" * size, "ik", "[]", "utf8mb4_bin"))
    for collation in ("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci"):
        result.append(("Alpha alpha À à E e é É ALPHA alpha", "space", "[]", collation))
    return result


def expression(body, parser, options, collation, oracle):
    parser_sql = literal(parser)
    if oracle:
        parser_sql = "CONVERT((@binary_json_parser := " + parser_sql + ") USING utf8mb4)"
    return (f"TOKENIZE(({body}) COLLATE {collation}, {parser_sql}, "
            f"{literal(options)})")


def statements(inputs, oracle):
    result = []
    for index, (text, parser, options, collation) in enumerate(inputs):
        expr = expression(literal(text), parser, options, collation, oracle)
        result.append(f"SELECT '{index}', {expr};")
    # CONCAT carries a TEXT-sized datum from the large literal. The result is
    # then consumed by JSON_EXTRACT and also retained over multirow ORDER BY.
    long_text = " ".join(term(i, 48) for i in range(2048))
    rows = [long_text, "tiny", None, "", long_text.replace("term", "word")]
    for parser in ("space", "beng"):
        body_rows = " UNION ALL ".join(
            f"SELECT {i} AS row_id, {literal(text)} AS body" for i, text in enumerate(rows))
        expr = expression("CONCAT(body, '')", parser, "[]", "utf8mb4_bin", oracle)
        result.append(f"SELECT CONCAT('multi-{parser}-', row_id), {expr} "
                      f"FROM ({body_rows}) token_rows ORDER BY row_id DESC;")
        expr = expression(literal(long_text), parser, "[]", "utf8mb4_bin", oracle)
        result.append(f"SELECT 'extract-{parser}', JSON_ARRAY(JSON_LENGTH({expr}), "
                      f"JSON_EXTRACT({expr}, '$[0]'), JSON_EXTRACT({expr}, '$[2047]'));")
    return result


def run(command, sql, timeout):
    process = subprocess.run(command, input="SET NAMES utf8mb4;\n" + "\n".join(sql),
                             text=True, capture_output=True, timeout=timeout)
    if process.returncode:
        raise RuntimeError(process.stderr)
    result = {}
    for line in process.stdout.splitlines():
        key, raw = line.split("\t", 1)
        if key in result:
            raise AssertionError(f"duplicate result {key}")
        # Preserve array order; binary layout equality is not required.
        result[key] = json.loads(raw)
    expected = sum(5 if "FROM (" in item else 1 for item in sql)
    assert len(result) == expected, (len(result), expected)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--timeout", type=int, default=180)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--snapshot", type=Path)
    mode.add_argument("--compare", type=Path)
    args = parser.parse_args()
    command = [args.client, "--no-defaults", "-h127.0.0.1", f"-P{args.port}", "-uroot",
               "-N", "-s", "--default-character-set=utf8mb4", "--max-allowed-packet=64M"]
    inputs = cases()
    direct_sql = statements(inputs, False)
    actual = run(command, direct_sql, args.timeout)
    expected = run(command, statements(inputs, True), args.timeout)
    assert actual == expected, "direct binary output differs from the tree serializer"
    if args.snapshot:
        args.snapshot.write_text(json.dumps(actual, ensure_ascii=False, sort_keys=True) + "\n")
    if args.compare:
        assert actual == json.loads(args.compare.read_text()), "JSON output differs from snapshot"

    def concurrent_session(seed):
        order = direct_sql[:]
        random.Random(seed).shuffle(order)
        assert run(command, order, args.timeout) == actual, f"session {seed} output differs"

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(concurrent_session, range(8)))
    # Wire conversion must keep every detached JSON payload alive through the
    # complete row, including several JSON cells and subsequent rows. Use an
    # independent Python JSON oracle, not the server's other TOKENIZE path.
    wire_values = [None, True, 42, "", 'quote" slash\\ newline\n tab\t 数据库 😀',
                   [], {}, ["x" * 600, {"nested": [1, False, None]}],
                   {"large": "汉字" * 22000, "tail": "kept"}]
    wire_sql = []
    expected_wire = {}
    for index, value in enumerate(wire_values):
        text = json.dumps(value, ensure_ascii=False)
        wire_sql.append(f"SELECT 'wire-{index}', CAST({literal(text)} AS JSON);")
        expected_wire[f"wire-{index}"] = value
    assert run(command + ["--raw"], wire_sql, args.timeout) == expected_wire, "JSON wire payload changed"
    multi = ["a" * 1024, {"escaped": 'a"b\\c\n'}, [None, True, "数据库"]]
    process = subprocess.run(command + ["--raw"], input="SET NAMES utf8mb4;\nSELECT " +
        ",".join(f"CAST({literal(json.dumps(v, ensure_ascii=False))} AS JSON)" for v in multi) + ";\n",
        text=True, capture_output=True, timeout=args.timeout)
    assert process.returncode == 0, process.stderr
    assert [json.loads(value) for value in process.stdout.strip().split("\t")] == multi
    print(f"PASS: {len(actual)} ordered JSON results match tree oracle; eight sessions agree")


if __name__ == "__main__":
    main()
