#!/usr/bin/env python3
"""Ordered DEFAULT view/unique/IK-compact regression against original tree path.

Uses an existing server only; builds/starts nothing. Snapshot/compare can use the
baseline binary as a second oracle. Large cases intentionally force full replay.
"""
import argparse
import concurrent.futures
import json
from pathlib import Path
import random
import re
import subprocess

from test_tokenizer_binary_json import expression, literal, run, term


def probe_charsets(command, timeout):
    supported = []
    for charset in ("latin1", "gbk"):
        process = subprocess.run(command, input=f"SELECT HEX(CONVERT(X'616263' USING {charset}));\n",
                                 text=True, capture_output=True, timeout=timeout)
        if process.returncode == 0:
            assert process.stdout.strip() == "616263", (charset, process.stdout, process.stderr)
            supported.append(charset)
        elif re.search(r"\bERROR\s+1115\b[^\n]*Unknown\s+character\s+set\b",
                       process.stderr, re.IGNORECASE):
            print(f"SKIP: unsupported charset {charset} (ERROR 1115)", flush=True)
        else:
            raise RuntimeError(f"charset probe {charset} failed: {process.stderr}")
    print("Supported optional charsets: " + (", ".join(supported) or "none"), flush=True)
    return tuple(supported)


def statements(oracle=False, large=True, supported_charsets=("latin1", "gbk")):
    cases = []
    for parser in ("ik", "beng"):
        for collation in ("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci"):
            for text in (None, "", "alpha ALPHA alpha", "À à E e é É ß SS İ i",
                         "数据库 北京大学 𠀀 カタカナ 한국어 😀", "abc\tDEF\nabc\x00ghi",
                         "一百二十三公斤 12万千米 3个 12个月 2026年中国人民银行"):
                cases.append((literal(text), parser, "[]", collation))
        for count in (63, 64, 65, 128, 512, 2048):
            text = " ".join(term(i) for i in range(count))
            cases += [(literal(text), parser, "[]", "utf8mb4_bin"),
                      (literal("tiny"), parser, "[]", "utf8mb4_bin")]
        configs = ['[{"output":"all"}]']
        if parser == "beng":
            configs.append('[{"additional_args":[{"min_token_size":1},{"max_token_size":10}]}]')
        for config in configs:
            cases.append((literal("ABC abc À à 中文 alpha"), parser, config, "utf8mb4_general_ci"))
    for mode in ("smart", "max_word"):
        config = json.dumps([{"additional_args": [{"ik_mode": mode}]}])
        for text in ("研究生命起源 北京大学生 中华人民共和国",
                     "三百二十公斤 12万千米 3个月 一百二十三个 五公斤 2026年",
                     "中国人民银行和中华人民共和国数据库 " * 50):
            cases.append((literal(text), "ik", config, "utf8mb4_bin"))
    for charset, coll, text in (("latin1", "latin1_swedish_ci", "CAFÉ café À à ALPHA alpha"),
                               ("gbk", "gbk_chinese_ci", "ABC abc 数据库 中国人民银行")):
        if charset not in supported_charsets:
            continue
        for parser in ("beng", "space"):
            cases.append((f"CONVERT({literal(text)} USING {charset})", parser, "[]", coll))
    if large:
        # All-unique BENG exceeds the dense collector cap after accepted terms.
        for count in (16384, 16385):
            cases.append((literal(" ".join(term(i) for i in range(count))),
                          "beng", "[]", "utf8mb4_bin"))
            cases.append((literal("tiny"), "beng", "[]", "utf8mb4_bin"))
        # One continuous CJK batch exceeds compact candidates/results. MAX_WORD
        # avoids an intentionally enormous smart-conflict search in the oracle.
        cases.append((literal("中华人民共和国" * 1800), "ik",
                      '[{"additional_args":[{"ik_mode":"max_word"}]}]', "utf8mb4_bin"))
        cases.append((literal("abc " * 262145), "beng", "[]", "utf8mb4_bin"))
    sql = []
    for i, (body, parser, options, coll) in enumerate(cases):
        expr = expression(body, parser, options, coll, oracle)
        if coll.startswith(("latin1_", "gbk_")):
            # Preserve the generic path's bytes even if a legacy non-UTF8
            # parser spelling is not representable as a Python UTF8 JSON value.
            expr = f"JSON_QUOTE(HEX(CAST({expr} AS CHAR CHARACTER SET binary)))"
        sql.append(f"SELECT 'case-{i}', {expr};")
    # Row/LOB lifetimes: input buffers and results survive later evals and sorting.
    texts = [" ".join(term(i) for i in range(512)), "ABC abc 中文", None, "", "tail"]
    body = " UNION ALL ".join(f"SELECT {i} AS id, {literal(t)} AS body" for i, t in enumerate(texts))
    for parser in ("beng", "ik"):
        expr = expression("CONCAT(body, '')", parser, "[]", "utf8mb4_bin", oracle)
        sql.append(f"SELECT CONCAT('row-{parser}-', id), {expr} FROM ({body}) t ORDER BY id DESC;")
    return sql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--timeout", type=int, default=300)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--snapshot", type=Path)
    mode.add_argument("--compare", type=Path)
    args = parser.parse_args()
    command = [args.client, "--no-defaults", "-h127.0.0.1", f"-P{args.port}", "-uroot",
               "-N", "-s", "--default-character-set=utf8mb4", "--max-allowed-packet=64M"]
    supported_charsets = probe_charsets(command, args.timeout)
    actual = run(command, statements(supported_charsets=supported_charsets), args.timeout)
    expected = run(command, statements(True, supported_charsets=supported_charsets), args.timeout)
    assert actual == expected, "ordered view/tree mismatch"
    if args.snapshot:
        args.snapshot.write_text(json.dumps(actual, ensure_ascii=False, sort_keys=True) + "\n")
    if args.compare:
        assert actual == json.loads(args.compare.read_text()), "baseline ordered output mismatch"
    small_sql = statements(large=False, supported_charsets=supported_charsets)
    small_expected = run(command, small_sql, args.timeout)

    def session(seed):
        sql = small_sql[:]
        random.Random(seed).shuffle(sql)
        assert run(command, sql, args.timeout) == small_expected, f"session {seed} mismatch"

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(session, range(8)))
    print(f"PASS: {len(actual)} ordered DEFAULT/ALL results, capacity replay, charsets, 8 sessions")


if __name__ == "__main__":
    main()
