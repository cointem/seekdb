#!/usr/bin/env python3
"""DEFAULT builtin IK AC/frame equivalence against the original DAT/tree path.

Run on an existing server. --snapshot/--compare supports an older binary oracle.
For cold-budget fallback, restart the SAME candidate with FTS_IK_AC_MAX_BYTES=0
and rerun --compare; no SQL result depends on acceleration availability.
"""
import argparse
import concurrent.futures
import json
from pathlib import Path
import random
import subprocess

from test_tokenizer_binary_json import expression, literal, run


def statements(oracle=False):
    seeds = ["研究生命起源", "北京大学生", "中华人民共和国", "中国人民银行",
             "长江大桥南京市长", "一百二十三公斤", "12万千米 3个月 2026年",
             "Alpha１２３ＡＢＣ abc-123 1,234.5", "一一对应 一一列举 一丁点儿",
             "数据库管理系统 数据库管理 数据库", "カタカナ 한국어 𠀀 😀 é ß İ"]
    rng = random.Random(71429)
    texts = [None, "", " ", "一", "中国", "北京大学", "12个月", "三百公斤"] + seeds
    for _ in range(60):
        texts.append(rng.choice(("", " ", ",", "。", "\t", "\x00")).join(
            rng.choice(seeds) for _ in range(rng.randint(1, 5))))
    # Multiple legacy SEGMENT_LIMIT batches, root resets and both sides of the
    # bounded decoded-frame input limit; retain a short doc after every long doc.
    texts += ["研究生命起源 北京大学生 " * 180, "tail", "q" * 65537, "tail",
              "中国人民银行 " * 5000, "tail"]
    sql = []
    for smart in ("smart", "max_word"):
        options = json.dumps([{"additional_args": [{"ik_mode": smart}]}])
        for coll in ("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci"):
            for index, text in enumerate(texts):
                expr = expression(literal(text), "ik", options, coll, oracle)
                sql.append(f"SELECT '{smart}-{coll}-{index}', {expr};")
    # Keep the original 512 result keys stable. Extra deferred-group cases have
    # their own key prefix, so the existing baseline snapshot remains usable.
    deferred_texts = ["alpha beta gamma 数据库 银行", "abc123xyz", "abc-123xyz",
              "北京大学数据库中国人民银行", "南京市长江大桥研究生命起源",
              "q 北京大学生 q 一百公斤 q 12个月 q カタカナ 한국어",
              "---", "alpha", "12个月", "研究生命起源 " * 24,
              "q " * 1100 + "12万千米 北京大学生 一百公斤",
              "北京大学生 一百公斤 " + "q " * 1100 + "tail"]
    for smart in ("smart", "max_word"):
        options = json.dumps([{"additional_args": [{"ik_mode": smart}]}])
        for coll in ("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci"):
            for index, text in enumerate(deferred_texts):
                expr = expression(literal(text), "ik", options, coll, oracle)
                sql.append(f"SELECT 'deferred-{smart}-{coll}-{index}', {expr};")
    # Explicit builtin names remain eligible; arbitrary custom names must use
    # the old pipeline even where the current implementation ignores that name.
    for dictionary in ("oceanbase.__ft_dict_ik_utf8", "unresolved_custom_dictionary"):
        options = json.dumps([{"additional_args": [{"dict_table": dictionary}, {"ik_mode": "smart"}]}])
        expr = expression(literal("北京大学生 一百公斤 Alpha"), "ik", options, "utf8mb4_bin", oracle)
        sql.append(f"SELECT 'dictionary-{dictionary}', {expr};")
    return sql


def malformed(command, oracle, timeout):
    results = []
    for encoded in ("e4", "c080", "f0808080", "eda080", "f4908080", "e4b8ade4"):
        expr = expression(f"CAST(X'{encoded}' AS CHAR CHARACTER SET utf8mb4)",
                          "ik", "[]", "utf8mb4_bin", oracle)
        sql = f"SET NAMES utf8mb4; SELECT HEX(CAST({expr} AS CHAR CHARACTER SET binary));\n"
        proc = subprocess.run(command, input=sql.encode(), capture_output=True, timeout=timeout)
        # Keep original success/error behavior. stderr contains client line
        # numbers, but both forms use the same single-line SQL here.
        results.append((proc.returncode, proc.stdout, proc.stderr))
    return results


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
    actual = run(command, statements(), args.timeout)
    assert actual == run(command, statements(True), args.timeout), "AC/frame differs from DAT oracle"
    assert malformed(command, False, args.timeout) == malformed(command, True, args.timeout), "invalid encoding behavior changed"
    if args.snapshot:
        args.snapshot.write_text(json.dumps(actual, ensure_ascii=False, sort_keys=True) + "\n")
    if args.compare:
        reference = json.loads(args.compare.read_text())
        if not any(key.startswith("deferred-") for key in reference):
            original = {key: value for key, value in actual.items() if not key.startswith("deferred-")}
            assert original == reference, "original baseline ordered output differs"
            print(f"Baseline: {len(reference)} original keys; extra deferred keys checked against DAT oracle")
        else:
            assert actual == reference, "baseline ordered output differs"
    small_sql = [sql for sql in statements() if len(sql) < 12000]
    expected = run(command, small_sql, args.timeout)

    def session(seed):
        shuffled = small_sql[:]
        random.Random(seed).shuffle(shuffled)
        assert run(command, shuffled, args.timeout) == expected, f"session {seed} differs"

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(session, range(8)))
    print(f"PASS: {len(actual)} ordered AC/DAT results; invalid encodings; eight sessions")


if __name__ == "__main__":
    main()
