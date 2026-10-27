#!/usr/bin/env python3
"""Check TOKENIZE lease isolation and idle eviction against its unpooled path.

Run against an existing server:
  python3 test_tokenizer_pipeline_concurrency.py --client /path/to/obclient --port PORT

Uses read-only SQL, 24 independent client connections, and the Python standard
library. No server is started. Pool saturation is a scheduling-dependent stress
case; this black-box test does not assert that the fallback branch was reached.
"""
import argparse
import concurrent.futures
import json
import random
import select
import subprocess
import threading


def literal(value):
    if value is None:
        return "CAST(NULL AS CHAR CHARACTER SET utf8mb4)"
    return "CONVERT(X'" + value.encode().hex() + "' USING utf8mb4)"


def make_cases():
    variants = [
        ("ik", {}), ("ik", {"ik_mode": "smart"}),
        ("ik", {"ik_mode": "max_word"}),
        ("beng", {}), ("beng", {"min_token_size": 2, "max_token_size": 20}),
        ("space", {}), ("space", {"min_token_size": 2, "max_token_size": 20}),
        ("ngram", {}), ("ngram", {"ngram_token_size": 1}),
        ("ngram", {"ngram_token_size": 3}),
        ("ngram2", {}), ("ngram2", {"min_ngram_size": 2, "max_ngram_size": 3}),
    ]
    # Long disjoint documents share the capped bucket count. Equal-length tiny
    # documents also reuse a key, then NULL/empty exercise clearing the old map.
    texts = [
        "Alpha alpha À à E e é É ALPHA alpha 中华人民共和国 北京大学 三公斤 " * 400,
        "Omega omega Ù ù A a à À OMEGA omega 数据库全文索引 南京大学 两公斤 " * 400,
        "alpha", "omega", None, "", "Alpha alpha À à E e é É ALPHA alpha",
    ]
    cases = []
    groups = []
    for variant_id, (parser, properties) in enumerate(variants):
        for collation_id, collation in enumerate(
                ("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci")):
            options = [{"output": "all"}] if (variant_id + collation_id) % 2 else []
            if properties:
                options.append({"additional_args": [{k: v} for k, v in properties.items()]})
            # Preserve both compact and spaced raw option strings as pool keys.
            options = json.dumps(options, separators=(",", ":") if variant_id % 2 else None)
            group = []
            for text in texts:
                group.append(len(cases))
                cases.append((text, parser, options, collation))
            groups.append(group)
    return cases, groups


def statement(case_id, case, oracle=False):
    text, parser, options, collation = case
    parser_arg, options_arg = literal(parser), literal(options)
    if oracle:
        # Plain user-variable reads may be constant expressions. Assignment is
        # explicitly nonconstant (T_OP_ASSIGN), so cg_expr selects eval_tokenize
        # instead of eval_reusable, even when running on the candidate binary.
        parser_arg = "CONVERT((@token_oracle_parser := " + parser_arg + ") USING utf8mb4)"
        options_arg = "CONVERT((@token_oracle_options := " + options_arg + ") USING utf8mb4)"
    return (f"SELECT {case_id}, TOKENIZE({literal(text)} COLLATE {collation}, "
            f"{parser_arg}, {options_arg});")


def parse_results(output, expected_ids):
    values = {}
    for line in output.splitlines():
        key, raw = line.split("\t", 1)
        key = int(key)
        if key in values:
            raise AssertionError(f"duplicate result for case {key}")
        value = None if raw == "NULL" else json.loads(raw)
        # Hash-map iteration order is not part of TOKENIZE's result contract.
        # Keep all token metadata, counts and spelling in the comparison.
        if isinstance(value, dict) and "tokens" in value:
            value["tokens"] = sorted(value["tokens"],
                                     key=lambda token: json.dumps(token, sort_keys=True))
        elif isinstance(value, list):
            value = sorted(value)
        values[key] = value
    if set(values) != set(expected_ids):
        raise AssertionError(f"missing/unexpected cases: {set(expected_ids) ^ set(values)}")
    return values


def check_results(actual, expected, label, cases):
    for key, value in actual.items():
        if value != expected[key]:
            text, parser, options, collation = cases[key]
            raise AssertionError(
                f"{label}: case {key}, parser={parser}, options={options}, "
                f"collation={collation}, input_bytes={len(text.encode()) if text else 0}: "
                f"expected {str(expected[key])[:1000]}, got {str(value)[:1000]}")


def run_batch(command, statements, order, timeout, barrier=None):
    header = "SET NAMES utf8mb4; SET ob_query_timeout=1000000000;\n"
    batch = "\n".join(statements[key] for key in order) + "\n"
    if barrier is None:
        result = subprocess.run(command, input=header + batch, text=True,
                                capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return parse_results(result.stdout, order)
    # Establish every connection before releasing the first long query. Each
    # process then consumes one batch, avoiding one client startup per statement.
    with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True) as process:
        try:
            process.stdin.write(header + "SELECT 'TOKENIZER_PIPELINE_READY';\n")
            process.stdin.flush()
            ready, _, _ = select.select([process.stdout], [], [], timeout)
            if not ready or process.stdout.readline().strip() != "TOKENIZER_PIPELINE_READY":
                raise RuntimeError("client failed to establish a ready session")
            barrier.wait(timeout=timeout)
            output, errors = process.communicate(batch, timeout=timeout)
            if process.returncode:
                raise RuntimeError(errors)
            return parse_results(output, order)
        except BaseException:
            barrier.abort()
            process.kill()
            process.communicate()
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--timeout", type=int, default=180, help="seconds per client batch")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    command = [args.client, "--no-defaults", "-h127.0.0.1", f"-P{args.port}",
               "-uroot", "-N", "-s", "--unbuffered", "--default-character-set=utf8mb4"]
    cases, groups = make_cases()
    direct = [statement(key, case) for key, case in enumerate(cases)]
    oracle = [statement(key, case, oracle=True) for key, case in enumerate(cases)]
    order = list(range(len(cases)))
    expected = run_batch(command, oracle, order, args.timeout)
    check_results(run_batch(command, direct, order, args.timeout), expected,
                  "sequential reuse", cases)
    # Visit all 36 configurations before revisiting any one of them. Repeating
    # this with different bucket counts exceeds the 16 idle slots and checks
    # reinitialization after eviction against the unpooled oracle.
    churn = [group[row] for row in range(len(groups[0])) for group in groups]
    check_results(run_batch(command, direct, churn, args.timeout), expected,
                  "idle eviction", cases)
    print(f"PASS: {len(cases)} cases across {len(groups)} configurations; reuse and eviction", flush=True)

    barrier = threading.Barrier(24)

    def worker(seed):
        remaining = list(range(1, len(groups)))
        random.Random(seed).shuffle(remaining)
        ordered = []
        for group_id in [0] + remaining:
            group = groups[group_id][:]
            if seed % 2:
                group[0], group[1] = group[1], group[0]
                group[2], group[3] = group[3], group[2]
            ordered.extend(group)
        # All workers start with the same parser/options/collation/bucket key,
        # but alternating disjoint documents, stressing exclusive active leases.
        actual = run_batch(command, direct, ordered, args.timeout, barrier)
        check_results(actual, expected, f"concurrent session {seed}", cases)

    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
        list(pool.map(worker, range(24)))
    # A final sweep verifies that concurrent release did not poison retained slots.
    check_results(run_batch(command, direct, churn, args.timeout), expected,
                  "after concurrent release", cases)
    print(f"PASS: 24 concurrent sessions agree with {len(cases)} unpooled oracle results")


if __name__ == "__main__":
    main()
