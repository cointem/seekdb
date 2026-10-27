#!/usr/bin/env python3
"""Check actual FTS DDL coordinator use in existing observer diagnostics.

Runs no SQL and starts no processes. Pass INFO logs and, when separate, the
server's redirected stderr. Use one fresh runner per diagnostic environment:
  FTS_BUILD_SHARE_SELFTEST=1                       (normal correctness run)
  FTS_BUILD_SHARE_MAX_BYTES=0                     (budget fallback)
  FTS_BUILD_SHARE_TEST_FAIL_PUBLISH=1              (publication failure)

Those flags affect only optional pure-parse sharing. Posting/score golden
comparisons remain a separate required correctness check in every mode.
"""
import argparse
import json
from pathlib import Path
import re


def value(line, key, required=True):
    match = re.search(r'\b' + re.escape(key) + r'\s*[=:]\s*"?([^,\s)\"]+)', line)
    if not match:
        if required:
            raise AssertionError(f"missing {key}: {line.strip()}")
        return None
    result = match.group(1)
    if result in ("true", "false"):
        return result == "true"
    if re.fullmatch(r"-?[0-9]+", result):
        return int(result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, action="append", required=True)
    parser.add_argument("--mode", choices=("normal", "budget", "publishfail"), default="normal")
    parser.add_argument("--parent-task-id", type=int)
    parser.add_argument("--require-hit", action="store_true")
    parser.add_argument("--require-cross-scn", action="store_true")
    parser.add_argument("--require-selftest", action="store_true")
    args = parser.parse_args()
    records = []
    selftests = []
    counters = ("share_hits", "share_misses", "share_fallbacks", "share_waits", "share_bytes")
    for path in args.log:
        with path.open(errors="replace") as log:
            for line in log:
                if "fts build shared parse self test" in line:
                    selftests.append(bool(value(line, "ok")))
                if "fts build pure parse sharing" not in line:
                    continue
                parent = value(line, "share_parent_task")
                if args.parent_task_id is not None and parent != args.parent_task_id:
                    continue
                row = {key: value(line, key) for key in counters}
                row.update(parent=parent, enabled=bool(value(line, "enabled")),
                           role="word_doc" if value(line, "is_fts_index_aux") else "doc_word",
                           parser=value(line, "parser"),
                           child=value(line, "share_child_task"),
                           snapshot=value(line, "share_snapshot"),
                           base_schema=value(line, "share_base_schema"))
                assert all(row[key] >= 0 for key in counters), row
                records.append(row)
    assert records, "no completed FTS source operator diagnostics found"
    if args.require_selftest:
        assert selftests and all(selftests), f"native domain self-test missing/failed: {selftests}"
    enabled = [row for row in records if row["enabled"]]
    totals = {key: sum(row[key] for row in enabled) for key in counters}
    if args.mode != "normal":
        assert enabled, "fallback diagnostic did not activate a validated coordinator domain"
        assert totals["share_hits"] == 0 and totals["share_bytes"] == 0, totals
        assert totals["share_fallbacks"] > 0, totals
        if args.mode == "publishfail":
            assert totals["share_misses"] > 0, "failure injection never reached a publisher"
    if args.require_hit:
        assert totals["share_hits"] > 0, "no actual sharing hits; inspect metadata guards/schema versions"
    cross_scn = []
    cross_schema = []
    for parent in sorted({row["parent"] for row in enabled}):
        group = [row for row in enabled if row["parent"] == parent]
        for left in group:
            for right in group:
                if (left["role"] != right["role"] and left["parser"] == right["parser"]
                        and left["snapshot"] != right["snapshot"]
                        and left["share_hits"] + right["share_hits"] > 0):
                    cross_scn.append(parent)
                if (left["role"] != right["role"] and left["parser"] == right["parser"]
                        and left["base_schema"] != right["base_schema"]
                        and left["share_hits"] + right["share_hits"] > 0):
                    cross_schema.append(parent)
    if args.require_cross_scn:
        assert cross_scn, "no two-role, different-SCN domain with actual hits found"
    print(json.dumps({"mode": args.mode, "operators": len(records),
                      "enabled_operators": len(enabled), "totals": totals,
                      "cross_scn_parents": sorted(set(cross_scn)),
                      "cross_schema_parents": sorted(set(cross_schema)),
                      "selftests": selftests, "records": records}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
