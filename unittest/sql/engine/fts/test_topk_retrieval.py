#!/usr/bin/env python3
"""Check exact FTS Top-K against complete scored results on a disposable server.

Creates and removes only a uniquely named database. Exercises dense and skewed
postings, ties, LIMIT/OFFSET, batch boundaries, fallback modes and DML visibility.
"""
import argparse
import math
import random
import subprocess
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--compact", action="store_true",
                        help="also compact the disposable server and recheck persisted postings")
    args = parser.parse_args()
    db = "fts_topk_test_" + uuid.uuid4().hex
    command = [args.client, "--no-defaults", "-h127.0.0.1", f"-P{args.port}",
               "-uroot", "-N", "-s", "--default-character-set=utf8mb4"]

    def sql(statement):
        result = subprocess.run(command, input="SET NAMES utf8mb4; " + statement,
                                text=True, capture_output=True)
        if result.returncode:
            raise AssertionError((result.stderr, statement[:1000]))
        return result.stdout.strip()

    def literal(value):
        return "'" + value.replace("'", "''") + "'"

    def rows(output):
        return [(int(i), float(s)) for i, s in
                (line.split("\t") for line in output.splitlines())]

    def compare(reference, actual, k, offset):
        expected = reference[offset:offset + k]
        assert len(actual) == len(expected), ("count", k, offset, len(actual), len(expected))
        assert len({i for i, _ in actual}) == len(actual), "duplicate IDs"
        by_id = dict(reference)
        for (i, score), (_, rank_score) in zip(actual, expected):
            assert i in by_id and math.isclose(score, by_id[i], rel_tol=1e-11, abs_tol=1e-11), ("document score", i, score, by_id.get(i))
            assert math.isclose(score, rank_score, rel_tol=1e-11, abs_tol=1e-11), ("rank score", k, offset, score, rank_score)

    count = 0

    def verify(table, query, limits=((1, 0), (20, 0), (100, 7)), rowset=None, mode="", predicate=None):
        nonlocal count
        expr = f"MATCH(body) AGAINST({literal(query)} {mode})"
        hint = "" if rowset is None else f"/*+ OPT_PARAM('rowsets_max_rows', {rowset}) */ "
        base = f"SELECT {hint}id,{expr} AS s FROM {db}.{table} WHERE " + (predicate or expr) + " ORDER BY s DESC"
        # Refresh both plans so they use the same cardinality estimates after
        # DML/compaction; cached plans may retain different BM25 statistics.
        statements = ["SET ob_enable_plan_cache=false", "BEGIN", base]
        for k, offset in limits:
            statements.extend(["SELECT 'NEXT_CASE'", base + f" LIMIT {k} OFFSET {offset}"])
        statements.append("ROLLBACK")
        outputs = sql(";".join(statements) + ";").split("NEXT_CASE")
        reference = rows(outputs[0].strip())
        for (k, offset), output in zip(limits, outputs[1:]):
            try:
                compare(reference, rows(output.strip()), k, offset)
            except AssertionError as error:
                raise AssertionError((table, query, rowset, mode, predicate, error.args)) from error
            count += 1
        assert len(outputs) == len(limits) + 1

    rng = random.Random(91431)
    words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india"]
    sql(f"CREATE DATABASE {db}")
    try:
        for table in ("dense", "skewed", "ties"):
            sql(f"CREATE TABLE {db}.{table}(id INT PRIMARY KEY,body TEXT,FULLTEXT INDEX ft(body) WITH PARSER beng)")
            values = []
            for i in range(1, 2049):
                if table == "ties":
                    tokens = ["alpha", "bravo", "charlie"]
                elif table == "dense":
                    tokens = rng.choices(words, k=rng.randrange(2, 65))
                else:
                    # Rare terms, disjoint runs, and matches after input refills.
                    tokens = ["commonword"] * (1 + i % 7)
                    if i % 3 == 0:
                        tokens += ["alpha"] * (1 + i % 5)
                    if i > 1200:
                        tokens += ["bravo"] * (1 + i % 11)
                    if i % 29 == 0:
                        tokens += ["charlie", "delta"]
                    if i % 173 == 0:
                        tokens += ["rareword"]
                values.append(f"({i},{literal(' '.join(tokens))})")
                if len(values) == 256:
                    sql(f"INSERT INTO {db}.{table} VALUES " + ",".join(values))
                    values.clear()
            sql(f"INSERT INTO {db}.{table} VALUES(3001,NULL),(3002,'')")

        queries = ["", "absentword", "alpha", "alpha alpha", "alpha bravo", "alpha bravo charlie",
                   "ALPHA bravo absentword", " ".join(words[:8]), " ".join(words)]
        queries += [" ".join(rng.sample(words, rng.randrange(2, 9))) for _ in range(12)]
        for table in ("dense", "skewed", "ties"):
            for query in queries:
                verify(table, query)
            verify(table, "alpha bravo", limits=((0, 0), (1, 17), (20, 150), (4096, 0), (20, 4096)))
        if args.compact:
            before = int(sql("SELECT COALESCE(SUM(frozen_scn),0) FROM oceanbase.DBA_OB_MAJOR_COMPACTION"))
            sql("ALTER SYSTEM MAJOR FREEZE")
            for _ in range(240):
                state = sql("SELECT COALESCE(SUM(frozen_scn),0),COUNT(CASE WHEN last_scn<>frozen_scn THEN 1 END) FROM oceanbase.DBA_OB_MAJOR_COMPACTION").split("\t")
                if int(state[0]) > before and int(state[1]) == 0:
                    break
                time.sleep(0.5)
            else:
                raise AssertionError("major compaction did not finish")
            for table in ("dense", "skewed", "ties"):
                for query in ("alpha", "alpha bravo", "alpha bravo charlie", " ".join(words[:8])):
                    verify(table, query)
        for rowset in (1, 2, 7, 64, 1024):
            verify("dense", "alpha bravo charlie", rowset=rowset)
            verify("skewed", "alpha bravo rareword", rowset=rowset)
        for query in ("+alpha -bravo", "+alpha bravo", "alpha bravo", "+alpha +charlie"):
            verify("dense", query, mode="IN BOOLEAN MODE")
        expr = "MATCH(body) AGAINST('alpha bravo')"
        verify("dense", "alpha bravo", predicate=expr + " > 0.2")
        verify("dense", "alpha bravo", predicate=expr + " AND id % 3 = 0")

        # Same prepared plan sees changing token lists, including the >8 fallback.
        prepared = f"SELECT id,MATCH(body) AGAINST(?) AS s FROM {db}.dense WHERE MATCH(body) AGAINST(?) ORDER BY s DESC LIMIT 20"
        for query in ("", "alpha bravo", " ".join(words), "alpha bravo charlie"):
            expr = f"MATCH(body) AGAINST({literal(query)})"
            base = f"SELECT id,{expr} AS s FROM {db}.dense WHERE {expr} ORDER BY s DESC"
            # Multiple EXECUTEs share the prepared statement within this session.
            script = f"PREPARE p FROM {literal(prepared)}; SET @q='alpha'; EXECUTE p USING @q,@q; SELECT 'NEXT_CASE'; SET @q={literal(query)}; {base}; SELECT 'NEXT_CASE'; EXECUTE p USING @q,@q; DEALLOCATE PREPARE p;"
            outputs = sql(script).split("NEXT_CASE")
            compare(rows(outputs[1].strip()), rows(outputs[2].strip()), 20, 0)
            count += 1

        sql(f"UPDATE {db}.dense SET body='alpha bravo bravo' WHERE id<=300; DELETE FROM {db}.dense WHERE id BETWEEN 301 AND 500")
        verify("dense", "alpha bravo charlie")
        # Uncommitted writes are visible to both full and Top-K scans, then rolled back.
        # verify starts a transaction itself; test the uncommitted state explicitly.
        expr = "MATCH(body) AGAINST('alpha bravo charlie')"
        base = f"SELECT id,{expr} AS s FROM {db}.dense WHERE {expr} ORDER BY s DESC"
        output = sql(f"SET ob_enable_plan_cache=false; BEGIN; INSERT INTO {db}.dense VALUES(4000,'alpha bravo charlie'); {base}; SELECT 'NEXT_CASE'; {base} LIMIT 20; ROLLBACK;").split("NEXT_CASE")
        compare(rows(output[0].strip()), rows(output[1].strip()), 20, 0)
        count += 1
        verify("dense", "alpha bravo charlie")
        print(f"PASS: {count} exact Top-K comparisons across dense/skewed/tied data, batch sizes, fallbacks, prepared reuse and DML")
    finally:
        sql(f"DROP DATABASE {db}")


if __name__ == "__main__":
    main()
