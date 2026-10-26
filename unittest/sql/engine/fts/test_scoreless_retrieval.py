#!/usr/bin/env python3
"""Differential SQL coverage for natural-language aggregate hit-only retrieval.

Run against a disposable seekdb instance; creates a uniquely named fts_scoreless_test database and removes only that database.
Uses the scored MATCH > 0 path as an independent execution-path oracle, and
checks membership against a deterministic English corpus as well.
"""
import argparse
import random
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    database = "fts_scoreless_test_" + uuid.uuid4().hex
    command = [args.client, "--no-defaults", "-h127.0.0.1", f"-P{args.port}",
               "-uroot", "-N", "-s", "--default-character-set=utf8mb4"]

    def sql(statement):
        statement = statement.replace("fts_scoreless_test", database)
        result = subprocess.run(command, input="SET NAMES utf8mb4; SET group_concat_max_len=1000000; " + statement,
                                text=True, capture_output=True, check=True)
        return result.stdout.strip()

    def literal(value):
        return "'" + value.replace("'", "''") + "'"

    rng = random.Random(1729)
    words = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]
    docs = {i: rng.choices(words, k=rng.randrange(1, 20)) for i in range(1, 769)}
    values = [f"({i},{literal(' '.join(tokens))})" for i, tokens in docs.items()]
    sql("CREATE DATABASE fts_scoreless_test; "
        "CREATE TABLE fts_scoreless_test.docs(id INT PRIMARY KEY, body TEXT, "
        "FULLTEXT INDEX ft(body) WITH PARSER beng); "
        "INSERT INTO fts_scoreless_test.docs VALUES " + ",".join(values) + ";"
        "INSERT INTO fts_scoreless_test.docs VALUES (800,NULL),(801,''),(802,'the and');")

    def match(query, mode=""):
        return f"MATCH(body) AGAINST({literal(query)} {mode})"

    def verify(query, expected=None, rowset=None):
        predicate = match(query)
        aggregate = "COUNT(*),COALESCE(GROUP_CONCAT(id ORDER BY id),'')"
        hint = "" if rowset is None else f"/*+ OPT_PARAM('rowsets_max_rows', {rowset}) */ "
        prefix = f"SELECT {hint}{aggregate} FROM fts_scoreless_test.docs WHERE "
        actual = sql(prefix + predicate + ";")
        scored = sql(prefix + predicate + " > 0;")
        assert actual == scored, (query, actual, scored)
        if expected is not None:
            fields = actual.split("\t")
            assert int(fields[0]) == len(expected), (query, actual, expected)
            assert (fields[1] if len(fields) > 1 else "") == ",".join(map(str, expected))
        return actual

    docs[802] = ["the", "and"]
    queries = ["", "the and", "absentword", "alpha", "alpha alpha", "ALPHA bravo"]
    queries += [" ".join(rng.choices(words + ["absentword"], k=rng.randrange(1, 9))) for _ in range(32)]
    for query in queries:
        terms = set(query.lower().split())
        expected = [i for i, tokens in docs.items() if terms.intersection(tokens)]
        verify(query, expected)

    # Exercise union state across tiny batches, tail batches and exhausted inputs.
    for rowset in (1, 2, 7, 64, 1024):
        for query in ("", "absentword", "alpha", "alpha bravo", " ".join(words)):
            terms = set(query.split())
            verify(query, [i for i, tokens in docs.items() if terms.intersection(tokens)], rowset)

    base = "SELECT COUNT(*) FROM fts_scoreless_test.docs WHERE "
    assert "calc_relevance=false" in sql("EXPLAIN " + base + match("alpha bravo")), "hit-only plan missing"
    assert "calc_relevance=false" not in sql("EXPLAIN " + base + match("alpha bravo") + " > 0.1"), "threshold needs scores"
    assert "calc_relevance=true" in sql("EXPLAIN " + base + match("+alpha -bravo", "IN BOOLEAN MODE")), "boolean mode needs scores"
    assert "calc_relevance=false" not in sql("EXPLAIN SELECT SUM(" + match("alpha") + ") FROM fts_scoreless_test.docs WHERE " + match("alpha")), "projection needs scores"
    assert "calc_relevance=true" in sql("EXPLAIN SELECT id FROM fts_scoreless_test.docs WHERE " + match("alpha bravo") + " LIMIT 20"), "implicit Top-K needs scores"

    # More than 256 distinct terms selects the existing TAAT fallback.
    long_query = "alpha bravo " + " ".join("term" + str(i) for i in range(300))
    long_matches = [i for i, tokens in docs.items() if set(tokens).intersection({"alpha", "bravo"})]
    verify(long_query, long_matches)

    # Reuse a prepared plan with empty, ordinary and large token lists.
    result = sql("PREPARE p FROM 'SELECT COUNT(*) FROM fts_scoreless_test.docs WHERE MATCH(body) AGAINST(?)'; "
                 "SET @q='alpha'; EXECUTE p USING @q; SET @q=''; EXECUTE p USING @q; "
                 "SET @q=" + literal(long_query) + "; EXECUTE p USING @q; DEALLOCATE PREPARE p;")
    alpha_count = sum("alpha" in tokens for tokens in docs.values())
    assert result.splitlines() == [str(alpha_count), "0", str(len(long_matches))], result

    # DML visibility, including rolled back postings.
    sql("UPDATE fts_scoreless_test.docs SET body='alpha' WHERE id<=10; DELETE FROM fts_scoreless_test.docs WHERE id BETWEEN 11 AND 20;")
    for i in range(1, 11):
        docs[i] = ["alpha"]
    for i in range(11, 21):
        del docs[i]
    verify("alpha bravo", [i for i, tokens in docs.items() if set(tokens).intersection({"alpha", "bravo"})])
    sql("BEGIN; INSERT INTO fts_scoreless_test.docs VALUES(900,'alpha'); ROLLBACK;")
    verify("alpha", [i for i, tokens in docs.items() if "alpha" in tokens])
    sql("DROP DATABASE fts_scoreless_test;")
    print("PASS: 38 membership cases + 25 batch-boundary cases, score-sensitive plans, 302-term fallback, prepared reuse and DML")


if __name__ == "__main__":
    main()
