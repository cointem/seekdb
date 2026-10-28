#!/usr/bin/env python3
"""Check encoded FTS sort keys and DDL postings against ordinary comparison.

Run against an existing server with --client /path/to/obclient --port PORT.
Requires an exclusive test server: temporarily changes _enable_newsort and
restores its original value. Creates a disposable database; standard library only.
"""
import argparse
import math
import subprocess
import uuid


def literal(value):
    return "NULL" if value is None else "CONVERT(X'" + value.encode().hex() + "' USING utf8mb4)"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    command = [args.client, "--no-defaults", "-h127.0.0.1", f"-P{args.port}",
               "-uroot", "-N", "-s", "--default-character-set=utf8mb4"]

    def sql(statement):
        result = subprocess.run(command, input="SET NAMES utf8mb4; SET ob_query_timeout=1000000000;\n" + statement,
                                text=True, capture_output=True, timeout=180)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout

    def get_newsort():
        values = {line.lower() for line in sql(
            "SELECT value FROM oceanbase.__all_virtual_parameter_stat "
            "WHERE name='_enable_newsort';").splitlines()}
        if len(values) != 1 or not values.issubset({"true", "false"}):
            raise AssertionError(("expected one _enable_newsort value", values))
        return values.pop() == "true"

    def set_newsort(enabled):
        sql(f"ALTER SYSTEM SET _enable_newsort={'true' if enabled else 'false'};")
        if get_newsort() != enabled:
            raise AssertionError("_enable_newsort change was not applied")

    # General CI uses one weight per code point, so include both precomposed
    # accents and combining sequences instead of assuming Unicode normalization.
    words = [None, "", " ", "  ", "a", "A", "a ", "A  ", "à", "À", "á", "Á",
             "e", "é", "É", "e\u0301", "E\u0301", "ß", "ss", "S", "İ", "i",
             "数据库", "北京大学", "𠀀", "😀", "a\0", "a\0b", "a \0", "a\x01",
             "\0", "\0\0", "\0a", " alpha", "alpha beta", "alpha  beta",
             "alpha \x01", "alpha " * 20, "a" * 83, "a" * 84, "a" * 512,
             "a" * 511 + "b", "é" * 84, "数据库" * 84]
    queries = ["alpha", "ALPHA", "à", "á", "e", "é", "e\u0301", "ß", "ss",
               "İ", "i", "数据库", "北京大学", "a" * 83, "a" * 84, "absentword"]
    db = "fts_sort_" + uuid.uuid4().hex[:10]
    original_newsort = get_newsort()
    sql(f"CREATE DATABASE {db};")
    try:
        for collation in ("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci"):
            key_table = f"{db}.keys_{collation}"
            if collation != "utf8mb4_unicode_ci":
                sql(f"CREATE TABLE {key_table}(id INT PRIMARY KEY, "
                    f"word VARCHAR(2048) COLLATE {collation}, doc_id VARBINARY(16));")
                rows = []
                for index, word in enumerate(words * 4):
                    # Match the fixed-size FTS docID domain, including leading,
                    # embedded and trailing NUL. Never substitute its hex text.
                    # Repeated docIDs force comparison of the secondary word
                    # key, as in the forward (docID, word) auxiliary table.
                    doc_id = ((index % 7 * 0x10200).to_bytes(8, "big")
                              + (index // 7 % 3 * 0x200001).to_bytes(8, "big"))
                    rows.append(f"({index},{literal(word)},X'{doc_id.hex()}')")
                sql(f"INSERT INTO {key_table} VALUES " + ",".join(reversed(rows)) + ";")
                for columns in (("word", "doc_id"), ("doc_id", "word")):
                    for direction in ("ASC", "DESC"):
                        order = ",".join(f"{column} {direction}" for column in columns) + ",id"
                        # With an explicit hint, check_can_encode_sortkey reads
                        # the hint directly and skips its type/cardinality policy.
                        # No LIMIT or presorted prefix can select another sorter.
                        def ordered(enabled):
                            hint = "true" if enabled else "false"
                            return sql(f"SELECT /*+ opt_param('enable_newsort','{hint}') */ id "
                                       f"FROM {key_table} ORDER BY {order};").splitlines()
                        expected = ordered(False)
                        actual = ordered(True)
                        if actual != expected:
                            raise AssertionError((collation, order, "encoded order differs", expected, actual))
                print(f"PASS {collation}: encoded/ordinary compound-key order", flush=True)

            # Compare two populated-table DDL builds with the same major-table
            # statistics. Indexed DML also checks membership, but its new rows
            # are in memtables: BM25's major-only average length estimate can
            # differ from DDL even when every posting is identical.
            kinds = ("dml", "ordinary", "encoded")
            tables = [f"{db}.{kind}_{collation}" for kind in kinds]
            docs = [None, "", " ", "alpha ALPHA à À á Á e é É e\u0301 E\u0301",
                    "ß ss S İ i 数据库 北京大学 𠀀 😀", "alpha\0beta alpha\x01beta",
                    "a" * 83 + " " + "a" * 84 + " " + "a" * 85,
                    "数据库 alpha à é " * 400]
            docs += words[4:]
            # Cross the FTS radix eligibility threshold in every DDL collation case.
            docs *= 64
            options = "WITH PARSER space PARSER_PROPERTIES=(min_token_size=1,max_token_size=84)"
            for kind, table in zip(kinds, tables):
                sql(f"CREATE TABLE {table}(id INT PRIMARY KEY, body LONGTEXT COLLATE {collation});")
                if kind == "dml":
                    sql(f"CREATE FULLTEXT INDEX ft ON {table}(body) {options};")
                rows = [f"({key},{literal(doc)})" for key, doc in enumerate(docs)]
                sql(f"INSERT INTO {table} VALUES " + ",".join(rows) + ";")
                if kind != "dml":
                    set_newsort(kind == "encoded")
                    sql(f"CREATE FULLTEXT INDEX ft ON {table}(body) {options};")

            for query in queries:
                results = []
                for table in tables:
                    match = f"MATCH(body) AGAINST({literal(query)})"
                    output = sql(f"SELECT id,{match} FROM {table} WHERE {match}>0 ORDER BY id; "
                                 f"SELECT 'COUNT',COUNT(*) FROM {table} WHERE {match}>0;")
                    lines = output.splitlines()
                    count = int(lines.pop().split("\t")[1])
                    values = [(int(key), float(score))
                              for key, score in (line.split("\t") for line in lines)]
                    if count != len(values):
                        raise AssertionError((collation, table, query, "count differs", count, len(values)))
                    results.append(values)
                dml, expected, actual = results
                expected_ids = [key for key, _ in expected]
                if [key for key, _ in actual] != expected_ids or [key for key, _ in dml] != expected_ids:
                    raise AssertionError((collation, query, "memberships differ", dml, expected, actual))
                for (key, score), (_, reference) in zip(actual, expected):
                    if not math.isclose(score, reference, rel_tol=1e-10, abs_tol=1e-10):
                        raise AssertionError((collation, query, key, "ordinary/encoded DDL scores differ", reference, score))
            print(f"PASS {collation}: ordinary/encoded DDL score/count; DML/DDL membership agree", flush=True)
        print("PASS: FTS encoded sort keys preserve collation and document routing")
    finally:
        try:
            sql(f"DROP DATABASE {db};")
        finally:
            set_newsort(original_newsort)


if __name__ == "__main__":
    main()
