#!/usr/bin/env python3
"""Check short binary values decoded from newly built secondary indexes.

Use an existing disposable seekdb server. This script creates a unique database,
populates three tables before creating their indexes, and removes the database.
All expected values and ordering come from Python bytes, independently of SQL
execution. No server parameter changes or global compactions are needed.
"""
import argparse
import random
import subprocess
import uuid


def binary(value):
    return "NULL" if value is None else "X'" + value.hex() + "'"


def values(width, row):
    # Equal non-NULL lengths encourage fixed string-difference encoding. The
    # binary column has more than sixteen different bytes in the changing part;
    # the second column's changing bytes fit the hexadecimal alphabet encoder.
    prefix = (b"\x00\xff\x80\x01\x00\x7f\x81\xfe" * 2)[:max(0, width - 4)]
    mixed = ((row * 0x9e3779b1) & 0xffffffff).to_bytes(4, "big")
    small = ((row * 13) & 0xffff)
    hex_bytes = bytes((small >> (4 * digit)) & 15 for digit in range(4))
    return (None if row % 31 == 0 else (prefix + mixed)[-width:],
            None if row % 29 == 0 else (prefix + hex_bytes)[-width:])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    command = [args.client, "--no-defaults", "-h127.0.0.1", f"-P{args.port}",
               "-uroot", "-N", "-s", "--raw", "--default-character-set=utf8mb4"]
    database = "short_diff_" + uuid.uuid4().hex[:12]
    checks = 0

    def sql(statement):
        result = subprocess.run(command,
                                input="SET ob_query_timeout=1000000000;\n" + statement,
                                text=True, capture_output=True, timeout=args.timeout)
        if result.returncode:
            raise RuntimeError(f"{result.stderr}\nSQL: {statement[:3000]}")
        return result.stdout.splitlines()

    def check_table(table, rows, phase):
        nonlocal checks
        for column, offset in (("b", 0), ("h", 1)):
            index = column + "_idx"
            items = [(key, cell[offset]) for key, cell in rows.items()]
            # NULL sorts first in ascending MySQL binary ordering.
            ordered = sorted(items, key=lambda item: (item[1] is not None, item[1] or b"", item[0]))
            nonnull = [item for item in ordered if item[1] is not None]
            lower, upper = nonnull[61][1], nonnull[-63][1]
            cases = [
                ("", ordered, "ASC", ""),
                ("", list(reversed(ordered)), "DESC", ""),
                ("", ordered[23:60], "ASC", "LIMIT 37 OFFSET 23"),
                (f"WHERE {column} IS NULL", [item for item in ordered if item[1] is None], "ASC", ""),
                (f"WHERE {column}>={binary(lower)} AND {column}<={binary(upper)}",
                 [item for item in ordered if item[1] is not None and lower <= item[1] <= upper], "ASC", ""),
                ("WHERE MOD(id,7)=3", [item for item in ordered if item[0] % 7 == 3], "ASC", ""),
            ]
            for predicate, expected_rows, direction, limit in cases:
                statement = (f"SELECT id,IFNULL(HEX({column}),'NULL') FROM {table} FORCE INDEX({index}) "
                             f"{predicate} ORDER BY {column} {direction},id {direction} {limit};")
                actual = sql(statement)
                expected = [str(key) + "\t" + ("NULL" if cell is None else cell.hex().upper())
                            for key, cell in expected_rows]
                if actual != expected:
                    first = next((i for i, pair in enumerate(zip(actual, expected)) if pair[0] != pair[1]),
                                 min(len(actual), len(expected)))
                    raise AssertionError((phase, table, column, predicate, direction, limit,
                                          "first difference", first, actual[first:first + 3],
                                          expected[first:first + 3], "row counts", len(actual), len(expected)))
                checks += 1

    sql(f"CREATE DATABASE {database};")
    try:
        for width in (1, 7, 16):
            table = f"{database}.w{width}"
            rows = {row: values(width, row) for row in range(512)}
            sql(f"CREATE TABLE {table}(id INT PRIMARY KEY,b VARBINARY({width}),h VARBINARY({width}));")
            order = list(rows)
            random.Random(731 + width).shuffle(order)
            sql(f"INSERT INTO {table} VALUES " + ",".join(
                f"({key},{binary(rows[key][0])},{binary(rows[key][1])})" for key in order) + ";")
            # Building each index after population creates its SSTable; FORCE
            # INDEX below scans the covered (binary value, primary key) columns.
            sql(f"CREATE INDEX b_idx ON {table}(b); CREATE INDEX h_idx ON {table}(h);")
            check_table(table, rows, "major secondary index")
            # Mix fresh versions/deletions with the just-built on-disk index.
            deleted = (0, 7, 31, 127, 255, 511)
            updated = (1, 29, 63, 128, 257, 510)
            changes = [f"DELETE FROM {table} WHERE id IN ({','.join(map(str, deleted))});"]
            for key in deleted:
                del rows[key]
            for key in updated:
                rows[key] = values(width, key + 4007)
                changes.append(f"UPDATE {table} SET b={binary(rows[key][0])},h={binary(rows[key][1])} WHERE id={key};")
            rows[600] = (None, None)
            rows[601] = values(width, 6001)
            changes.append(f"INSERT INTO {table} VALUES (600,NULL,NULL),"
                           f"(601,{binary(rows[601][0])},{binary(rows[601][1])});")
            sql("\n".join(changes))
            check_table(table, rows, "major plus memtable versions")
            print(f"PASS width={width}: binary/hex-alphabet/NULL, major and versioned secondary scans", flush=True)
    finally:
        sql(f"DROP DATABASE IF EXISTS {database};")
    print(f"PASS: {checks} independent Python-byte oracle comparisons across widths 1/7/16")


if __name__ == "__main__":
    main()
