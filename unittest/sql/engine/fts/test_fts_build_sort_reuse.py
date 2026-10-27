#!/usr/bin/env python3
"""Compare ordinary/encoded DDL over the exact test_fts_build_reuse corpus.

Requires an exclusive existing test server. Saves complete normalized auxiliary
postings, scores, row-count statistics and EXPLAIN diagnostics to --report, then
restores changed parameters and drops the disposable database. --force-spill
instead compares encoded builds at 64M/2M manual workareas, requiring observed
disk spills for both auxiliary DDL sorts. Does not start a server.
"""
import argparse
import json
import math
from pathlib import Path
import re
import subprocess
import uuid

from test_fts_build_reuse import build_corpus


def literal(value):
    return 'NULL' if value is None else "CONVERT(X'" + value.encode().hex() + "' USING utf8mb4)"


def quoted(value):
    return '`' + value.replace('`', '``') + '`'


def parameter_value(name, value):
    if name == '_sort_area_size':
        match = re.fullmatch(r'(\d+)\s*([KMGT]?)B?', value.upper())
        if match:
            return int(match[1]) * 1024 ** ('', 'K', 'M', 'G', 'T').index(match[2])
    return value.lower()


WORKAREA_COLUMNS = ('plan_id', 'sql_id', 'operation_id', 'db_id', 'total_executions',
                    'onepass_executions', 'multipasses_executions', 'last_execution',
                    'last_tempseg_size', 'max_tempseg_size', 'last_memory_used', 'policy')


def parse_workareas(output):
    rows = {}
    for line in output.splitlines():
        values = line.split('\t')
        if len(values) != len(WORKAREA_COLUMNS):
            raise AssertionError(('invalid workarea history row', line))
        row = dict(zip(WORKAREA_COLUMNS, values))
        for name in WORKAREA_COLUMNS:
            if name not in ('sql_id', 'last_execution', 'policy'):
                row[name] = int(row[name])
        key = tuple(row[name] for name in WORKAREA_COLUMNS[:4])
        if key in rows:
            raise AssertionError(('duplicate workarea key', key))
        rows[key] = row
    return rows


def spill_evidence(before, after, plans, auxiliary, database):
    """Require a newly recorded, disk-writing sort for each actual INSERT target."""
    changed = []
    for key, row in after.items():
        previous = before.get(key, {})
        if row['total_executions'] > previous.get('total_executions', 0):
            item = dict(row)
            item['spill_execution_delta'] = sum(row[name] - previous.get(name, 0)
                                                for name in ('onepass_executions', 'multipasses_executions'))
            changed.append(item)
    evidence = {'new_workareas': changed, 'plans': plans, 'auxiliary_sorts': {}}
    for kind in (13, 14):
        aux_id, aux_name = auxiliary[kind]
        # The SQL must INSERT into this exact auxiliary, rather than merely
        # mentioning its name in a diagnostic SELECT or in the source table.
        target = re.compile(r'\bINTO\s+' + re.escape(quoted(database) + '.' + quoted(aux_name))
                            + r'(?=\s|\()', re.IGNORECASE)
        matched_plans = [plan for plan in plans if target.search(plan['sql'])]
        matches = [row for row in changed if any(
            row['plan_id'] == plan['plan_id'] and row['sql_id'] == plan['sql_id']
            for plan in matched_plans)]
        spills = [row for row in matches if row['spill_execution_delta'] > 0
                  and row['last_tempseg_size'] > 0
                  and row['last_execution'].upper() in ('ONE PASS', 'MULTI-PASS')]
        evidence['auxiliary_sorts'][str(kind)] = {
            'table_id': aux_id, 'table_name': aux_name, 'workareas': matches,
            'spill_observed': bool(spills)}
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--force-spill', action='store_true',
                        help='exclusive server: compare encoded DDL at MANUAL 64M/2M; '
                             'fail unless both auxiliary targets have actual disk-spill evidence')
    parser.add_argument('--parser', choices=('all', 'ik_smart', 'ik_max', 'beng', 'beng_custom'),
                        default='ik_max', help='default: the first previously differing configuration')
    args = parser.parse_args()
    command = [args.client, '--no-defaults', '-h127.0.0.1', f'-P{args.port}',
               '-uroot', '-N', '-s', '--default-character-set=utf8mb4']

    def sql(statement):
        header = ("SET NAMES utf8mb4; SET sql_mode='NO_AUTO_VALUE_ON_ZERO'; "
                  "SET ob_query_timeout=1000000000; SET ob_enable_index_direct_select='ON';\n")
        result = subprocess.run(command, input=header + statement, text=True,
                                capture_output=True, timeout=240)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout

    def parameter(name, value=None):
        if value is not None:
            sql(f"ALTER SYSTEM SET {name}='" + value.replace("'", "''") + "';")
        values = {row for row in sql(
            "SELECT value FROM oceanbase.__all_virtual_parameter_stat "
            f"WHERE name='{name}';").splitlines()}
        if len(values) != 1:
            raise AssertionError(('invalid parameter', name, values))
        actual = values.pop()
        if value is not None and parameter_value(name, actual) != parameter_value(name, value):
            raise AssertionError(('parameter not applied', name, value, actual))
        return actual

    def newsort(enabled=None):
        actual = parameter('_enable_newsort', None if enabled is None else str(enabled).lower()).lower()
        if actual not in ('true', 'false'):
            raise AssertionError(('invalid _enable_newsort', actual))
        actual = actual == 'true'
        if enabled is not None and actual != enabled:
            raise AssertionError(('_enable_newsort not applied', enabled, actual))
        return actual

    documents, configs, queries = build_corpus()
    if args.parser != 'all':
        configs = [config for config in configs if config[0] == args.parser]
    db = 'fts_sort_reuse_' + uuid.uuid4().hex[:10]
    report = {'database': db, 'document_count': len(documents), 'cases': {}, 'differences': []}
    parameter_names = ['_enable_newsort']
    if args.force_spill:
        parameter_names += ['_sort_area_size', 'workarea_size_policy', 'enable_sql_operator_dump']
    originals = {name: parameter(name) for name in parameter_names}
    report['original_parameters'] = originals
    modes = (('memory', True, '64M'), ('spill', True, '2M')) if args.force_spill else (
        ('ordinary', False, None), ('encoded', True, None))
    sql(f'CREATE DATABASE {db};')

    def auxiliary_tables(table_name):
        table_id = int(sql("SELECT table_id FROM oceanbase.__all_table WHERE table_name="
                           + literal(table_name) + ';').strip())
        auxiliary = {}
        for line in sql(f"SELECT index_type,table_id,table_name FROM oceanbase.__all_table "
                        f"WHERE data_table_id={table_id} AND index_type IN (12,13,14);").splitlines():
            kind, aux_id, aux_name = line.split('\t')
            auxiliary[int(kind)] = (int(aux_id), aux_name)
        if set(auxiliary) != {12, 13, 14}:
            raise AssertionError(('missing auxiliary tables', table_name, auxiliary))
        return table_id, auxiliary

    def workareas():
        return parse_workareas(sql('SELECT ' + ','.join(WORKAREA_COLUMNS)
            + " FROM oceanbase.__all_virtual_sql_workarea_history_stat WHERE operation_type='PHY_SORT';"))

    def build_plans(table_name):
        plans = []
        for line in sql("SELECT plan_id,sql_id,HEX(query_sql),HEX(COALESCE(statement,'')) "
                        'FROM oceanbase.__all_virtual_plan_stat '
                        f"WHERE query_sql LIKE 'INSERT%' AND LOCATE({literal(table_name)},query_sql)>0;").splitlines():
            plan_id, sql_id, query_hex, statement_hex = line.split('\t')
            plans.append({'plan_id': int(plan_id), 'sql_id': sql_id,
                          'sql': bytes.fromhex(query_hex).decode(),
                          'statement': bytes.fromhex(statement_hex).decode()})
        return plans

    def postings(table_name):
        table_id, auxiliary = auxiliary_tables(table_name)
        columns = sql(f'SELECT column_name FROM oceanbase.__all_column WHERE table_id={table_id};').splitlines()

        def column(prefix):
            matched = [name for name in columns if name.startswith(prefix)]
            if len(matched) != 1:
                raise AssertionError((table_name, prefix, matched))
            return quoted(matched[0])

        doc_id, word = column('__doc_id'), column('__word_segment')
        word_count, doc_length = column('__word_count'), column('__doc_length')
        mapping_table = quoted(db) + '.' + quoted(auxiliary[12][1])
        visible = "/*+ opt_param('hidden_column_visible','true') */"
        mapping = {}
        for line in sql(f'SELECT {visible} HEX({doc_id}),id FROM {mapping_table};').splitlines():
            encoded_id, key = line.split('\t')
            mapping[encoded_id] = int(key)
        output = {'auxiliary': auxiliary, 'base_table_id': table_id}
        for kind in (13, 14):
            aux_table = quoted(db) + '.' + quoted(auxiliary[kind][1])
            rows = []
            for line in sql(f'SELECT {visible} HEX({doc_id}),HEX({word}),{word_count},{doc_length} '
                            f'FROM {aux_table};').splitlines():
                encoded_id, token, frequency, length = line.split('\t')
                rows.append([mapping[encoded_id], token, int(frequency), int(length)])
            rows.sort()
            output[str(kind)] = rows
            lengths = {}
            for key, _, _, length in rows:
                if key in lengths and lengths[key] != length:
                    raise AssertionError((table_name, kind, key, 'inconsistent document length'))
                lengths[key] = length
            output[f'{kind}_summary'] = {'posting_count': len(rows), 'indexed_documents': len(lengths),
                                         'word_count_sum': sum(row[2] for row in rows),
                                         'document_length_sum': sum(lengths.values())}
        ids = [table_id] + [entry[0] for entry in auxiliary.values()]
        output['optimizer_table_stats'] = sql(
            'SELECT table_id,partition_id,row_cnt,sstable_row_cnt,memtable_row_cnt,macro_blk_cnt,micro_blk_cnt '
            'FROM oceanbase.__all_table_stat WHERE table_id IN (' + ','.join(map(str, ids)) + ');')
        return output

    def capture(name, table_name, cycle, mode):
        table = quoted(db) + '.' + quoted(table_name)
        output = postings(table_name)
        output['base_row_count'] = int(sql(f'SELECT COUNT(*) FROM {table};').strip())
        output['average_if_exact_document_count'] = max(
            10.0, output['13_summary']['word_count_sum'] / output['base_row_count'])
        output['create_table'] = sql(f'SHOW CREATE TABLE {table};')
        statements = []
        for query in queries:
            match = f'MATCH(body) AGAINST({literal(query)})'
            statements.append(f'SELECT id,{match} FROM {table} WHERE {match}>0 ORDER BY id; SELECT "END";')
        chunks = sql('\n'.join(statements)).split('END\n')[:-1]
        if len(chunks) != len(queries):
            raise AssertionError(('missing query results', table_name))
        output['scores'] = {query: [[int(key), float(score)]
                                   for key, score in (line.split('\t') for line in chunk.splitlines())]
                            for query, chunk in zip(queries, chunks)}
        output['explain'] = {}
        for query in ('数据库', 'alpha'):
            match = f'MATCH(body) AGAINST({literal(query)})'
            output['explain'][query] = sql(f'EXPLAIN EXTENDED SELECT id,{match} FROM {table} WHERE {match}>0 ORDER BY id;')
        report['cases'][f'{name}/{cycle}/{mode}'] = output

    try:
        if args.force_spill:
            parameter('workarea_size_policy', 'MANUAL')
            parameter('enable_sql_operator_dump', 'true')
            # Probe support before changing any index or collecting evidence.
            workareas()
            build_plans('__fts_probe_nonexistent_table__')
        for name, analyzer, props in configs:
            for mode, enabled, sort_area in modes:
                newsort(enabled)
                if sort_area:
                    parameter('_sort_area_size', sort_area)
                    report.setdefault('effective_parameters', {})[mode] = {
                        key: parameter(key) for key in parameter_names}
                table_name = f'{db}_{name}_{mode}'
                table = quoted(db) + '.' + quoted(table_name)
                sql(f'CREATE TABLE {table}(id BIGINT AUTO_INCREMENT PRIMARY KEY, body LONGTEXT);')
                for start in range(0, len(documents), 100):
                    values = ','.join(f'({key},{literal(documents[key])})'
                                      for key in range(start, min(start + 100, len(documents))))
                    sql(f'INSERT INTO {table} VALUES {values};')
                for cycle in range(2):
                    before = workareas() if args.force_spill else None
                    sql(f'CREATE FULLTEXT INDEX ft ON {table}(body) WITH PARSER {analyzer} {props};')
                    if args.force_spill:
                        # Snapshot immediately after DDL, before any score or
                        # normalized-postings query can add its own sort stats.
                        after = workareas()
                        _, auxiliary = auxiliary_tables(table_name)
                        evidence = spill_evidence(before, after, build_plans(table_name), auxiliary, db)
                        report.setdefault('workarea_evidence', {})[f'{name}/{cycle}/{mode}'] = evidence
                        if mode == 'spill' and not all(item['spill_observed'] for item in
                                                      evidence['auxiliary_sorts'].values()):
                            raise AssertionError(('actual spill not proven for both auxiliary INSERT targets',
                                                  name, cycle, 'see --report workarea_evidence'))
                    capture(name, table_name, cycle, mode)
                    if cycle == 0:
                        # Identical to the golden test's update/delete/rebuild.
                        sql(f'UPDATE {table} SET body="new alpha 数据库" WHERE id%17=0;'
                            f'DELETE FROM {table} WHERE id%19=0;'
                            f'INSERT INTO {table} VALUES (10000,"tail beta 北京大学");'
                            f'DROP INDEX ft ON {table};')
                    print(f'Captured {name}/{cycle}/{mode}', flush=True)
            for cycle in range(2):
                expected = report['cases'][f'{name}/{cycle}/{modes[0][0]}']
                actual = report['cases'][f'{name}/{cycle}/{modes[1][0]}']
                for kind in ('13', '14'):
                    if actual[kind] != expected[kind]:
                        expected_rows = {tuple(row) for row in expected[kind]}
                        actual_rows = {tuple(row) for row in actual[kind]}
                        report['differences'].append({'case': f'{name}/{cycle}', 'auxiliary': kind,
                                                      'only_' + modes[0][0]: sorted(expected_rows - actual_rows)[:20],
                                                      'only_' + modes[1][0]: sorted(actual_rows - expected_rows)[:20]})
                for query, reference in expected['scores'].items():
                    values = actual['scores'][query]
                    if [row[0] for row in values] != [row[0] for row in reference]:
                        report['differences'].append({'case': f'{name}/{cycle}', 'query': query, 'membership': True})
                    else:
                        differences = [[key, score, ref] for (key, score), (_, ref) in zip(values, reference)
                                       if not math.isclose(score, ref, rel_tol=1e-10, abs_tol=1e-10)]
                        if differences:
                            report['differences'].append({'case': f'{name}/{cycle}', 'query': query,
                                                          'scores': differences})
        if report['differences']:
            raise AssertionError(f"{modes[0][0]}/{modes[1][0]} builds differ; see {args.report}")
        print(f'PASS {len(configs)} configurations: exact postings and strict scores agree', flush=True)
    finally:
        try:
            args.report.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True) + '\n')
        finally:
            try:
                sql(f'DROP DATABASE {db};')
            finally:
                failures = []
                for name, value in reversed(list(originals.items())):
                    try:
                        parameter(name, value)
                    except Exception as error:
                        failures.append(f'{name}: {error}')
                if failures:
                    raise RuntimeError('parameter restoration failed: ' + '; '.join(failures))


if __name__ == '__main__':
    main()
