#!/usr/bin/env python3
"""Run against a dedicated seekdb instance and mock_service.py on port 8000."""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path


def quote(value):
    return "'" + value.replace('\\', '\\\\').replace("'", "''") + "'"


def run(args):
    command = [args.client, '--no-defaults', '-h127.0.0.1', '-P' + str(args.port), '-uroot', '-N', '-B', '--raw', 'test']

    def sql(statement, error=False):
        result = subprocess.run(command, input=statement, text=True, capture_output=True)
        if error:
            assert result.returncode != 0, statement
        else:
            assert result.returncode == 0, result.stderr + '\n' + statement
        return result.stdout.strip()

    def endpoint(name, prefix=''):
        path = '/v1/rerank' if name == 'rank' else '/compatible-mode/v1/' + ('embeddings' if name == 'embed' else 'chat/completions')
        config = json.dumps({'url': 'http://127.0.0.1:8000/' + prefix + path.lstrip('/')})
        sql(f"call DBMS_AI_SERVICE.ALTER_AI_MODEL_ENDPOINT('test_{name}_ep', {quote(config)});")

    for name, kind, provider in [('complete', 'completion', 'openai'), ('embed', 'dense_embedding', 'openai'), ('rank', 'rerank', 'siliconflow')]:
        sql(f"call DBMS_AI_SERVICE.CREATE_AI_MODEL('test_{name}', {quote(json.dumps({'type': kind, 'model_name': 'fixture'}))});")
        config = {'ai_model_name': 'test_' + name, 'scope': 'all', 'url': 'http://127.0.0.1:8000/', 'access_key': 'fixture', 'provider': provider}
        sql(f"call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT('test_{name}_ep', {quote(json.dumps(config))});")
        endpoint(name)
    try:
        assert sql("select ai_complete('test_complete', ai_prompt('{0}/{0}/{1}', '你好', ai_prompt('x{0}', 'y')), null);") == '你好/你好/xy'
        result = sql("select ai_complete('test_complete', ai_prompt('{0}', json_object('text','你好','array',json_array(1,true))));")
        assert json.loads(result) == {'text': '你好', 'array': [1, True]}
        sql("select ai_complete('test_complete', ai_prompt('{999999999999999999999999999999}', 'x'));", error=True)
        sql("select ai_complete('test_complete', ai_prompt('{2}', 'x'));", error=True)
        sql("create table test_ai_lob(t longtext, a longtext);")
        sql("insert into test_ai_lob values('{0}{0}', repeat('abcdef', 20000));")
        assert sql("select length(ai_complete('test_complete', ai_prompt(t,a))) from test_ai_lob;") == '240000'
        endpoint('complete', 'empty/')
        sql("select ai_complete('test_complete', 'empty');", error=True)
        endpoint('complete')
        for dimension in ['0', '-1', '2.5', 'null']:
            sql(f"select ai_embed('test_embed', 'x', {dimension});", error=True)
        sql('create table test_ai_batch(id int primary key, content text);')
        rows = [(i, None if i % 9 == 0 else 'row-' + str(i)) for i in range(300)]
        sql('insert into test_ai_batch values' + ','.join(f'({i},{"null" if v is None else quote(v)})' for i, v in rows) + ';')
        before = len(args.request_log.read_text().splitlines())
        result = sql("select id, ai_embed('test_embed',content,4) from test_ai_batch where id >= 7 order by id;")
        for line, (i, value) in zip(result.splitlines(), rows[7:]):
            rid, vector = line.split('\t')
            assert int(rid) == i
            if value is None:
                assert vector == 'NULL'
            else:
                expected = int.from_bytes(hashlib.sha256(value.encode()).digest()[:4], 'big') / 2**32
                vector = json.loads(vector)
                assert len(vector) == 4 and abs(vector[0] - expected) < 1e-7
        assert len(result.splitlines()) == 293
        requests = [json.loads(line) for line in args.request_log.read_text().splitlines()[before:]]
        embedding = [r['body']['input'] for r in requests if r['path'].endswith('/embeddings')]
        assert any(len(batch) > 1 for batch in embedding), 'executor did not batch embedding requests'
        assert [v for batch in embedding for v in batch] == [v for _, v in rows[7:] if v is not None]
        for fault in ['bad-index', 'bad-number', 'bad-dimension', 'bad-count']:
            endpoint('embed', fault + '/')
            sql("select ai_embed('test_embed','x',4);", error=True)
        endpoint('embed')
        documents = ['apple' if i % 3 == 0 else 'other' for i in range(47)]
        expected = sorted(range(47), key=lambda i: (-int(documents[i] == 'apple'), i))
        result = json.loads(sql(f"select ai_rerank('test_rank','apple',{quote(json.dumps(documents))},null);"))
        assert [item['index'] for item in result] == expected
        objects = [{'id': i, 'text': text} for i, text in enumerate(documents)]
        result = json.loads(sql(f"select ai_rerank('test_rank','apple',{quote(json.dumps(objects))},'text');"))
        assert [item['id'] for item in result] == expected
        for fault in ['bad-index', 'duplicate-index', 'bad-number', 'bad-count']:
            endpoint('rank', fault + '/')
            sql("select ai_rerank('test_rank','a','[\"a\",\"b\"]');", error=True)
        print('PASS: prompt/LOB, invalid completion, embedding batch and row mapping, response validation, stable rerank across 3 batches')
    finally:
        for table in ['test_ai_lob', 'test_ai_batch']:
            sql('drop table if exists ' + table + ';')
        for name in ['complete', 'embed', 'rank']:
            sql(f"call DBMS_AI_SERVICE.DROP_AI_MODEL('test_{name}');")
            sql(f"call DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT('test_{name}_ep');")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', required=True)
    parser.add_argument('--port', type=int, default=12883)
    parser.add_argument('--request-log', type=Path, required=True)
    run(parser.parse_args())
