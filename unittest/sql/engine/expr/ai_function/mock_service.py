#!/usr/bin/env python3
"""Deterministic HTTP fixtures for native AI expression integration tests."""
import argparse
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if self.server.request_log:
            with self.server.request_log.open('a') as stream:
                stream.write(json.dumps({'path': self.path, 'body': body}, ensure_ascii=False) + '\n')
        if self.path.startswith('/http-error/'):
            self.send_error(503)
            return
        if self.path.endswith('/embeddings') or self.path.endswith('/api/embed'):
            inputs = body['input']
            assert isinstance(inputs, list) and all(isinstance(x, str) for x in inputs)
            dim = body.get('dimensions', 768)
            vectors = [[int.from_bytes(hashlib.sha256(x.encode()).digest()[:4], 'big') / 2**32] * dim for x in inputs]
            data = [{'index': i, 'embedding': v} for i, v in enumerate(vectors)]
            if self.path.startswith('/bad-index/'):
                data[-1]['index'] = len(data)
            if self.path.startswith('/bad-number/'):
                data[-1]['embedding'][0] = 'not-a-number'
            if self.path.startswith('/bad-dimension/'):
                data[-1]['embedding'].append(0)
            if self.path.startswith('/bad-count/'):
                data.pop()
            result = {'embeddings': vectors} if self.path.endswith('/api/embed') else {'data': list(reversed(data))}
        elif self.path.endswith('/rerank'):
            docs = body['documents']
            data = [{'index': i, 'relevance_score': int(body['query'] in doc)} for i, doc in enumerate(docs)]
            if self.path.startswith('/bad-index/'):
                data[-1]['index'] = len(data)
            if self.path.startswith('/duplicate-index/') and len(data) > 1:
                data[-1]['index'] = 0
            if self.path.startswith('/bad-number/'):
                data[-1]['relevance_score'] = 'bad'
            if self.path.startswith('/bad-count/'):
                data.pop()
            result = {'results': list(reversed(data))}
        elif self.path.endswith('/api/generate'):
            assert body.get('stream') is False
            result = {'response': body['prompt'], 'done': True}
        elif self.path.endswith('/chat/completions'):
            prompt = body['messages'][-1]['content']
            if self.path.startswith('/empty/'):
                prompt = ''
            result = {'choices': [{'message': {'role': 'assistant', 'content': prompt}}]}
        else:
            self.send_error(404)
            return
        encoded = json.dumps(result, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, *_):
        pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--request-log', type=Path)
    args = parser.parse_args()
    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    server.request_log = args.request_log
    server.serve_forever()
