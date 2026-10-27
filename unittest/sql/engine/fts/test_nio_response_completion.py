#!/usr/bin/env python3
"""Exercise SQL-NIO completion through standard MySQL text packets (root/no password).

No schema changes. Optional --client also checks the unchanged compressed path.
The large response intentionally delays reads until another connection finishes;
this tests progress and byte preservation, without claiming a specific send-buffer
size or requiring a timing-sensitive assertion about EAGAIN.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import select
import socket
import struct
import subprocess

MAX_PACKET = 0xffffff


class QueryError(Exception):
    pass


def lenenc(data, pos=0):
    tag = data[pos]
    pos += 1
    if tag < 251:
        return tag, pos
    if tag == 251:
        return None, pos
    width = {252: 2, 253: 3, 254: 8}[tag]
    return int.from_bytes(data[pos:pos + width], 'little'), pos + width


def literal(value):
    return "CONVERT(X'" + json.dumps(value, ensure_ascii=False).encode().hex() + "' USING utf8mb4)"


class Client:
    def __init__(self, host, port, small_receive=False):
        self.sock = socket.socket()
        if small_receive:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
        self.sock.settimeout(30)
        self.sock.connect((host, port))
        greeting, seq = self.packet(0)
        assert greeting[0] == 10, greeting[:30]
        pos = greeting.index(0, 1) + 1 + 4 + 8 + 1
        lower = int.from_bytes(greeting[pos:pos + 2], 'little')
        upper = int.from_bytes(greeting[pos + 5:pos + 7], 'little')
        server_caps = lower | upper << 16
        # PROTOCOL_41, SECURE_CONNECTION, LONG_PASSWORD, LONG_FLAG,
        # TRANSACTIONS, PLUGIN_AUTH. No EOF deprecation, compression or multiresults.
        caps = (1 | 4 | 512 | 8192 | 32768 | 524288) & server_caps
        assert caps & 512 and caps & 32768
        login = struct.pack('<IIB23x', caps, 64 << 20, 45) + b'root\0\0'
        if caps & 524288:
            login += b'mysql_native_password\0'
        self.send(login, seq + 1)
        auth, seq = self.packet(seq + 2)
        if auth[0] == 254:
            self.send(b'', seq + 1)
            auth, _ = self.packet(seq + 2)
        assert auth[0] == 0, ('root/no-password authentication failed', auth)

    def close(self):
        self.sock.close()

    def exact(self, size):
        result = bytearray()
        while len(result) < size:
            part = self.sock.recv(size - len(result))
            if not part:
                raise AssertionError('unexpected connection EOF')
            result += part
        return bytes(result)

    def packet(self, expected):
        result = bytearray()
        while True:
            header = self.exact(4)
            size = int.from_bytes(header[:3], 'little')
            assert header[3] == expected % 256, (header[3], expected)
            result += self.exact(size)
            last = expected
            expected += 1
            if size != MAX_PACKET:
                return bytes(result), last

    def send(self, payload, sequence=0):
        assert len(payload) < MAX_PACKET
        self.sock.sendall(len(payload).to_bytes(3, 'little') + bytes([sequence % 256]) + payload)

    def submit(self, sql):
        self.send(b'\x03' + sql.encode())

    def result(self):
        first, seq = self.packet(1)
        if first[0] == 255:
            raise QueryError(first[1:])
        if first[0] == 0:
            return []
        count, _ = lenenc(first)
        for _ in range(count):
            _, seq = self.packet(seq + 1)
        eof, seq = self.packet(seq + 1)
        assert eof[0] == 254 and len(eof) < 9, eof
        rows = []
        while True:
            row, seq = self.packet(seq + 1)
            if row[0] == 254 and len(row) < 9:
                return rows
            if row[0] == 255:
                raise QueryError(row[1:])
            values, pos = [], 0
            for _ in range(count):
                size, pos = lenenc(row, pos)
                values.append(None if size is None else row[pos:pos + size])
                if size is not None:
                    pos += size
            assert pos == len(row), (pos, len(row))
            rows.append(values)

    def query(self, sql):
        self.submit(sql)
        return self.result()


def worker(host, port, index):
    client = Client(host, port)
    try:
        for n in range(40):
            value = {'worker': index, 'iteration': n, 'escaped': '\n\\"数据库', 'nested': [None, True, 42]}
            sql = 'SELECT CAST(' + literal(value) + ' AS JSON), NULL, ' + str(n)
            row = client.query(sql)[0]
            assert json.loads(row[0]) == value and row[1] is None and row[2] == str(n).encode()
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--client', help='optional obclient for --compress verification')
    args = parser.parse_args()
    client = Client(args.host, args.port)
    try:
        values = [None, True, 42, '', [], {}, ['quote"', '\\', '\n', '数据库 😀'], {'large': 'x' * 14000}]
        # Read each response before issuing the next, then submit several complete
        # requests together before reading. Every command restarts packet seq at 0.
        for value in values:
            assert json.loads(client.query('SELECT CAST(' + literal(value) + ' AS JSON)')[0][0]) == value
        for value in values:
            client.submit('SELECT CAST(' + literal(value) + ' AS JSON)')
        for value in values:
            assert json.loads(client.result()[0][0]) == value
        client.submit('SELECT * FROM definitely_missing_nio_completion_table')
        client.submit('SELECT 123')
        try:
            client.result()
            raise AssertionError('expected SQL error')
        except QueryError:
            pass
        assert client.result() == [[b'123']]
    finally:
        client.close()
    slow = Client(args.host, args.port, small_receive=True)
    try:
        # 8 MiB response, bounded row size and receive buffer. Leave the response
        # unread while independent work runs; pipeline a successor on the same fd.
        slow.submit(' UNION ALL '.join(["SELECT REPEAT('z',262144)"] * 32))
        assert select.select([slow.sock], [], [], 30)[0], 'response never became readable'
        slow.submit('SELECT 456')
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda i: worker(args.host, args.port, i), range(4)))
        rows = slow.result()
        assert len(rows) == 32 and all(row == [b'z' * 262144] for row in rows)
        assert slow.result() == [[b'456']]
    finally:
        slow.close()
    if args.client:
        statements = "SELECT JSON_ARRAY('compression',42);\nSELECT * FROM definitely_missing_nio_completion_table;\nSELECT JSON_ARRAY('after_error',TRUE);\nSELECT LENGTH(REPEAT('x',20000));\n"
        process = subprocess.run([args.client, '--no-defaults', '--compress', '--force', '--raw',
            '--skip-column-names', '-h' + args.host, '-P' + str(args.port), '-uroot'],
            input=statements, text=True, capture_output=True, timeout=60)
        lines = [line for line in process.stdout.splitlines() if line]
        assert len(lines) == 3, (process.stdout, process.stderr)
        assert json.loads(lines[0]) == ['compression', 42]
        assert json.loads(lines[1]) == ['after_error', True]
        assert lines[2] == '20000' and 'ERROR' in process.stderr
    print('PASS: sequential/pipelined JSON, SQL error reuse, concurrent clients, delayed 8MiB drain' +
          (', compressed result/error reuse' if args.client else ''))


if __name__ == '__main__':
    main()
