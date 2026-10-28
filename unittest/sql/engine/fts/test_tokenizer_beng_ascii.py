#!/usr/bin/env python3
"""Ordered fused ASCII BENG DEFAULT coverage against the original pipeline.

Uses an existing server only. The nonconstant parser expression selects the
ordinary tree path, providing independent tokenization/normalization coverage.
"""
import argparse
import json
import random
import string
import subprocess

from test_tokenizer_binary_json import expression, literal, run


def cases():
    inputs = [None, '', ''.join(chr(i) for i in range(128)),
              'ABC abc AbC 123 alpha ALPHA alpha beta Beta',
              'the THE a A about FROM com WWW und with that who',
              "alpha_beta+GAMMA-delta.abc,DEF;ghi:JKL/$money%^&*()[]{}<>=~`|\\\"'",
              ''.join('before' + chr(i) + 'AFTER ' for i in range(128)),
              'a aa aaa aaaa aaaaaa aaaaaaa aaaaaaaa',
              'a' * 83 + ' ' + 'B' * 84 + ' ' + 'c' * 85,
              'prefix ' + 'Q' * 513 + ' suffix TAIL',
              'BEFORE ' + 'r' * 4096 + ' after',
              'alpha ALPHA 数据库 beta', 'prefix I İ i ı suffix',
              'prefix é À à 𠀀 😀 suffix']
    rng = random.Random(176901)
    ascii_bytes = ''.join(chr(i) for i in range(128))
    punctuation = string.punctuation + ''.join(chr(i) for i in range(33)) + chr(127)
    for _ in range(64):
        inputs.append(''.join(rng.choice(ascii_bytes) for _ in range(rng.randrange(1, 513))))
        words = [''.join(rng.choice(string.ascii_letters + string.digits)
                         for _ in range(rng.randrange(1, 96))) for _ in range(12)]
        inputs.append(''.join(word + rng.choice(punctuation) for word in words))
    configs = ('[]', '[{"additional_args":[{"min_token_size":1},{"max_token_size":10}]}]',
               '[{"additional_args":[{"min_token_size":2},{"max_token_size":10}]}]',
               '[{"additional_args":[{"min_token_size":1},{"max_token_size":84}]}]')
    return [(body, config, coll) for coll in
            ('utf8mb4_bin', 'utf8mb4_general_ci', 'utf8mb4_unicode_ci')
            for config in configs for body in inputs]


def statements(inputs, oracle):
    return [f"SELECT 'case-{i}', " + expression(literal(body), 'beng', config, coll, oracle) + ';'
            for i, (body, config, coll) in enumerate(inputs)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client', required=True)
    parser.add_argument('--port', required=True, type=int)
    parser.add_argument('--timeout', default=240, type=int)
    args = parser.parse_args()
    command = [args.client, '--no-defaults', '-h127.0.0.1', f'-P{args.port}', '-uroot',
               '-N', '-s', '--raw', '--default-character-set=utf8mb4', '--max-allowed-packet=64M']
    inputs = cases()
    # ALL exercises the ordinary sink's repeat frequencies/doc length, while
    # the nonconstant parser goes through the one-shot analyzer pipeline.
    all_inputs = [(body, json.dumps([{"output": "all"}] + json.loads(config)), coll)
                  for body, config, coll in inputs]
    inputs += all_inputs
    actual = run(command, statements(inputs, False), args.timeout)
    expected = run(command, statements(inputs, True), args.timeout)
    assert actual == expected, 'fused BENG changed ordered token spellings/membership'
    # When supported, Turkish ASCII I must retain the real Unicode mapping and
    # therefore reject the byte-preserving fused path before emitting terms.
    probe = subprocess.run(command, input="SHOW COLLATION LIKE 'utf8mb4_turkish_ci';\n",
                           text=True, capture_output=True, timeout=args.timeout)
    if probe.returncode:
        raise RuntimeError(probe.stderr)
    if probe.stdout.strip():
        turkish = [('alpha I III i IN with tail', '[]', 'utf8mb4_turkish_ci')]
        assert run(command, statements(turkish, False), args.timeout) == run(
            command, statements(turkish, True), args.timeout), 'Turkish I fallback changed'
    print(f'PASS: {len(actual)} ordered ASCII/control/punctuation/min-max/fallback BENG cases')


if __name__ == '__main__':
    main()
