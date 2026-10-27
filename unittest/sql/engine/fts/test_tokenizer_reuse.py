#!/usr/bin/env python3
"""Compare tokenizer results across builds and concurrent sessions.

Capture a baseline with --snapshot FILE, then check another build against it
with --compare FILE. Uses read-only queries, optional session-local prepared
statements, and no third-party packages.
"""
import argparse
import concurrent.futures
import json
from pathlib import Path
import random
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", required=True)
    parser.add_argument("--port", type=int, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--snapshot", type=Path)
    mode.add_argument("--compare", type=Path)
    parser.add_argument("--extended", action="store_true", help="also vary parser/options through prepared bindings")
    args = parser.parse_args()
    command = [args.client, "--no-defaults", "-h127.0.0.1", f"-P{args.port}",
               "-uroot", "-N", "-s", "--default-character-set=utf8mb4"]
    rng = random.Random(1729)
    words = ["数据库", "全文索引", "中华人民共和国", "北京大学", "三公斤", "两百万元",
             "Alpha", "alpha", "ABC123", "ß", "İ", "é", "Ｅ１２", "カタカナ", "한국어",
             "𠀀", "😀", "the", "and", "123.45", "-", "\t", "\n", "，", "。​"]
    texts = [None, "", " ", "the and", "数据库数据库数据库", "alpha " * 1000,
             "全文索引测试𠀀😀" * 300, "AaBb 123 ＡＢＣ１２３", "e\u0301 é ß İ",
             # Alternate input slices and converted temporary normalization buffers.
             "alpha ALPHA alpha beta BETA gamma GAMMA alpha",
             "...alpha!!! ALPHA beta, BETA abc123 ABC123",
             # Multiple IK batches and arbitration chains, followed by tiny input.
             "中华人民共和国 北京大学 三公斤 数据库 abc123 " * 1100,
             "数据库 a"]
    texts += [" ".join(rng.choices(words, k=rng.randrange(1, 80))) for _ in range(247)]
    statements = []
    for text in texts:
        value = "CAST(NULL AS CHAR CHARACTER SET utf8mb4)" if text is None else "CONVERT(X'" + text.encode().hex() + "' USING utf8mb4)"
        for name in ("ik", "beng"):
            statements.append(f"SELECT {len(statements)}, TOKENIZE({value}, '{name}', '[{{\"output\":\"all\"}}]');")

    if args.extended:
        variants = {
            "ik": [None, {"ik_mode": "smart"}, {"ik_mode": "max_word"}],
            "beng": [None, {"min_token_size": 2, "max_token_size": 20}],
            "space": [None, {"min_token_size": 2, "max_token_size": 20}],
            "ngram": [None, {"ngram_token_size": 1}, {"ngram_token_size": 3}],
            "ngram2": [None, {"min_ngram_size": 2, "max_ngram_size": 3}],
        }
        unique_words = ['pool' + ''.join(chr(97 + (i // (26 ** j)) % 26)
                                         for j in range(3)) for i in range(2048)]
        boundary_texts = [' '.join(unique_words[:n]) for n in (1, 63, 64, 65, 128, 512, 2048)]
        boundary_texts += [boundary_texts[3] + ' ' + boundary_texts[3].upper()]
        for text in texts[:24] + boundary_texts:
            value = "CAST(NULL AS CHAR CHARACTER SET utf8mb4)" if text is None else "CONVERT(X'" + text.encode().hex() + "' USING utf8mb4)"
            for name, options in variants.items():
                for option in options:
                    config = [{"output": "all"}]
                    if option is not None:
                        config.append({"additional_args": [{k: v} for k, v in option.items()]})
                    else:
                        config = []  # Exercise the default JSON array output, too.
                    encoded = json.dumps(config).encode().hex()
                    statements.append(f"SET @token_case={len(statements)}, @token_text={value}, @token_parser='{name}', @token_options=CONVERT(X'{encoded}' USING utf8mb4); EXECUTE token_stmt USING @token_text,@token_parser,@token_options;")

    def run(seed):
        ordered = statements[:]
        random.Random(seed).shuffle(ordered)
        header = "SET NAMES utf8mb4;\n"
        if args.extended:
            header += "PREPARE token_stmt FROM 'SELECT @token_case, TOKENIZE(CONVERT(? USING utf8mb4), CONVERT(? USING utf8mb4), CONVERT(? USING utf8mb4))';\n"
        result = subprocess.run(command, input=header + "\n".join(ordered),
                                text=True, capture_output=True)
        if result.returncode:
            raise RuntimeError(result.stderr)
        values = {}
        for line in result.stdout.splitlines():
            key, raw = line.split("\t", 1)
            item = None if raw == "NULL" else json.loads(raw)
            if isinstance(item, dict) and "tokens" in item:
                item["tokens"] = sorted(item["tokens"], key=lambda token: json.dumps(token, sort_keys=True))
            if isinstance(item, list):
                item = sorted(item)
            values[key] = item
        assert len(values) == len(statements), (len(values), len(statements))
        return values

    actual = run(0)
    if args.snapshot:
        args.snapshot.write_text(json.dumps(actual, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
    else:
        expected = json.loads(args.compare.read_text())
        assert actual == expected, "tokenizer output differs from the baseline"
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for result in pool.map(run, range(1, 9)):
            assert result == actual, "tokenizer output differs between concurrent sessions"
    print(f"PASS: {len(statements)} tokenizer cases; eight concurrent sessions agree")


if __name__ == "__main__":
    main()
