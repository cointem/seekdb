#!/usr/bin/env python3
"""Check native exhaustive root-index verification from a diagnostic server log.

Start the candidate with FTS_ROOT_RANGE_INDEX_VERIFY=1 and execute one builtin
IK TOKENIZE (or build an IK index). Normal benchmark servers omit the variable.
The native verifier compares all 63,488 valid BMP scalars against retained
UTF8_BIN strcmp search, plus seven invalid/supplementary/multi-char fallbacks,
and repeats after resetting and rebuilding the owned table.
"""
import argparse
from pathlib import Path
import re


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('log', type=Path)
    args = parser.parse_args()
    text = args.log.read_text(errors='replace')
    summaries = [line for line in text.splitlines() if 'fts root range index verification' in line]
    checks = [line for line in text.splitlines() if 'fts root range exhaustive comparison' in line]
    assert summaries, 'no verification: set env before server start and execute builtin IK'
    assert len(checks) >= 2 * len(summaries), (len(checks), len(summaries))
    for line in summaries:
        assert re.search(r'valid[=:]true', line), line
        assert re.search(r'payload_bytes[=:]262144', line), line
    for line in checks:
        assert re.search(r'valid_bmp[=:]63488', line), line
        assert re.search(r'negatives[=:][1-9][0-9]*', line), line
        assert re.search(r'fallback_cases[=:]7', line), line
    print(f'PASS: {len(summaries)} native root indexes, exhaustive BMP/negative/fallback and rebuild')
    for line in summaries:
        print(line)


if __name__ == '__main__':
    main()
