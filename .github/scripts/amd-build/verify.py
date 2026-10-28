#!/usr/bin/env python3
"""Verify artifact integrity and real target GPU library loading before startup."""
import ctypes
import hashlib
import json
import platform
from pathlib import Path

root = Path(__file__).resolve().parent
manifest = json.loads((root / 'manifest.json').read_text())
if platform.system() != 'Linux' or platform.machine() != 'x86_64':
    raise SystemExit('This artifact requires Linux x86_64.')
for name, expected in manifest['files'].items():
    if hashlib.sha256((root / name).read_bytes()).hexdigest() != expected:
        raise SystemExit('Artifact checksum mismatch: ' + name)
# RTLD_NOW forces unresolved transitive symbols to fail before starting seekdb.
bridge = ctypes.CDLL(str(root / 'lib/libseekdb_cuvs_bridge.so'), mode=2)
for symbol in ('seekdb_cuvs_build', 'seekdb_cuvs_search', 'seekdb_cuvs_free',
               'seekdb_cuvs_serialize', 'seekdb_cuvs_deserialize'):
    getattr(bridge, symbol)
print('Artifact verified; source commit:', manifest['commit'], flush=True)
