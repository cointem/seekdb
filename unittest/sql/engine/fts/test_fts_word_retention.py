#!/usr/bin/env python3
"""Compile/run the real deferred-retention hashmap API against existing oblib archives.

Uses the configured build's C++ flags, keeps assertions enabled, and builds only
this small regression executable in a temporary directory. Starts no server.
"""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


def main():
    root = Path(__file__).resolve().parents[4]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, default=root / 'build_release')
    args = parser.parse_args()
    build = args.build_dir.resolve()
    commands = json.loads((build / 'compile_commands.json').read_text())
    entry = next(item for item in commands
                 if '/src/sql/' in item['file'] and 'clang++' in item['command'])
    original_flags = shlex.split(entry['command'])
    # Unity source files live under the build tree, so infer the source root
    # from an actual source include rather than the compilation unit's path.
    source_include = next(token[2:] for token in original_flags
                          if token.startswith('-I') and token.endswith('/src/oblib'))
    original_root = source_include[:-len('/src/oblib')]
    generated_include = next(token[2:] for token in original_flags
                             if token.startswith('-I') and token.endswith('/generated'))
    original_build = generated_include[:-len('/generated')]
    command = entry['command'].replace(original_build, str(build)).replace(original_root, str(root))
    flags = []
    skip = False
    for token in shlex.split(command):
        if skip:
            skip = False
        elif token in ('-o', '-c'):
            skip = True
        elif token != '-DNDEBUG':
            flags.append(token)
    archives = [build / 'src/oblib/liboblib.a',
                build / 'src/oblib/lib/libob_malloc.a',
                build / 'third-party/jemalloc/lib/libjemalloc_pic.a']
    for archive in archives:
        if not archive.is_file():
            raise RuntimeError(f'Existing build archive required: {archive}')
    with tempfile.TemporaryDirectory(prefix='fts-word-retention-') as scratch:
        executable = Path(scratch) / 'test_fts_word_retention'
        link_flags = ['-Wl,-dead_strip'] if sys.platform == 'darwin' else [
            '-Wl,--gc-sections', '-ldl', '-lpthread', '-lrt']
        source = Path(__file__).with_suffix('.cpp')
        subprocess.run(flags + [str(source)] + [str(path) for path in archives]
                       + link_flags + ['-o', str(executable)], cwd=root, check=True)
        subprocess.run([str(executable)], cwd=scratch, check=True)


if __name__ == '__main__':
    main()
