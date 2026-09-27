"""Helper to extract the pinned Node.js dependencies from the local npm cache.

This is only needed when the tests/node_interop/node_modules directory is
missing (for example on a fresh checkout) and there is no network access to
run npm install. It extracts the exact tarballs of the pinned versions
from the user's npm cache (~/.npm/_cacache).

Usage: python extract_npm_cache.py <this directory>
"""
import base64
import binascii
import json
import os
import subprocess
import sys

CACHE = os.path.expanduser('~/.npm/_cacache')
DEST = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(__file__)
PACKAGES = {
    'https://registry.npmjs.org/socket.io-adapter/-/socket.io-adapter-2.5.7.tgz':  # noqa: E501
        'socket.io-adapter',
    'https://registry.npmjs.org/socket.io-parser/-/socket.io-parser-4.2.6.tgz':
        'socket.io-parser',
    'https://registry.npmjs.org/@socket.io/component-emitter/-/component-emitter-3.1.2.tgz':  # noqa: E501
        '@socket.io/component-emitter',
    'https://registry.npmjs.org/debug/-/debug-4.3.7.tgz': 'debug',
    'https://registry.npmjs.org/ms/-/ms-2.1.3.tgz': 'ms',
}


def main():
    entries = {}
    for root, dirs, files in os.walk(os.path.join(CACHE, 'index-v5')):
        for f in files:
            with open(os.path.join(root, f), 'rb') as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line.split(b'\t', 1)[1])
                    except Exception:
                        continue
                    key = entry.get('key', '')
                    prefix = 'make-fetch-happen:request-cache:'
                    if key.startswith(prefix):
                        entries[key[len(prefix):]] = entry.get('integrity')

    for url, name in PACKAGES.items():
        integrity = entries.get(url)
        if not integrity:
            print('MISSING', url)
            sys.exit(1)
        algo, b64 = integrity.split('-', 1)
        hex_digest = binascii.hexlify(base64.b64decode(b64)).decode()
        content = os.path.join(CACHE, 'content-v2', algo, hex_digest[:2],
                               hex_digest[2:4], hex_digest[4:])
        dest = os.path.join(DEST, 'node_modules', name)
        os.makedirs(dest, exist_ok=True)
        subprocess.run(['tar', 'xzf', content, '-C', dest,
                        '--strip-components=1'], check=True)
        print('extracted', name)


if __name__ == '__main__':
    main()
