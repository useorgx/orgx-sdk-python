"""Read-only release preflight; refuse collisions or uncertain package ownership."""
import json
import re
import sys
from urllib.error import HTTPError
from urllib.request import urlopen

kind = sys.argv[1]
if kind == 'npm':
    with open('package.json') as source:
        target = json.load(source)['version']
    endpoint = 'https://registry.npmjs.org/@useorgx%2Fsdk/latest'
    expected = 'https://github.com/useorgx/orgx-sdk-typescript'
elif kind == 'pypi':
    import tomllib
    with open('pyproject.toml', 'rb') as source:
        target = tomllib.load(source)['project']['version']
    endpoint = 'https://pypi.org/pypi/orgx/json'
    expected = 'https://github.com/useorgx/orgx-sdk-python'
else:
    raise SystemExit('Unknown registry')

try:
    with urlopen(endpoint, timeout=15) as response:
        raw = response.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise SystemExit('Registry response exceeds bound')
        record = json.loads(raw)
except HTTPError as error:
    if error.code != 404:
        raise SystemExit(f'Registry preflight HTTP {error.code}') from None
    print(json.dumps({'registry': kind, 'http_status': 404, 'target_version': target, 'published': False}))
    raise SystemExit(0)

info = record if kind == 'npm' else record['info']
repository = (info.get('repository') or {}).get('url', '') if kind == 'npm' else (info.get('project_urls') or {}).get('Repository', '')
repository = repository.removeprefix('git+').removesuffix('.git').rstrip('/')
if repository != expected:
    raise SystemExit('Existing package repository does not establish OrgX ownership')
current = info['version']
if not all(re.fullmatch(r'\d+\.\d+\.\d+', version) for version in (current, target)):
    raise SystemExit('Release version needs explicit reconciliation')
current_parts = tuple(map(int, current.split('.')))
target_parts = tuple(map(int, target.split('.')))
if current_parts > target_parts or ('--publish' in sys.argv and current_parts == target_parts):
    raise SystemExit('Target release already exists or is older than current registry version')
print(json.dumps({'registry': kind, 'http_status': 200, 'current_version': current, 'target_version': target, 'repository_verified': True}))
