#!/usr/bin/env python

"""Copyright 2026 NOAA
All rights reserved.

Resolve the intended observation usage from a build_gsinfo repository (the
"control logic"; by default NOAA-PSL/build_gsinfo-fix, branch psl-develop)
for a given analysis cycle.

The date resolution mirrors build_gsinfo's create_satinfo.sh and
create_ozinfo.sh: for every satellite listed in ``satinfo/satellites`` (or
``ozinfo/satellites``) the latest ``<sensor_sat>/YYYYMMDDHH`` file whose date
is on or before the cycle is used. convinfo is resolved the same way from
dated ``convinfo/YYYYMMDDHH`` files when the repository has them (as
build_gsinfo-fix does), otherwise ``convinfo/merged_convinfo.txt`` is used
for every cycle, as build_gsinfo's create_convinfo.sh does.

Files are read from any git ref (branch, tag or commit) without touching a
working tree, so the branch can be changed with a command line option. A
local checkout can also be read as-is by passing an empty ref.
"""

import bisect
import fcntl
import hashlib
import os
import re
import subprocess
import time
from functools import lru_cache
from pathlib import Path

DEFAULT_GSINFO_URL = 'https://github.com/NOAA-PSL/build_gsinfo-fix.git'
DEFAULT_GSINFO_REF = 'psl-develop'
CONVINFO_PATH = 'convinfo/merged_convinfo.txt'
DATED_FILE = re.compile(r'^\d{10}$')


def _git(repo, *args, binary=False):
    result = subprocess.run(['git', '--git-dir' if repo.suffix == '.git' or
                             (repo / 'HEAD').is_file() else '-C', str(repo), *args],
                            capture_output=True, check=True)
    return result.stdout if binary else result.stdout.decode()


def _is_url(location):
    return '://' in str(location) or str(location).startswith('git@')


def _cached_clone(url, cache_dir=None, refresh_minutes=None):
    """Bare clone of ``url`` in a shared cache, fetched at most every
    ``refresh_minutes`` ($OBS_USAGE_GSINFO_REFRESH_MINUTES, default 60) so
    that thousands of cycle tasks do not each contact the git server."""
    root = Path(cache_dir or os.environ.get(
        'OBS_USAGE_CACHE', Path.home() / '.cache' / 'score-monitoring'))
    if refresh_minutes is None:
        refresh_minutes = float(os.environ.get('OBS_USAGE_GSINFO_REFRESH_MINUTES', 60))
    name = re.sub(r'\.git$', '', url.rstrip('/').split('/')[-1])
    repo = root / f'{name}-{hashlib.sha1(url.encode()).hexdigest()[:8]}.git'
    root.mkdir(parents=True, exist_ok=True)
    with open(repo.with_suffix('.lock'), 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not repo.exists():
            subprocess.run(['git', 'clone', '--quiet', '--bare', url, str(repo)],
                           check=True)
            (repo / 'FETCH_HEAD').touch()
            return repo
        stamp = repo / 'FETCH_HEAD'
        age = time.time() - (stamp.stat().st_mtime if stamp.exists() else 0)
        if age < refresh_minutes * 60:
            return repo
        try:
            subprocess.run(['git', '--git-dir', str(repo), 'fetch', '--quiet',
                            '--prune', url, '+refs/heads/*:refs/heads/*',
                            '+refs/tags/*:refs/tags/*'],
                           check=True, capture_output=True)
            stamp.touch()
        except subprocess.CalledProcessError as error:
            print(f'warning: could not update {repo} from {url}; using cached '
                  f'copy ({error.stderr.decode().strip()})')
    return repo


class GsinfoSource:
    """Read-only access to build_gsinfo files at a given git ref."""

    def __init__(self, location=DEFAULT_GSINFO_URL, ref=DEFAULT_GSINFO_REF,
                 cache_dir=None):
        self.location = str(location)
        self.ref = ref or ''
        self.repo = (_cached_clone(self.location, cache_dir)
                     if _is_url(location) else Path(location))
        if self.ref:
            try:
                self.commit = self._resolve(self.ref)
            except ValueError:
                if not _is_url(location):
                    raise
                # the ref may have been pushed since the last throttled fetch
                self.repo = _cached_clone(self.location, cache_dir, refresh_minutes=0)
                self.commit = self._resolve(self.ref)
            listing = _git(self.repo, 'ls-tree', '-r', '-z', self.commit)
            entries = [item.split('\t', 1) for item in listing.split('\0') if item]
            self.files = {path for _, path in entries}
            self.symlinks = {path for meta, path in entries
                             if meta.startswith('120000 ')}
        else:
            self.commit = self._worktree_commit()
            self.symlinks = set()
            self.files = {
                str(Path(dirpath, name).relative_to(self.repo))
                for dirpath, _, names in os.walk(self.repo)
                if '.git' not in Path(dirpath).relative_to(self.repo).parts
                for name in names}

    def _resolve(self, ref):
        for candidate in (ref, f'origin/{ref}', f'refs/remotes/origin/{ref}'):
            try:
                return _git(self.repo, 'rev-parse', '--verify', '--quiet',
                            f'{candidate}^{{commit}}').strip()
            except subprocess.CalledProcessError:
                continue
        raise ValueError(f'build_gsinfo ref {ref!r} not found in {self.location}')

    def _worktree_commit(self):
        try:
            commit = _git(self.repo, 'rev-parse', 'HEAD').strip()
            dirty = _git(self.repo, 'status', '--porcelain').strip()
            return commit + ('-dirty' if dirty else '')
        except (subprocess.CalledProcessError, FileNotFoundError):
            return 'worktree'

    @lru_cache(maxsize=None)
    def read(self, path):
        if not self.ref:
            return (self.repo / path).read_text(errors='replace')
        for _ in range(16):
            text = _git(self.repo, 'cat-file', 'blob', f'{self.commit}:{path}',
                        binary=True).decode(errors='replace')
            if path not in self.symlinks:
                return text
            # git stores a symlink (e.g. convinfo/1900010100) as its target path
            path = os.path.normpath(os.path.join(os.path.dirname(path), text.strip()))
        raise ValueError(f'too many levels of symbolic links: {path}')

    def listdir(self, path):
        prefix = path.rstrip('/') + '/'
        return sorted({name[len(prefix):].split('/')[0]
                       for name in self.files if name.startswith(prefix)})

    def describe(self):
        name = re.sub(r'\.git$', '', self.location.rstrip('/').split('/')[-1])
        return {'location': self.location, 'name': name,
                'ref': self.ref or '(working tree)', 'commit': self.commit}


def _data_lines(text):
    for line in text.splitlines():
        fields = line.split()
        if fields and not fields[0].startswith('!'):
            yield fields


def _parse_usage_table(text):
    """Parse satinfo/ozinfo rows: ``sensor_sat chan|level iuse ...``."""
    table = {}
    for fields in _data_lines(text):
        try:
            table.setdefault(fields[0].lower(), {})[int(fields[1])] = int(fields[2])
        except (IndexError, ValueError):
            continue
    return table


class GsinfoInventory:
    """Intended usage (satinfo, ozinfo, convinfo) resolved by cycle date."""

    def __init__(self, source):
        self.source = source
        self.warnings = []
        self._lists = {top: self._satellites(top) for top in ('satinfo', 'ozinfo')}
        self._dates = {}
        self._tables = {}
        self.convinfo_files = set()

    def _satellites(self, top):
        path = f'{top}/satellites'
        if path not in self.source.files:
            raise ValueError(f'{path} not found in build_gsinfo '
                             f'{self.source.describe()}')
        return [fields[0] for fields in _data_lines(self.source.read(path))]

    def _dated_files(self, top, name):
        key = (top, name)
        if key not in self._dates:
            self._dates[key] = sorted(
                int(item) for item in self.source.listdir(f'{top}/{name}'.rstrip('/'))
                if DATED_FILE.match(item))
        return self._dates[key]

    def _resolve(self, top, when):
        """Return ``({sensor: {chan|level: iuse}}, {sensor: file})``."""
        date = int(when.strftime('%Y%m%d%H'))
        usage, provenance = {}, {}
        for name in self._lists[top]:
            dates = self._dated_files(top, name)
            position = bisect.bisect_right(dates, date)
            if position == 0:
                message = f'{top}/{name}: no file dated on or before {date}'
                if message not in self.warnings:
                    self.warnings.append(message)
                continue
            path = f'{top}/{name}/{dates[position - 1]}'
            if path not in self._tables:
                self._tables[path] = _parse_usage_table(self.source.read(path))
            for sensor, table in self._tables[path].items():
                usage.setdefault(sensor, {}).update(table)
                provenance[sensor] = path
        return usage, provenance

    def satinfo(self, when):
        return self._resolve('satinfo', when)

    def ozinfo(self, when):
        return self._resolve('ozinfo', when)

    def convinfo(self, when=None):
        """Return ``({(otype, type): {subtype: iuse}}, file)`` for a cycle.

        Uses the latest dated ``convinfo/YYYYMMDDHH`` file on or before the
        cycle when the repository has dated files, else merged_convinfo.txt.
        """
        dates = self._dated_files('convinfo', '')
        path = CONVINFO_PATH
        if dates and when is not None:
            date = int(when.strftime('%Y%m%d%H'))
            position = bisect.bisect_right(dates, date)
            if position:
                path = f'convinfo/{dates[position - 1]}'
            else:
                message = f'convinfo: no file dated on or before {date}; using {path}'
                if message not in self.warnings:
                    self.warnings.append(message)
        if path not in self.source.files:
            raise ValueError(f'{path} not found in build_gsinfo '
                             f'{self.source.describe()}')
        self.convinfo_files.add(path)
        return self._convinfo_table(path), path

    @lru_cache(maxsize=None)
    def _convinfo_table(self, path):
        table = {}
        for fields in _data_lines(self.source.read(path)):
            try:
                otype, typ, sub, iuse = (fields[0].lower(), int(fields[1]),
                                         int(fields[2]), int(fields[3]))
            except (IndexError, ValueError):
                continue
            table.setdefault((otype, typ), {})[sub] = iuse
        return table

    @lru_cache(maxsize=None)
    def obs_input_names(self):
        """Map GSI summary names ``(dplat, dtype)`` to info-file names (dsis).

        Needed where the two differ, e.g. ozone ``n19 sbuv2`` -> ``sbuv8_n19``.
        """
        names = {}
        for path in sorted(self.source.files):
            if not (path.startswith('obs_input/') and path.endswith('.txt')):
                continue
            for fields in _data_lines(self.source.read(path)):
                if len(fields) >= 4 and fields[3] != 'null':
                    names.setdefault((fields[2].lower(), fields[1].lower()),
                                     fields[3].lower())
        return names
