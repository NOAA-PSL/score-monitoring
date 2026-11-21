#!/usr/bin/env python

"""Copyright 2026 NOAA
All rights reserved.

Append-only, file-based archive of per-cycle observation usage records.

Each cycle task (obs_usage_record.py) appends one compact JSON record to
``<archive>/<experiment>/<YYYY>/<YYYYMM>.jsonl.gz``. Every record is its own
gzip member, so appending never rewrites earlier records and the file stays
a valid multi-member gzip file (``zcat`` reads it). A record is about 1 kB, so
1988 to present is tens of MB in about 450 monthly files.

Nothing is ever replaced: when a cylc run is rewound (``runN``), a new
workflow is installed (``..._installN``) or a catch-up workflow re-records a
period, the re-recorded cycles get new records next to the old ones. Readers
pick, for each cycle, the most recently created record (so the latest run or
install wins); earlier versions stay available for ``--as-of``, ``--upto``
and ``--compare`` views of how usage changed.

Appends hold an exclusive ``flock`` for a fraction of a second, so many
cycle tasks can record at once; the archive must be on a file system with
working ``flock`` across the hosts the tasks run on (local disk, NFSv4, or
Lustre mounted with ``flock``).
"""

import datetime as dt
import fcntl
import gzip
import json
import os
import re
import zlib
from pathlib import Path

from obs_usage_status import (ABSENT, CONVENTIONAL, FAILURES, OZONE, RADIANCE,
                              CycleResult)

SCHEMA = 1
GROUP_CODES = {RADIANCE: 'rad', OZONE: 'oz', CONVENTIONAL: 'conv'}
CODE_GROUPS = {code: group for group, code in GROUP_CODES.items()}
CYCLE_FORMAT = '%Y%m%d%H'
INSTALL = re.compile(r'[_.-]?install(\d+)$')


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')


def parse_time(text):
    """ISO time (``Z`` allowed, naive = UTC) as an aware datetime."""
    value = dt.datetime.fromisoformat(str(text).replace('Z', '+00:00'))
    return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)


def parse_workflow_id(workflow_id):
    """Split a cylc workflow id such as
    ``score-monitoring/cylc_scout_sfs/test_sfs_2023_gdas_gsi_install0/run2``
    into the workflow name without its ``installN`` suffix, the install
    number (0 when there is no suffix) and the run number (0 for
    ``cylc install --no-run-name``)."""
    parts = [part for part in str(workflow_id).strip('/').split('/') if part]
    run = 0
    if parts and re.fullmatch(r'run\d+', parts[-1]):
        run = int(parts.pop()[3:])
    name = parts[-1] if parts else ''
    match = INSTALL.search(name)
    return {'workflow_id': str(workflow_id),
            'workflow': name[:match.start()] if match else name,
            'install': int(match.group(1)) if match else 0,
            'run': run}


def source_label(record):
    return f"install{record['install']}/run{record['run']}"


def parse_source(text):
    """``install1/run2`` -> ``(1, 2)``."""
    match = re.fullmatch(r'install(\d+)/run(\d+)', text.strip())
    if not match:
        raise ValueError(f'expected installN/runM, got {text!r}')
    return int(match.group(1)), int(match.group(2))


def make_record(cycle, results, *, source, valid=True, warnings=(), **provenance):
    """Compact record of one cycle's ``{(group, row): CycleResult}``.

    Rows that are not used (outcome absent: white on the chart) and whose run
    flags agree with build_gsinfo are omitted; reasons are kept only for
    failing rows.
    """
    rows = []
    for (group, name), result in sorted((results or {}).items()):
        if result.outcome == ABSENT and not result.mismatch:
            continue
        rows.append([GROUP_CODES[group], name, result.intent, result.outcome,
                     ' | '.join(result.reasons) if result.outcome in FAILURES else '',
                     ' | '.join(result.mismatch)])
    return {'schema': SCHEMA, 'cycle': cycle.strftime(CYCLE_FORMAT),
            'created': utc_now(), **source, 'valid': bool(valid),
            'warnings': list(warnings), **provenance, 'rows': rows}


def record_results(record):
    """``{(group, row): CycleResult}``, or ``None`` for an unreadable log."""
    if not record.get('valid', True):
        return None
    return {(CODE_GROUPS[group], name): CycleResult(
                intent, outcome, reason.split(' | ') if reason else [],
                mismatch.split(' | ') if mismatch else [])
            for group, name, intent, outcome, reason, mismatch in record['rows']}


def month_path(archive, experiment, month):
    return Path(archive) / experiment / f'{month:%Y}' / f'{month:%Y%m}.jsonl.gz'


def _members(data):
    """Yield ``(end_offset, text)`` for each complete gzip member."""
    view, offset = memoryview(data), 0
    while offset < len(data):
        stream = zlib.decompressobj(wbits=31)
        try:
            text = stream.decompress(view[offset:]) + stream.flush()
        except zlib.error:
            return
        if not stream.eof:
            return
        offset = len(data) - len(stream.unused_data)
        yield offset, text


def append_record(archive, experiment, record):
    """Append ``record`` under an exclusive lock and return the file path.

    A member truncated by a crashed writer is cut off first so that it can
    never hide the records appended after it.
    """
    month = dt.datetime.strptime(record['cycle'], CYCLE_FORMAT)
    path = month_path(archive, experiment, month)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = gzip.compress(
        (json.dumps(record, separators=(',', ':')) + '\n').encode(), mtime=0)
    with open(path, 'a+b') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        try:
            stream.seek(0)
            data = stream.read()
            good = 0
            for good, _ in _members(data):
                pass
            if good != len(data):
                print(f'warning: dropping {len(data) - good} bytes of an '
                      f'incomplete record at the end of {path}')
                stream.truncate(good)
            stream.seek(0, os.SEEK_END)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)
    return path


def read_month(archive, experiment, month):
    """All records of a month in the order they were appended."""
    path = month_path(archive, experiment, month)
    if not path.is_file():
        return []
    records = []
    for _, text in _members(path.read_bytes()):
        for line in text.decode().splitlines():
            if line.strip():
                records.append(json.loads(line))
    return records


def select_latest(records, as_of=None, upto=None):
    """Return ``{cycle: record}`` with the latest version of each cycle.

    The latest version is the most recently created record (the last one
    appended on a tie): a rewound run or a catch-up workflow replaces the
    cycles it re-recorded, and a new install replaces every cycle it covers,
    while all other cycles keep earlier records. ``as_of`` ignores records
    created after that time; ``upto`` (a pair ``(install, run)``) ignores
    records from later installs/runs.
    """
    as_of = parse_time(as_of) if isinstance(as_of, str) else as_of
    chosen = {}
    for record in records:
        created = parse_time(record['created'])
        if as_of is not None and created > as_of:
            continue
        if upto is not None and (record['install'], record['run']) > tuple(upto):
            continue
        cycle = dt.datetime.strptime(record['cycle'], CYCLE_FORMAT)
        if cycle not in chosen or created >= chosen[cycle][0]:
            chosen[cycle] = (created, record)
    return {cycle: record for cycle, (_, record) in chosen.items()}
