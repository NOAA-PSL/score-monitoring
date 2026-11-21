#!/usr/bin/env python

"""Copyright 2026 NOAA
All rights reserved.

Observation usage dashboard: a stop-light chart of every satellite sensor,
ozone sensor and conventional data type, comparing the intended usage in
build_gsinfo (default: NOAA-PSL/build_gsinfo-fix, branch psl-develop) with
what GSI actually did according to the gsistat log of every cycle. By
default every 6-hourly cycle is one column and one page is drawn per month;
``--resolution month`` rolls cycles up into a conservative monthly overview
(one column per month). Each page is drawn as one figure per group
(satellite radiance, ozone, conventional), sized for AMS journals
(``--width``, see obs_usage_figures.py).

Production (cycles recorded by the obs_usage_record.py cylc task):

    python obs_usage_dashboard.py --archive $OBS_USAGE_ARCHIVE_DIR \\
        --experiment $EXPERIMENT_NAME --start 1988-01 --end 2025-12 \\
        --resolution month --output-dir dashboard

For each cycle the newest record wins (e.g. a rewound run, a new install
or a catch-up workflow). ``--as-of`` / ``--upto`` show an earlier version and
``--compare-as-of`` / ``--compare-upto`` add charts of what changed since that
version.

Reanalysis streams (one experiment each) are joined into one record by
listing them oldest first, e.g. for the full period at the end of production:

    python obs_usage_dashboard.py --archive $OBS_USAGE_ARCHIVE_DIR \\
        --experiment stream1988 stream1993@1993 stream1997@1997 ... \\
        --start 1988-01 --end 2025-12 --resolution month --output-dir full

A stream given as ``NAME@START`` is used from START on and replaces the older
streams there; where streams without a START overlap (e.g. a newer stream's
spin-up), the oldest stream that recorded the cycle is used.

Prototype (gsistat logs in a directory, evaluated on the fly):

    python obs_usage_dashboard.py --gsistat-dir /path/to/gsistats \\
        --start 2015-05 --end 2015-05 --output-dir dashboard

See README.md for details.
"""

import argparse
import ast
import csv
import datetime as dt
import hashlib
import json
import os
import re
import sys
from collections import Counter, OrderedDict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from obs_usage_archive import (month_path, parse_source, parse_time,  # noqa: E402
                               read_month, record_results, select_latest,
                               source_label)
from obs_usage_figures import (AMS_WIDTHS, FORMATS, GROUP_FILES, Chart,  # noqa: E402
                               FigureSizeError, figure_style)
from obs_usage_gsinfo import (DEFAULT_GSINFO_REF, DEFAULT_GSINFO_URL,  # noqa: E402
                              GsinfoInventory, GsinfoSource)
from obs_usage_gsistat import parse_gsistat  # noqa: E402
from obs_usage_status import (ABSENT, CONVENTIONAL, CONVENTIONAL_VARIABLES,  # noqa: E402
                              FAILURES, GROUPS, MISSING, MONITORED,
                              NOT_ASSIMILATED, NOT_STAGED, OFF, OK_ASSIMILATED,
                              PARTIAL, STATUSES, UNEXPECTED, CycleResult,
                              aggregate_cycles, evaluate_cycle)

DEFAULT_FILE_FORMAT = 'gsistats.%Y%m%d%H_control'
OUTCOMES = (OK_ASSIMILATED, PARTIAL, NOT_ASSIMILATED, NOT_STAGED, UNEXPECTED,
            MONITORED, ABSENT, MISSING)
UNUSED = CycleResult(OFF, ABSENT)
STATE_FILE = '.obs_usage_dashboard_state.json'
CONVENTIONAL_LABEL_KEYS = {'ps': 'fit_psfc_data', 't': 'fit_t_data',
                           'q': 'fit_q_data', 'uv': 'fit_uv_data'}
# GNSS radio occultation: convinfo/gsistat "type" is the WMO satellite id
GNSSRO_SATELLITES = {
    3: 'Metop-B', 4: 'Metop-A', 5: 'Metop-C', 41: 'CHAMP', 42: 'TerraSAR-X',
    43: 'TanDEM-X', 44: 'PAZ', 421: 'Oceansat-2', 440: 'Megha-Tropiques',
    722: 'GRACE-A', 723: 'GRACE-B', 740: 'COSMIC-1 FM1', 741: 'COSMIC-1 FM2',
    742: 'COSMIC-1 FM3', 743: 'COSMIC-1 FM4', 744: 'COSMIC-1 FM5',
    745: 'COSMIC-1 FM6', 750: 'COSMIC-2 E1', 751: 'COSMIC-2 E2',
    752: 'COSMIC-2 E3', 753: 'COSMIC-2 E4', 754: 'COSMIC-2 E5',
    755: 'COSMIC-2 E6', 786: 'C/NOFS', 803: 'GRACE-FO 1', 804: 'GRACE-FO 2',
    820: 'SAC-C', 821: 'SAC-D', 825: 'KOMPSAT-5'}

# change between a baseline version and the current one; same CVD-safe colors
CHANGES = OrderedDict([
    ('worse', ('Worse than the baseline', '#D55E00')),
    ('still_failing', ('Failing in both versions', '#000000')),
    ('improved', ('Fixed or improved since the baseline', '#009E73')),
    ('changed', ('Changed, failing in neither (e.g. intent changed)', '#56B4E9')),
    ('not_comparable', ('Missing in either version', '#BBBBBB')),
    ('same', ('Unchanged, not failing', '#FFFFFF')),
])
SCORE = {'unexpected': 0, 'not_used': 0, 'partial': 1, 'assimilated': 2,
         'monitored': 2, 'unused': 2}


def parse_month(text):
    return dt.datetime.strptime(text, '%Y-%m')


def parse_stream(text):
    """``NAME`` or ``NAME@START`` -> ``(name, start or None)``; START is
    YYYY, YYYY-MM, YYYY-MM-DD or YYYYMMDDHH."""
    name, _, start = text.partition('@')
    if not name.strip():
        raise ValueError(f'no experiment name in {text!r}')
    if not start:
        return name.strip(), None
    digits = re.sub(r'[-:T ]', '', start.strip())
    formats = {4: '%Y', 6: '%Y%m', 8: '%Y%m%d', 10: '%Y%m%d%H'}
    if not digits.isdigit() or len(digits) not in formats:
        raise ValueError(f'expected NAME@YYYY[-MM[-DD]] or NAME@YYYYMMDDHH, got {text!r}')
    return name.strip(), dt.datetime.strptime(digits, formats[len(digits)])


def month_range(start, end):
    months, current = [], start
    while current <= end:
        months.append(current)
        current = (current.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return months


def month_cycles(month, hours):
    following = (month.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    cycles, current = [], month
    while current < following:
        cycles.append(current)
        current += dt.timedelta(hours=hours)
    return cycles


def conventional_labels(path=Path(__file__).resolve().parent / 'db_test1.py'):
    """Read prepbufr report-type descriptions already maintained for score-db
    registration (``get_conventional_instruments`` in db_test1.py) without
    importing that script (it needs a database connection)."""
    try:
        tree = ast.parse(Path(path).read_text())
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef)
                        and node.name == 'get_conventional_instruments')
        assign = next(node for node in function.body if isinstance(node, ast.Assign))
        return ast.literal_eval(assign.value)
    except (OSError, SyntaxError, ValueError, StopIteration):
        return {}


def row_label(group, name, labels):
    if group != CONVENTIONAL:
        return name
    var, typ = name.split()
    typ = int(typ)
    if var == 'gps':
        return f'gps {typ:03d} {GNSSRO_SATELLITES.get(typ, "GNSS-RO")}'
    info = labels.get(CONVENTIONAL_LABEL_KEYS.get(var), {}).get(typ, {})
    return f'{var} {typ} {info.get("instrument", "")}'.strip()


def row_order(key):
    group, name = key
    if group == CONVENTIONAL:
        var, typ = name.split()
        return (GROUPS.index(group), list(CONVENTIONAL_VARIABLES).index(var), int(typ))
    return (GROUPS.index(group), name, 0)


def gsinfo_label(info):
    return f"{info.get('name', '')} {info.get('ref', '')} @ {str(info.get('commit', ''))[:7]}"


def column_name(column, resolution):
    return column.strftime('%Y%m%d%H' if resolution == 'cycle' else '%Y-%m')


# ---------------------------------------------------------------- inputs --

class GsistatDirSource:
    """Evaluate gsistat logs in a directory on the fly (no archive)."""

    def __init__(self, args):
        self.args = args
        self.gsinfo = GsinfoSource(args.gsinfo, args.gsinfo_ref, args.cache_dir)
        self.inventory = GsinfoInventory(self.gsinfo)
        self.aliases = self.inventory.obs_input_names()

    def path(self, cycle):
        return Path(self.args.gsistat_dir) / cycle.strftime(self.args.file_format)

    def signature(self, month):
        return [self.gsinfo.commit] + [
            (str(path), path.stat().st_size, path.stat().st_mtime_ns)
            for path in map(self.path, month_cycles(month, self.args.cycle_hours))
            if path.is_file()]

    def load(self, month, version=None):
        """Return ``{cycle: (results or None, info)}`` for every expected cycle."""
        loaded = OrderedDict()
        for cycle in month_cycles(month, self.args.cycle_hours):
            path = self.path(cycle)
            info = {'origin': str(path), 'source': '', 'state': 'ok',
                    'gsinfo': gsinfo_label(self.gsinfo.describe()),
                    'iterations': self.args.iterations}
            if not path.is_file():
                loaded[cycle] = (None, info | {'state': 'missing'})
                continue
            usage = parse_gsistat(path)
            if not usage.valid:
                loaded[cycle] = (None, info | {'state': 'unreadable: ' + '; '.join(
                    usage.warnings or ['required gsistat sections not found'])})
                continue
            convinfo, _ = self.inventory.convinfo(cycle)
            loaded[cycle] = (evaluate_cycle(
                usage, self.inventory.satinfo(cycle)[0], self.inventory.ozinfo(cycle)[0],
                convinfo, self.aliases, self.args.iterations), info)
        return loaded

    def describe(self):
        return {'gsistat_dir': str(self.args.gsistat_dir),
                'file_format': self.args.file_format,
                'build_gsinfo': self.gsinfo.describe(),
                'build_gsinfo_convinfo_files': sorted(self.inventory.convinfo_files),
                'build_gsinfo_warnings': self.inventory.warnings}


class ArchiveSource:
    """Cycles recorded by obs_usage_record.py; the latest version wins.

    Several experiments (reanalysis streams, oldest first) are joined: a
    stream with a START is used from START on, replacing older streams;
    otherwise the oldest stream that recorded a cycle is used.
    """

    def __init__(self, args):
        self.args = args
        self.streams = args.streams

    def covering(self, cycle):
        """Names of the streams that may supply ``cycle``, oldest first."""
        return [name for i, (name, start) in enumerate(self.streams)
                if (start is None or start <= cycle)
                and not any(later is not None and later <= cycle
                            for _, later in self.streams[i + 1:])]

    def signature(self, month):
        paths = [month_path(self.args.archive, name, month) for name, _ in self.streams]
        return [[str(path), path.stat().st_size, path.stat().st_mtime_ns]
                for path in paths if path.is_file()]

    def load(self, month, version=(None, None)):
        as_of, upto = version
        chosen = {name: select_latest(read_month(self.args.archive, name, month),
                                      as_of, upto)
                  for name, _ in self.streams}
        loaded = OrderedDict()
        for cycle in month_cycles(month, self.args.cycle_hours):
            name = next((name for name in self.covering(cycle)
                         if cycle in chosen[name]), None)
            if name is None:
                loaded[cycle] = (None, {'origin': '', 'source': '', 'gsinfo': '',
                                        'state': 'missing (not recorded)'})
                continue
            record = chosen[name][cycle]
            label = source_label(record)
            info = {'origin': record.get('gsistat', {}).get('origin', ''),
                    'source': label if len(self.streams) == 1 else f'{name}/{label}',
                    'experiment': name, 'workflow': record.get('workflow', ''),
                    'state': 'ok',
                    'gsinfo': gsinfo_label(record.get('build_gsinfo', {})),
                    'iterations': record.get('iterations', 'all')}
            results = record_results(record)
            if results is None:
                info['state'] = 'unreadable: ' + '; '.join(record.get('warnings') or [])
            loaded[cycle] = (results, info)
        return loaded

    def describe(self):
        return {'archive': str(self.args.archive), 'experiments': [
            {'name': name, 'start': start and f'{start:%Y-%m-%dT%H}'}
            for name, start in self.streams]}


# ---------------------------------------------------------------- tables --

def month_table(loaded, column_of):
    """``{row: {column: CellStatus}}`` for one month of loaded cycles, plus
    the cell of rows that do not appear (unused, or missing)."""
    columns = OrderedDict()
    for cycle, (results, _) in loaded.items():
        columns.setdefault(column_of(cycle), []).append(results)
    rows = {row for results, _ in loaded.values() if results for row in results}
    table = {row: {column: aggregate_cycles([
                       None if results is None else results.get(row, UNUSED)
                       for results in members])
                   for column, members in columns.items()}
             for row in rows}
    fill = {column: aggregate_cycles([None if results is None else UNUSED
                                      for results in members])
            for column, members in columns.items()}
    return table, fill


def merge_tables(parts, columns):
    """Merge per-month ``(table, fill)`` parts into one ordered table."""
    rows = sorted({row for table, _ in parts for row in table}, key=row_order)
    merged = OrderedDict((row, OrderedDict()) for row in rows)
    for table, fill in parts:
        for row in rows:
            for column, cell in fill.items():
                merged[row][column] = table.get(row, {}).get(column, cell)
    return OrderedDict((row, OrderedDict((column, cells[column]) for column in columns))
                       for row, cells in merged.items())


def active_rows(table):
    """Drop rows that are 'not used' in every present cycle and never differ
    from build_gsinfo."""
    return OrderedDict((row, cells) for row, cells in table.items()
                       if any(cell.mismatch or any(outcome not in (ABSENT, MISSING)
                                                   for outcome in cell.counts)
                              for cell in cells.values()))


def evidence_rows(loaded, period_of):
    """Failing or mismatching rows grouped by period/row/outcome/source."""
    groups = OrderedDict()
    for cycle, (results, info) in loaded.items():
        period = period_of(cycle)
        if results is None:
            items = [(('*', '*'), CycleResult('', MISSING, [info['state']]))]
        else:
            items = sorted(((row, result) for row, result in results.items()
                            if result.outcome in FAILURES or result.mismatch),
                           key=lambda item: row_order(item[0]))
        for row, result in items:
            key = (period, row, result.intent, result.outcome,
                   ' | '.join(result.mismatch), info['source'])
            entry = groups.setdefault(key, {'cycles': 0, 'first': cycle,
                                            'reason': ' | '.join(result.reasons),
                                            'origin': info['origin']})
            entry['cycles'] += 1
            entry['last'] = cycle
    return groups


def compare_cell(baseline, current):
    if MISSING in (baseline.status, current.status):
        return 'not_comparable'
    before, after = SCORE[baseline.status], SCORE[current.status]
    if after > before:
        return 'improved'
    if after < before:
        return 'worse'
    if after < 2:
        return 'still_failing'
    return 'same' if baseline.status == current.status else 'changed'


# --------------------------------------------------------------- outputs --

def write_status(path, table, resolution, labels):
    with path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['group', 'row', 'label', 'period', 'status', 'status_label',
                         'intent', 'cycles_expected', 'cycles_present',
                         *(f'cycles_{key}' for key in OUTCOMES if key != MISSING),
                         'run_flags_differ_from_build_gsinfo'])
        for (group, name), cells in table.items():
            for column, cell in cells.items():
                writer.writerow([
                    group, name, row_label(group, name, labels),
                    column_name(column, resolution), cell.status,
                    STATUSES[cell.status][0], '/'.join(cell.intents),
                    cell.expected, cell.present,
                    *(cell.counts.get(key, 0) for key in OUTCOMES if key != MISSING),
                    int(cell.mismatch)])


def write_evidence(path, evidence, resolution, labels):
    with path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['group', 'row', 'label', 'period', 'cycles', 'first_cycle',
                         'last_cycle', 'intent', 'outcome', 'status', 'reason',
                         'run_flags_vs_build_gsinfo', 'source', 'gsistat'])
        for (period, (group, name), intent, outcome, mismatch, source), entry \
                in evidence.items():
            writer.writerow([
                group, name, 'all rows' if group == '*' else row_label(group, name, labels),
                column_name(period, resolution), entry['cycles'],
                entry['first'].strftime('%Y%m%d%H'), entry['last'].strftime('%Y%m%d%H'),
                intent, outcome, aggregate_cycles([CycleResult(intent, outcome)]).status
                if outcome != MISSING else 'missing', entry['reason'], mismatch,
                source, entry['origin']])


def write_changes(path, current, before, changes, resolution, labels,
                  sources, baseline_sources):
    with path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['group', 'row', 'label', 'period', 'change',
                         'baseline_status', 'current_status', 'baseline_source',
                         'current_source'])
        for row, cells in changes.items():
            for column, change in cells.items():
                old, new = before[row][column].status, current[row][column].status
                # cells no newer record replaced (same status and source) are skipped
                if change == 'same' or (old == new and baseline_sources.get(
                        column, '') == sources.get(column, '')):
                    continue
                writer.writerow([
                    row[0], row[1], row_label(*row, labels),
                    column_name(column, resolution), change, old, new,
                    baseline_sources.get(column, ''), sources.get(column, '')])


def short_run(label):
    """``[experiment/]install1/run2`` -> ``i1/r2``."""
    return re.sub(r'^(?:.*/)?install(\d+)/run(\d+)$', r'i\1/r\2', label)


def short_names(names):
    """Names without the leading and trailing words (split at _ . -) that
    all of them share, e.g. ``x_1988stream_y``, ``x_1993stream_y`` ->
    ``1988stream``, ``1993stream``."""
    words = [re.split(r'(?<=[_.-])', name) for name in names]
    head = tail = 0
    if len(names) > 1:
        while all(len(w) - head > 1 and w[head] == words[0][head] for w in words):
            head += 1
        while all(len(w) - head - tail > 1 and w[-1 - tail] == words[0][-1 - tail]
                  for w in words):
            tail += 1
    return {name: ''.join(w[head:len(w) - tail]).strip('_.-') or name
            for name, w in zip(names, words)}


# ------------------------------------------------------------------ main --

def column_sources(loaded, column_of, field='source'):
    found = OrderedDict()
    for cycle, (_, info) in loaded.items():
        if info.get(field):
            found.setdefault(column_of(cycle), set()).add(info[field])
    return {column: next(iter(labels)) if len(labels) == 1 else 'mixed'
            for column, labels in found.items()}


def version_text(as_of, upto):
    parts = (([f'as of {as_of}'] if as_of else [])
             + ([f'up to install{upto[0]}/run{upto[1]}'] if upto else []))
    return ', '.join(parts) or 'latest'


def build_page(source, months, columns, column_of, version):
    """Load the months of one page (one version) into a merged table."""
    parts, evidence, sources, experiments, workflows = [], OrderedDict(), {}, {}, {}
    gsinfo, iterations = Counter(), set()
    present = expected = 0
    for month in months:
        loaded = source.load(month, version)
        parts.append(month_table(loaded, column_of))
        evidence.update(evidence_rows(loaded, column_of))
        sources.update(column_sources(loaded, column_of))
        experiments.update(column_sources(loaded, column_of, 'experiment'))
        workflows.update(column_sources(loaded, column_of, 'workflow'))
        expected += len(loaded)
        for results, info in loaded.values():
            present += results is not None
            if info['gsinfo']:
                gsinfo[info['gsinfo']] += 1
                iterations.add(info['iterations'])
    return {'table': merge_tables(parts, columns), 'evidence': evidence,
            'fill': {column: cell for _, fill in parts for column, cell in fill.items()},
            'sources': sources, 'experiments': experiments, 'workflows': workflows,
            'present': present, 'expected': expected,
            'gsinfo': gsinfo, 'iterations': iterations}


def filled_row(page, row, columns):
    """A row's cells, or the page's fill (not used / missing) if it has none."""
    return page['table'].get(row) or OrderedDict(
        (column, page['fill'][column]) for column in columns)


def gsinfo_parts(versions, limit=2):
    """Info parts naming the most common build_gsinfo versions of a page."""
    names = [name for name, _ in versions.most_common()]
    if not names:
        return ['no records']
    parts = names[:limit]
    if len(names) > limit:
        parts[-1] += f' (+{len(names) - limit} more)'
    return parts


def remove_figures(out, stem):
    """Delete the figures of an earlier run of a page; its number of parts may
    have changed (``<stem>_dashboard.png`` and ``<stem>_change.png`` are the
    single-figure names of earlier versions)."""
    pattern = re.compile(re.escape(stem) + r'_(dashboard|change|(%s)(_p\d+)?(_change)?)\.(%s)'
                         % ('|'.join(GROUP_FILES.values()), '|'.join(FORMATS)))
    for path in out.glob(f'{stem}_*'):
        if pattern.fullmatch(path.name):
            path.unlink()


def make_page(args, source, stem, months, columns, column_of, labels, style, state):
    out = Path(args.output_dir)
    signature = hashlib.sha1(json.dumps(
        [vars(args).get(key) for key in sorted(vars(args)) if key != 'changed_only']
        + [source.signature(month) for month in months], default=str).encode()
    ).hexdigest()
    status_csv, evidence_csv, summary_json = (
        out / f'{stem}_{name}' for name in ('status.csv', 'evidence.csv', 'summary.json'))
    if args.changed_only and state.get(stem) == signature and summary_json.is_file():
        return []

    page = build_page(source, months, columns, column_of, (args.as_of, args.upto))
    table = page['table'] if args.show_inactive else active_rows(page['table'])
    write_status(status_csv, table, args.resolution, labels)
    write_evidence(evidence_csv, page['evidence'], args.resolution, labels)
    remove_figures(out, stem)

    version = version_text(args.as_of, args.upto)
    span = (columns[0].strftime('%B %Y') if args.resolution == 'cycle'
            else f'{columns[0]:%Y-%m} to {columns[-1]:%Y-%m}')
    if args.resolution == 'cycle':
        xlabel = (f'Day of {columns[0]:%B %Y} (one column per cycle, '
                  f'every {args.cycle_hours} h)')
    else:
        xlabel = ('Year' if len({column.year for column in columns}) > 3 else 'Month') \
            + ' (one column per month)'
    iterations = '/'.join(sorted(page['iterations'])) or args.iterations
    created = dt.datetime.now(dt.timezone.utc)
    info = ([span, *gsinfo_parts(page['gsinfo']), f'outer iterations: {iterations}',
             f"cycles present: {page['present']}/{page['expected']}"]
            + ([f'version: {version}'] if args.archive else [])
            + [f'created {created:%Y-%m-%d}'])
    footnote = ('A sensor or data type is green in a cycle only if every channel, level '
                'or staged subtype intended for assimilation was assimilated in every '
                'outer iteration' + ('; a month is green only if every cycle is green.'
                                     if args.resolution == 'month' else '.'))
    strips = []
    if args.archive:
        strips.append(('cylc install/run (iN/rM)', page['sources'], short_run))
        workflows = set(page['workflows'].values())
        if len(workflows) > 1 or 'mixed' in workflows:  # e.g. a catch-up workflow
            strips.insert(0, ('cylc workflow', page['workflows'],
                              short_names(sorted(workflows - {'mixed'})).get))
        if len(args.streams) > 1:
            strips.insert(0, ('experiment', page['experiments'],
                              short_names([name for name, _ in args.streams]).get))
    cell_text = {(row, column): str(sum(cell.counts.get(key, 0) for key in FAILURES))
                 for row, cells in table.items() for column, cell in cells.items()
                 if args.resolution == 'month'
                 and any(cell.counts.get(key) for key in FAILURES)}
    figures = Chart(
        style, OrderedDict((row, {column: cell.status for column, cell in cells.items()})
                           for row, cells in table.items()),
        columns, args.resolution, STATUSES, {row: row_label(*row, labels) for row in table},
        title='GSI observation usage vs. build_gsinfo intent', subtitle=args.title,
        info=info, xlabel=xlabel, footnote=footnote,
        marks={row: any(cell.mismatch for cell in cells.values())
               for row, cells in table.items()},
        cell_text=cell_text, cell_note='numbers: failing cycles', strips=strips,
    ).save(out, stem)
    tables = [status_csv, evidence_csv]

    summary = {'title': args.title, 'created': created.isoformat(timespec='seconds'),
               'start': f'{columns[0]:%Y-%m}', 'end': f'{columns[-1]:%Y-%m}',
               'resolution': args.resolution, 'cycle_hours': args.cycle_hours,
               'cycles_expected': page['expected'], 'cycles_present': page['present'],
               **source.describe(), 'version': version, 'iterations': iterations,
               'build_gsinfo_versions': dict(page['gsinfo']),
               'sources': dict(Counter(page['sources'].values())),
               'workflows': dict(Counter(page['workflows'].values())),
               'conventional_variables_checked': list(CONVENTIONAL_VARIABLES),
               'statuses': {key: {'label': label, 'color': color}
                            for key, (label, color) in STATUSES.items()},
               'status_counts': OrderedDict()}

    if args.compare_as_of or args.compare_upto:
        base = build_page(source, months, columns, column_of,
                          (args.compare_as_of, args.compare_upto))
        rows = sorted(set(table) | set(base['table'] if args.show_inactive
                                       else active_rows(base['table'])), key=row_order)
        current = OrderedDict((row, filled_row(page, row, columns)) for row in rows)
        before = OrderedDict((row, filled_row(base, row, columns)) for row in rows)
        changes = OrderedDict((row, OrderedDict(
            (column, compare_cell(before[row][column], cell))
            for column, cell in cells.items())) for row, cells in current.items())
        base_text = version_text(args.compare_as_of, args.compare_upto)
        figures += Chart(
            style, changes, columns, args.resolution, CHANGES,
            {row: row_label(*row, labels) for row in changes},
            title='Change in GSI observation usage vs. build_gsinfo intent',
            subtitle=args.title, info=[span, f'baseline: {base_text}', f'current: {version}'],
            xlabel=xlabel, footnote='Statuses are compared cell by cell; failing = intended '
            'but not (fully) assimilated, or assimilated but not intended.',
        ).save(out, stem, '_change')
        tables.append(out / f'{stem}_changes.csv')
        write_changes(tables[-1], current, before, changes, args.resolution, labels,
                      page['sources'], base['sources'])
        summary['compare'] = {'baseline': base_text, 'change_counts': dict(Counter(
            change for cells in changes.values() for change in cells.values()))}

    for (group, _), cells in table.items():
        for column, cell in cells.items():
            period = summary['status_counts'].setdefault(
                column_name(column, args.resolution), {})
            period.setdefault(group, {}).setdefault(cell.status, 0)
            period[group][cell.status] += 1
    summary['figures'] = [path.name for path in figures]
    summary_json.write_text(json.dumps(summary, indent=2, default=str) + '\n')
    state[stem] = signature
    if not figures:
        print(f'{stem}: no rows to draw')
    return figures + tables + [summary_json]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[1],
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument('--archive', default=None,
                        help='obs usage archive written by obs_usage_record.py '
                             '(e.g. $OBS_USAGE_ARCHIVE_DIR)')
    inputs.add_argument('--gsistat-dir',
                        help='directory holding one gsistat log per cycle '
                             '(evaluated on the fly)')
    parser.add_argument('--experiment', nargs='+', metavar='NAME[@START]',
                        default=[os.environ['EXPERIMENT_NAME']]
                        if os.getenv('EXPERIMENT_NAME') else None,
                        help='experiment in the archive (default: $EXPERIMENT_NAME), or '
                             'several reanalysis streams, oldest first, joined into one '
                             'record: NAME@START (YYYY[-MM[-DD]]) uses a stream from '
                             'START on; otherwise the oldest stream that recorded a '
                             'cycle is used')
    parser.add_argument('--start', required=True, help='first month, YYYY-MM')
    parser.add_argument('--end', required=True, help='last month, YYYY-MM')
    parser.add_argument('--cycle-hours', type=int, default=6,
                        help='hours between analysis cycles (default: 6)')
    parser.add_argument('--resolution', choices=('cycle', 'month'), default='cycle',
                        help='one column per cycle with one page per month '
                             '(default), or one column per month on one page')
    version = parser.add_argument_group('versions (--archive only)')
    version.add_argument('--as-of', help='ignore records created after this UTC time, '
                         'e.g. 2026-11-19T21:00')
    version.add_argument('--upto', help='ignore records of later cylc installs/runs '
                         'than this one, e.g. install0/run1')
    version.add_argument('--compare-as-of', help='also draw what changed since the '
                         'version as of this time')
    version.add_argument('--compare-upto', help='also draw what changed since this '
                         'install/run, e.g. install0/run1')
    direct = parser.add_argument_group('gsistat directory (--gsistat-dir only)')
    direct.add_argument('--file-format',
                        default=os.getenv('OBS_USAGE_FILE_FORMAT') or DEFAULT_FILE_FORMAT,
                        help='strftime pattern of gsistat file names relative to '
                             '--gsistat-dir (default: $OBS_USAGE_FILE_FORMAT or '
                             f"{DEFAULT_FILE_FORMAT.replace('%', '%%')!r})")
    direct.add_argument('--gsinfo', default=os.getenv('OBS_USAGE_GSINFO_URL')
                        or DEFAULT_GSINFO_URL,
                        help='build_gsinfo git URL or local clone '
                             f'(default: $OBS_USAGE_GSINFO_URL or {DEFAULT_GSINFO_URL})')
    direct.add_argument('--gsinfo-ref', default=os.getenv('OBS_USAGE_GSINFO_REF',
                                                          DEFAULT_GSINFO_REF),
                        help='build_gsinfo branch, tag or commit (default: '
                             f'$OBS_USAGE_GSINFO_REF or {DEFAULT_GSINFO_REF}); '
                             "'' reads a local clone's working tree as-is")
    direct.add_argument('--cache-dir', default=None,
                        help='where a build_gsinfo URL is cloned '
                             '(default: $OBS_USAGE_CACHE or ~/.cache/score-monitoring)')
    direct.add_argument('--iterations', choices=('all', 'first', 'last'), default='all',
                        help='GSI outer iterations that must show assimilation '
                             '(default: all reported iterations)')
    parser.add_argument('--show-inactive', action='store_true',
                        help='also plot rows that are never intended for use '
                             'and never used')
    parser.add_argument('--changed-only', action='store_true',
                        help='skip pages whose inputs did not change since the '
                             'last run into the same --output-dir')
    parser.add_argument('--title', default=None,
                        help='chart title (default: the experiment name)')
    parser.add_argument('--output-dir', default='.',
                        help='where figures, CSVs and JSON are written (default: .)')
    figures = parser.add_argument_group(
        'figures', 'One figure per group and page, sized for AMS journals; long groups '
        'are split into parts at most 9 in deep.')
    figures.add_argument('--width', type=float, choices=AMS_WIDTHS, default=6.5,
                         help='width in inches: 3.2 (one column), 4.5, 5.5 (two columns) '
                              'or 6.5 (default)')
    figures.add_argument('--font-size', type=float, default=7.0,
                         help='text size in points (default: 7; titles are 1-2 pt larger)')
    figures.add_argument('--format', nargs='+', choices=FORMATS, default=['png'],
                         help='file formats (default: png); pdf, svg and eps are vector '
                              'graphics with embedded fonts')
    figures.add_argument('--dpi', type=int, default=300,
                         help='resolution of png and tif (default: 300)')
    figures.add_argument('--font', default=os.getenv('OBS_USAGE_FONT'),
                         help='font to use (default: $OBS_USAGE_FONT, else the first '
                              'installed of Inter (CIRES), Helvetica Neue, Helvetica, '
                              'Arial, Liberation Sans, DejaVu Sans)')
    figures.add_argument('--font-dir', default=os.getenv('OBS_USAGE_FONT_DIR'),
                         help='directory of .ttf fonts to add, e.g. the static Inter '
                              'fonts (default: $OBS_USAGE_FONT_DIR)')
    args = parser.parse_args(argv)

    if args.dpi < 72 or not 5 <= args.font_size <= 12:
        parser.error('--dpi must be at least 72 and --font-size between 5 and 12')
    if args.archive and not args.experiment:
        parser.error('--archive needs --experiment (or $EXPERIMENT_NAME)')
    if not args.archive and any((args.as_of, args.upto, args.compare_as_of,
                                 args.compare_upto)):
        parser.error('version options need --archive')
    args.streams = []
    if args.archive:
        args.archive = os.path.expandvars(os.path.expanduser(args.archive))
        try:
            args.streams = [parse_stream(text) for text in args.experiment]
        except ValueError as error:
            parser.error(str(error))
        names = [name for name, _ in args.streams]
        starts = [start for _, start in args.streams if start is not None]
        if len(set(names)) < len(names):
            parser.error('an experiment is listed twice')
        if any(later <= earlier for earlier, later in zip(starts, starts[1:])):
            parser.error('list the experiments oldest first (increasing @START)')
        if len(names) > 1 and (args.upto or args.compare_upto):
            parser.error('--upto/--compare-upto need a single experiment (install/run '
                         'numbers belong to one); use --as-of/--compare-as-of')
    try:
        for name in ('as_of', 'compare_as_of'):
            if getattr(args, name):
                parse_time(getattr(args, name))
        for name in ('upto', 'compare_upto'):
            if getattr(args, name):
                setattr(args, name, parse_source(getattr(args, name)))
        months = month_range(parse_month(args.start), parse_month(args.end))
    except ValueError as error:
        parser.error(str(error))
    if args.title is None:
        names = [name for name, _ in args.streams]
        args.title = (', '.join(names) if len(names) <= 2 else
                      f'{names[0]} ... {names[-1]} ({len(names)} experiments)')

    source = ArchiveSource(args) if args.archive else GsistatDirSource(args)
    labels = conventional_labels()
    style = figure_style(args.width, args.font_size, args.dpi, args.format, args.font,
                         args.font_dir)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    state_path = out / STATE_FILE
    try:
        state = json.loads(state_path.read_text())
    except (OSError, ValueError):
        state = {}

    written = []
    try:
        if args.resolution == 'cycle':
            for month in months:
                written += make_page(args, source,
                                     f'obs_usage_{month:%Y%m}_{args.cycle_hours}h', [month],
                                     month_cycles(month, args.cycle_hours),
                                     lambda cycle: cycle, labels, style, state)
        else:
            written += make_page(args, source, f'obs_usage_{months[0]:%Y%m}_'
                                 f'{months[-1]:%Y%m}_monthly', months, months,
                                 lambda cycle: cycle.replace(day=1, hour=0), labels, style,
                                 state)
    except FigureSizeError as error:
        parser.error(str(error))
    state_path.write_text(json.dumps(state, indent=1) + '\n')
    for path in written:
        print(f'wrote {path}')
    if not written:
        print('nothing changed; no pages written')
    return written


if __name__ == '__main__':
    main()
