#!/usr/bin/env python

"""Copyright 2026 NOAA
All rights reserved.

Observation usage dashboard: a stop-light chart of every satellite sensor,
ozone sensor and conventional data type, comparing the intended usage in
build_gsinfo (default: NOAA-PSL/build_gsinfo-fix, branch psl-develop) with
what GSI actually did according to the gsistat log of every cycle. By
default every 6-hourly cycle is one column and one page is drawn per month;
``--resolution month`` rolls cycles up into a conservative monthly overview
(one column per month).

Example (one gsistat file per cycle in a local directory):

    python obs_usage_dashboard.py --gsistat-dir /path/to/gsistats \\
        --start 2015-05 --end 2015-05 --output-dir dashboard

Change the build_gsinfo branch/tag/commit with ``--gsinfo-ref`` and point to
another repository URL (e.g. NOAA-PSL/build_gsinfo) or a local clone with
``--gsinfo``. See README.md for details.
"""

import argparse
import ast
import csv
import datetime as dt
import json
import os
import sys
from collections import OrderedDict, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from obs_usage_gsinfo import (DEFAULT_GSINFO_REF, DEFAULT_GSINFO_URL,  # noqa: E402
                              GsinfoInventory, GsinfoSource)
from obs_usage_gsistat import parse_gsistat  # noqa: E402
from obs_usage_status import (ABSENT, CONVENTIONAL, CONVENTIONAL_VARIABLES,  # noqa: E402
                              GROUPS, MISSING, MONITORED, NOT_ASSIMILATED,
                              NOT_STAGED, OFF, OK_ASSIMILATED, PARTIAL,
                              STATUSES, UNEXPECTED, CycleResult,
                              aggregate_cycles, evaluate_cycle)

DEFAULT_FILE_FORMAT = 'gsistats.%Y%m%d%H_control'
FAILURES = (UNEXPECTED, NOT_ASSIMILATED, NOT_STAGED, PARTIAL)
OUTCOMES = (OK_ASSIMILATED, PARTIAL, NOT_ASSIMILATED, NOT_STAGED, UNEXPECTED,
            MONITORED, ABSENT, MISSING)
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


def parse_month(text):
    return dt.datetime.strptime(text, '%Y-%m')


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


def evaluate_period(args, inventory):
    """Parse and evaluate every expected cycle.

    Returns ``(months, cycle_results, cycle_files)`` where ``cycle_results``
    maps cycle -> ``{row: CycleResult}`` or ``None`` when missing/unreadable.
    """
    months = month_range(parse_month(args.start), parse_month(args.end))
    aliases = inventory.obs_input_names()
    cycle_results, cycle_files = OrderedDict(), OrderedDict()
    for month in months:
        for cycle in month_cycles(month, args.cycle_hours):
            path = Path(args.gsistat_dir) / cycle.strftime(args.file_format)
            if not path.is_file():
                cycle_results[cycle], cycle_files[cycle] = None, (str(path), 'missing')
                continue
            usage = parse_gsistat(path)
            if not usage.valid:
                cycle_results[cycle] = None
                cycle_files[cycle] = (str(path), 'unreadable: ' + '; '.join(
                    usage.warnings or ['required gsistat sections not found']))
                continue
            satinfo, _ = inventory.satinfo(cycle)
            ozinfo, _ = inventory.ozinfo(cycle)
            convinfo, _ = inventory.convinfo(cycle)
            cycle_results[cycle] = evaluate_cycle(
                usage, satinfo, ozinfo, convinfo, aliases, args.iterations)
            cycle_files[cycle] = (str(path), 'ok')
    return months, cycle_results, cycle_files


def build_table(columns, column_of, cycle_results):
    """Return ``{row: {column: (CellStatus, [(cycle, CycleResult|None)])}}``.

    ``column_of`` maps a cycle to its column (the cycle itself, or its month).
    """
    rows = sorted({row for results in cycle_results.values() if results
                   for row in results}, key=row_order)
    members = defaultdict(list)
    for cycle in cycle_results:
        members[column_of(cycle)].append(cycle)
    table = OrderedDict()
    for row in rows:
        table[row] = OrderedDict()
        for column in columns:
            entries = [(cycle, None if cycle_results[cycle] is None else
                        cycle_results[cycle].get(row, CycleResult(OFF, ABSENT)))
                       for cycle in members[column]]
            table[row][column] = (aggregate_cycles([e[1] for e in entries]), entries)
    return table


def active_rows(table, columns):
    """Drop rows that are 'not intended and not used' in every present cycle."""
    return OrderedDict(
        (row, by_column) for row, by_column in table.items()
        if any(result is not None and result.outcome != ABSENT
               for column in columns for _, result in by_column[column][1]))


def column_name(column, resolution):
    return column.strftime('%Y%m%d%H' if resolution == 'cycle' else '%Y-%m')


def write_outputs(args, table, columns, cycle_files, labels, provenance, stem):
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    status_path = out / f'{stem}_status.csv'
    with status_path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['group', 'row', 'label', 'period', 'status', 'status_label',
                         'intent', 'cycles_expected', 'cycles_present',
                         *(f'cycles_{key}' for key in OUTCOMES if key != MISSING),
                         'run_flags_differ_from_build_gsinfo'])
        for (group, name), by_column in table.items():
            for column in columns:
                status = by_column[column][0]
                writer.writerow([
                    group, name, row_label(group, name, labels),
                    column_name(column, args.resolution), status.status,
                    STATUSES[status.status][0], '/'.join(status.intents),
                    status.expected, status.present,
                    *(status.counts.get(key, 0) for key in OUTCOMES if key != MISSING),
                    int(status.mismatch)])

    evidence_path = out / f'{stem}_evidence.csv'
    with evidence_path.open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['group', 'row', 'label', 'cycle', 'intent', 'outcome',
                         'status', 'reason', 'run_flags_vs_build_gsinfo', 'file'])
        for cycle, (path, state) in cycle_files.items():
            if state != 'ok':
                writer.writerow(['*', '*', 'all rows', cycle.strftime('%Y%m%d%H'),
                                 '', MISSING, 'missing', state, '', path])
        for (group, name), by_column in table.items():
            for column in columns:
                for cycle, result in by_column[column][1]:
                    if result is None or not (result.outcome in FAILURES or result.mismatch):
                        continue
                    writer.writerow([
                        group, name, row_label(group, name, labels),
                        cycle.strftime('%Y%m%d%H'), result.intent, result.outcome,
                        aggregate_cycles([result]).status, ' | '.join(result.reasons),
                        ' | '.join(result.mismatch), cycle_files[cycle][0]])

    summary = dict(provenance)
    counts = summary['status_counts'] = OrderedDict()
    for (group, _), by_column in table.items():
        for column in columns:
            period = counts.setdefault(column_name(column, args.resolution), {})
            status = by_column[column][0].status
            period.setdefault(group, {}).setdefault(status, 0)
            period[group][status] += 1
    summary_path = out / f'{stem}_summary.json'
    summary_path.write_text(json.dumps(summary, indent=2, default=str) + '\n')
    return status_path, evidence_path, summary_path


def _text_color(hex_color):
    red, green, blue = (int(hex_color[i:i + 2], 16) / 255 for i in (1, 3, 5))
    return 'black' if 0.299 * red + 0.587 * green + 0.114 * blue > 0.6 else 'white'


def _x_ticks(columns, resolution):
    if resolution == 'cycle':
        days = [(j, column) for j, column in enumerate(columns) if column.hour == 0]
        step = max(1, round(len(days) / 31))
        return ([j + 0.5 for j, _ in days[::step]],
                [str(column.day) for _, column in days[::step]],
                [j for j, _ in days if j])
    step = max(1, len(columns) // 12)
    return ([j + 0.5 for j in range(0, len(columns), step)],
            [columns[j].strftime('%Y-%m') for j in range(0, len(columns), step)],
            [j for j, column in enumerate(columns) if j and column.month == 1])


def plot_dashboard(table, columns, resolution, labels, provenance, path, title=''):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib.colors import ListedColormap
    from matplotlib.patches import Patch

    keys = list(STATUSES)
    cmap = ListedColormap([STATUSES[key][1] for key in keys])
    panels = [[row for row in table if row[0] == group] for group in GROUPS]
    panels = [panel for panel in panels if panel]
    # radiance + ozone in one column and conventional in another keeps the
    # chart on a single page
    stacks = [[p for p in panels if p[0][0] != CONVENTIONAL],
              [p for p in panels if p[0][0] == CONVENTIONAL]]
    stacks = [stack for stack in stacks if stack]
    gap = 3.0  # rows of space for a panel title and x tick labels
    n_rows = max([sum(len(p) + gap for p in stack) for stack in stacks] or [1])
    cell_h = 0.14
    cell_w = 0.05 if resolution == 'cycle' else 0.32
    label_w = 2.3
    data_w = max(len(columns) * cell_w, 0.96)
    panel_w = label_w + data_w + 0.3
    fig_w = max(8.0, panel_w * len(stacks) + 0.3)
    legend_h = 1.0
    footer_h = 0.3
    fig_h = n_rows * cell_h + 0.85 + legend_h + footer_h
    fig = plt.figure(figsize=(fig_w, fig_h))
    top = 1 - (0.85 + gap * cell_h) / fig_h
    ticks, tick_labels, separators = _x_ticks(columns, resolution)
    if resolution == 'cycle':
        period = columns[0].strftime('%B %Y') if columns else ''
        x_title = f'{period}: day of month (one column per cycle, ticks at 00 UTC)'
    else:
        x_title = 'month'

    for s, stack in enumerate(stacks):
        left = (s * panel_w + label_w + 0.2) / fig_w
        width = data_w / fig_w
        y_top = top
        for panel in stack:
            n = len(panel)
            height = n * cell_h / fig_h
            ax = fig.add_axes([left, y_top - height, width, height])
            y_top -= height + gap * cell_h / fig_h
            matrix = np.array([[keys.index(table[row][column][0].status)
                                for column in columns] for row in panel])
            ax.pcolormesh(np.arange(len(columns) + 1), np.arange(n + 1), matrix,
                          cmap=cmap, vmin=-0.5, vmax=len(keys) - 0.5)
            ax.hlines(np.arange(1, n), 0, len(columns), color='0.75', linewidth=0.3)
            ax.vlines(separators, 0, n, color='0.55', linewidth=0.4)
            if resolution == 'month' and len(columns) <= 24:
                for i, row in enumerate(panel):
                    for j, column in enumerate(columns):
                        status = table[row][column][0]
                        failing = sum(status.counts.get(k, 0) for k in FAILURES)
                        if failing:
                            ax.text(j + 0.5, i + 0.5, str(failing), ha='center',
                                    va='center', fontsize=4.5,
                                    color=_text_color(STATUSES[status.status][1]))
            ax.set_xlim(0, len(columns))
            ax.set_ylim(n, 0)
            ax.set_yticks(np.arange(n) + 0.5)
            ax.set_yticklabels([
                row_label(*row, labels) + (' \u25c6' if any(
                    table[row][column][0].mismatch for column in columns) else '')
                for row in panel], fontsize=5.5)
            ax.set_xticks(ticks)
            ax.set_xticklabels(tick_labels, fontsize=4.5 if resolution == 'cycle' else 6,
                               rotation=0 if resolution == 'cycle' or len(columns) <= 6
                               else 90)
            ax.xaxis.tick_top()
            ax.tick_params(length=0, pad=1.5)
            for spine in ax.spines.values():
                spine.set_color('0.4')
                spine.set_linewidth(0.5)
            ax.set_title(f'{panel[0][0]} ({n})', fontsize=7, fontweight='bold',
                         loc='left', pad=8)

    handles = [Patch(facecolor=color, edgecolor='0.3', linewidth=0.5, label=label)
               for label, color in STATUSES.values()]
    handles.append(Patch(facecolor='none', edgecolor='none',
                         label='\u25c6 after a name: run usage flags differ from build_gsinfo'))
    fig.legend(handles=handles, loc='lower center', ncol=2, fontsize=6,
               frameon=False, bbox_to_anchor=(0.5, 0.12 / fig_h))
    gsinfo = provenance['build_gsinfo']
    fig.text(0.5, 1 - 0.12 / fig_h, 'GSI observation usage vs. build_gsinfo intent',
             ha='center', va='top', fontsize=10, fontweight='bold')
    if title:
        fig.text(0.5, 1 - 0.35 / fig_h, title, ha='center', va='top', fontsize=8,
                 fontweight='bold')
    present = sum(table[row][column][0].present
                  for row in list(table)[:1] for column in columns)
    expected = sum(table[row][column][0].expected
                   for row in list(table)[:1] for column in columns)
    fig.text(0.5, 1 - 0.55 / fig_h,
             f"{gsinfo['name']} {gsinfo['ref']} @ {gsinfo['commit'][:7]}  |  "
             f"outer iterations: {provenance['iterations']}  |  "
             f"cycles present {present}/{expected}  |  {x_title}"
             + ('  |  numbers = failing cycles' if resolution == 'month' else '')
             + f"  |  created {provenance['created'][:10]}",
             ha='center', va='top', fontsize=6, color='0.3')
    fig.text(0.5, (legend_h + 0.05) / fig_h,
             'A sensor or data type is green in a cycle only if every channel, level or '
             'staged subtype intended for assimilation was assimilated in every outer '
             'iteration' + ('; a month is green only if every cycle is green.'
                            if resolution == 'month' else '.'),
             ha='center', va='bottom', fontsize=5.5, color='0.3')
    fig.savefig(path, dpi=200)
    plt.close(fig)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[1],
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--gsistat-dir', required=True,
                        help='directory holding one gsistat log per cycle')
    parser.add_argument('--start', required=True, help='first month, YYYY-MM')
    parser.add_argument('--end', required=True, help='last month, YYYY-MM')
    parser.add_argument('--file-format',
                        default=os.getenv('GSI_FIT_FILE_NAME_FORMAT') or DEFAULT_FILE_FORMAT,
                        help='strftime pattern of gsistat file names relative to '
                             '--gsistat-dir (default: $GSI_FIT_FILE_NAME_FORMAT or '
                             f"{DEFAULT_FILE_FORMAT.replace('%', '%%')!r})")
    parser.add_argument('--cycle-hours', type=int, default=6,
                        help='hours between analysis cycles (default: 6)')
    parser.add_argument('--resolution', choices=('cycle', 'month'), default='cycle',
                        help='one column per cycle with one page per month '
                             '(default), or one column per month on one page')
    parser.add_argument('--gsinfo', default=DEFAULT_GSINFO_URL,
                        help='build_gsinfo git URL or local clone '
                             f'(default: {DEFAULT_GSINFO_URL})')
    parser.add_argument('--gsinfo-ref', default=DEFAULT_GSINFO_REF,
                        help='build_gsinfo branch, tag or commit '
                             f'(default: {DEFAULT_GSINFO_REF}); '
                             "'' reads a local clone's working tree as-is")
    parser.add_argument('--cache-dir', default=None,
                        help='where a build_gsinfo URL is cloned '
                             '(default: ~/.cache/score-monitoring)')
    parser.add_argument('--iterations', choices=('all', 'first', 'last'), default='all',
                        help='GSI outer iterations that must show assimilation '
                             '(default: all reported iterations)')
    parser.add_argument('--show-inactive', action='store_true',
                        help='also plot rows that are never intended for use '
                             'and never used')
    parser.add_argument('--title', default='', help='experiment name for the chart title')
    parser.add_argument('--output-dir', default='.',
                        help='where figures, CSVs and JSON are written (default: .)')
    args = parser.parse_args(argv)

    source = GsinfoSource(args.gsinfo, args.gsinfo_ref, args.cache_dir)
    inventory = GsinfoInventory(source)
    months, cycle_results, cycle_files = evaluate_period(args, inventory)
    if args.resolution == 'cycle':
        columns, column_of = list(cycle_results), (lambda cycle: cycle)
        pages = [(f'obs_usage_{month:%Y%m}_{args.cycle_hours}h',
                  [c for c in columns if (c.year, c.month) == (month.year, month.month)])
                 for month in months]
        stem = (f'obs_usage_{parse_month(args.start):%Y%m}_'
                f'{parse_month(args.end):%Y%m}_{args.cycle_hours}h')
    else:
        columns, column_of = months, (lambda cycle: cycle.replace(day=1, hour=0))
        stem = (f'obs_usage_{parse_month(args.start):%Y%m}_'
                f'{parse_month(args.end):%Y%m}_monthly')
        pages = [(stem, columns)]
    table = build_table(columns, column_of, cycle_results)
    if not args.show_inactive:
        table = active_rows(table, columns)
    labels = conventional_labels()
    present = sum(result is not None for result in cycle_results.values())
    provenance = {
        'title': args.title,
        'created': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'),
        'gsistat_dir': str(args.gsistat_dir),
        'file_format': args.file_format,
        'start': args.start, 'end': args.end, 'cycle_hours': args.cycle_hours,
        'resolution': args.resolution,
        'cycles_expected': len(cycle_results), 'cycles_present': present,
        'iterations': args.iterations,
        'build_gsinfo': source.describe(),
        'build_gsinfo_convinfo_files': sorted(inventory.convinfo_files),
        'build_gsinfo_warnings': inventory.warnings,
        'conventional_variables_checked': list(CONVENTIONAL_VARIABLES),
        'statuses': {key: {'label': label, 'color': color}
                     for key, (label, color) in STATUSES.items()},
    }
    outputs = write_outputs(args, table, columns, cycle_files, labels, provenance, stem)
    images = []
    for name, page_columns in pages:
        page = table if args.show_inactive else active_rows(table, page_columns)
        images.append(Path(args.output_dir) / f'{name}_dashboard.png')
        plot_dashboard(page, page_columns, args.resolution, labels, provenance,
                       images[-1], args.title)
    print(f'cycles present: {present}/{len(cycle_results)}; rows: {len(table)}')
    for path in (*images, *outputs):
        print(f'wrote {path}')


if __name__ == '__main__':
    main()
