"""Tests for the observation usage dashboard (scripts/obs_usage_*.py)."""

import csv
import datetime as dt
import gzip
import multiprocessing
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))

import obs_usage_dashboard  # noqa: E402
import obs_usage_record  # noqa: E402
from obs_usage_archive import (append_record, make_record, month_path,  # noqa: E402
                               parse_workflow_id, read_month, select_latest)
from obs_usage_figures import (Chart, FigureSizeError, FigureStyle, paginate,  # noqa: E402
                               source_strip)
from obs_usage_gsinfo import GsinfoInventory, GsinfoSource  # noqa: E402
from obs_usage_gsistat import parse_lines  # noqa: E402
from obs_usage_status import (CONVENTIONAL, OZONE, RADIANCE,  # noqa: E402
                              CycleResult, aggregate_cycles, evaluate_cycle)

STAT = '   1.000   0.1   0.2   0.3   0.4   0.5'


def gsistat(amsua_nobs=(500, 400), ssmis_read=0, ps187_asm=0):
    """A minimal gsistat log with one outer iteration."""
    lines = [
        'RADINFO_READ:  jpch_rad=   4',
        '    1 amsua_n15            chan=     1 var=   1.0 use=  1 ermax=   2.5',
        '    2 amsua_n15            chan=     2 var=   1.0 use=  1 ermax=   2.5',
        '    3 hirs4_n19            chan=     1 var=   1.0 use= -1 ermax=   2.5',
        '    4 ssmis_f16            chan=     1 var=   1.0 use=  1 ermax=   2.5',
        'OZINFO_READ:  jpch_oz=    1',
        '  1 sbuv8_n19            lev =    1 use = -1 pob =     0.000',
        'current fit of surface pressure data, ranges in mb',
        ' o-g 01      ps asm 120 0000       609 -0.362E+00  0.115E+01',
        ' o-g 01      ps mon 181 0000        20 -0.362E+00  0.115E+01',
        f' o-g 01      ps asm 187 0000  {ps187_asm:8d} -0.362E+00  0.115E+01',
        f'    1     1 amsua_n15     {amsua_nobs[0]:6d}    10{STAT}',
        f'    2     2 amsua_n15     {amsua_nobs[1]:6d}    10{STAT}',
        f'    3     1 hirs4_n19        300    10{STAT}',
        '    it      satellite instrument       # read       # keep      # assim',
        f'o-g 01 rad   n15          amsua   1000  900  {sum(amsua_nobs)}  1.0 1.0 0.2 0.2',
        'o-g 01 rad   n19          hirs4    800  700  0  1.0 1.0 0.2 0.2',
        f'o-g 01 rad   f16          ssmis   {ssmis_read}  0  0  1.0 1.0 0.2 0.2',
        'o-g 01 oz n19         sbuv2           7810      7744         0   0.0  0.0',
    ]
    return [line + '\n' for line in lines]


def git(repo, *args):
    subprocess.run(['git', '-C', str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def gsinfo_repo(tmp_path):
    """A tiny build_gsinfo-like git repository."""
    repo = tmp_path / 'build_gsinfo'
    files = {
        'satinfo/satellites': 'amsua_n15\nhirs4_n19\nssmis_f16\n',
        'satinfo/amsua_n15/1900010100': 'amsua_n15 1 1\namsua_n15 2 1\n',
        'satinfo/amsua_n15/2020010100': 'amsua_n15 1 1\namsua_n15 2 -1\n',
        'satinfo/hirs4_n19/1900010100': '!sensor chan iuse\nhirs4_n19 1 -1\n',
        'satinfo/ssmis_f16/1900010100': 'ssmis_f16 1 1\n',
        'ozinfo/satellites': 'sbuv8_n19\n',
        'ozinfo/sbuv8_n19/1900010100': 'sbuv8_n19 1 -1\n',
        'obs_input/obs_input.txt': 'sbuvbufr sbuv2 n19 sbuv8_n19 0.0 0 0\n',
        'convinfo/merged_convinfo.txt': ('! otype type sub iuse\n ps 120 0 1\n'
                                         ' ps 181 0 -1\n ps 187 0 -1\n t 120 0 1\n'),
        'convinfo/2020010100': ' ps 120 0 1\n ps 187 0 1\n',
    }
    for name, text in files.items():
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(text)
    os.symlink('merged_convinfo.txt', repo / 'convinfo' / '1900010100')
    git(repo, 'init', '-q')
    git(repo, 'add', '-A')
    git(repo, '-c', 'user.name=t', '-c', 'user.email=t@t', 'commit', '-qm', 'init')
    return repo


def evaluate(lines, repo, when=dt.datetime(2015, 5, 1)):
    inventory = GsinfoInventory(GsinfoSource(repo, 'HEAD'))
    return evaluate_cycle(parse_lines(lines), inventory.satinfo(when)[0],
                          inventory.ozinfo(when)[0], inventory.convinfo(when)[0],
                          inventory.obs_input_names())


def status(results, group, name):
    return aggregate_cycles([results[(group, name)]]).status


def test_parser_reads_usage_evidence():
    usage = parse_lines(gsistat())
    assert usage.valid
    assert usage.rad_config['amsua_n15'] == {1: 1, 2: 1}
    assert usage.oz_config['sbuv8_n19'] == {1: -1}
    assert usage.rad_channels[1][('amsua_n15', 2)] == 400
    assert usage.rad_summary[1][('n15', 'amsua')] == (1000, 900, 900)
    assert usage.oz_summary[1][('n19', 'sbuv2')] == (7810, 7744, 0)
    assert usage.conventional[1][('ps', 'mon', 181, 0)] == 20
    assert not parse_lines(gsistat()[:7]).valid


@pytest.mark.parametrize('ref', ['HEAD', ''])
def test_gsinfo_resolves_dated_files_and_symlinks(gsinfo_repo, ref):
    inventory = GsinfoInventory(GsinfoSource(gsinfo_repo, ref))
    early, late = dt.datetime(2015, 5, 1), dt.datetime(2021, 1, 1)
    assert inventory.satinfo(early)[0]['amsua_n15'] == {1: 1, 2: 1}
    assert inventory.satinfo(late)[0]['amsua_n15'] == {1: 1, 2: -1}
    convinfo, path = inventory.convinfo(early)
    assert path == 'convinfo/1900010100' and convinfo[('t', 120)] == {0: 1}
    convinfo, path = inventory.convinfo(late)
    assert path == 'convinfo/2020010100' and ('t', 120) not in convinfo


def test_new_gsinfo_ref_is_fetched_despite_throttle(gsinfo_repo, tmp_path, monkeypatch):
    monkeypatch.delenv('OBS_USAGE_GSINFO_REFRESH_MINUTES', raising=False)
    url, cache = f'file://{gsinfo_repo}', tmp_path / 'cache'
    head = GsinfoSource(url, 'HEAD', cache_dir=cache).commit
    git(gsinfo_repo, 'tag', 'v2')
    assert GsinfoSource(url, 'v2', cache_dir=cache).commit == head
    with pytest.raises(ValueError, match='not found'):
        GsinfoSource(url, 'no-such-ref', cache_dir=cache)


def test_cycle_statuses(gsinfo_repo):
    results = evaluate(gsistat(), gsinfo_repo)
    assert status(results, RADIANCE, 'amsua_n15') == 'assimilated'
    assert status(results, RADIANCE, 'hirs4_n19') == 'monitored'
    assert status(results, RADIANCE, 'ssmis_f16') == 'not_used'
    assert status(results, OZONE, 'sbuv8_n19') == 'monitored'
    assert status(results, CONVENTIONAL, 'ps 120') == 'assimilated'
    assert status(results, CONVENTIONAL, 'ps 181') == 'monitored'
    assert status(results, CONVENTIONAL, 't 120') == 'not_used'


def test_partial_unexpected_and_staged_not_assimilated(gsinfo_repo):
    results = evaluate(gsistat(amsua_nobs=(500, 0), ssmis_read=50, ps187_asm=7),
                       gsinfo_repo)
    assert status(results, RADIANCE, 'amsua_n15') == 'partial'
    assert status(results, RADIANCE, 'ssmis_f16') == 'not_used'
    assert status(results, CONVENTIONAL, 'ps 187') == 'unexpected'
    # one intended channel failing is a sensor failure in that cycle
    results = evaluate(gsistat(amsua_nobs=(0, 0)), gsinfo_repo)
    assert status(results, RADIANCE, 'amsua_n15') == 'not_used'


def test_rollup_is_conservative():
    ok = CycleResult('assimilate', 'assimilated')
    assert aggregate_cycles([ok, ok]).status == 'assimilated'
    assert aggregate_cycles([ok, None]).status == 'missing'
    assert aggregate_cycles([ok, None, CycleResult('assimilate', 'not_staged')]
                            ).status == 'not_used'
    assert aggregate_cycles([CycleResult('monitor', 'monitored'),
                             CycleResult('monitor', 'absent')]).status == 'monitored'


def test_dashboard_end_to_end(gsinfo_repo, tmp_path):
    logs = tmp_path / 'gsistats'
    logs.mkdir()
    (logs / 'gsistats.2015050100_control').write_text(''.join(gsistat()))
    (logs / 'gsistats.2015050106_control').write_text(
        ''.join(gsistat(amsua_nobs=(500, 0))))
    (logs / 'gsistats.2015050112_control').write_text('truncated\n')
    out = tmp_path / 'out'
    out.mkdir()
    stale = out / 'obs_usage_201505_6h_dashboard.png'  # single figure of old versions
    stale.write_bytes(b'')
    obs_usage_dashboard.main(['--gsistat-dir', str(logs), '--start', '2015-05',
                              '--end', '2015-05', '--gsinfo', str(gsinfo_repo),
                              '--gsinfo-ref', 'HEAD', '--output-dir', str(out),
                              '--width', '3.2', '--format', 'png', 'pdf'])
    assert not stale.exists()
    from PIL import Image
    for group in ('radiance', 'ozone', 'conventional'):  # AMS one-column figures
        with Image.open(out / f'obs_usage_201505_6h_{group}.png') as image:
            assert image.width == 3.2 * 300 and image.height <= 9 * 300
        assert (out / f'obs_usage_201505_6h_{group}.pdf').stat().st_size > 0
    rows = {(row['row'], row['period']): row['status'] for row in
            csv.DictReader((out / 'obs_usage_201505_6h_status.csv').open())}
    assert rows[('amsua_n15', '2015050100')] == 'assimilated'
    assert rows[('amsua_n15', '2015050106')] == 'partial'
    assert rows[('amsua_n15', '2015050112')] == 'missing'
    evidence = (out / 'obs_usage_201505_6h_evidence.csv').read_text()
    assert 'unreadable' in evidence and '1/2 intended channels' in evidence

    obs_usage_dashboard.main(['--gsistat-dir', str(logs), '--start', '2015-05',
                              '--end', '2015-05', '--gsinfo', str(gsinfo_repo),
                              '--gsinfo-ref', 'HEAD', '--resolution', 'month',
                              '--output-dir', str(out)])
    rows = {row['row']: row['status'] for row in csv.DictReader(
        (out / 'obs_usage_201505_201505_monthly_status.csv').open())}
    assert rows['amsua_n15'] == 'partial' and rows['hirs4_n19'] == 'missing'


@pytest.mark.parametrize('workflow_id, expected', [
    ('score-monitoring/cylc_scout_sfs/test_sfs_2023_gdas_gsi_install0/run2',
     ('test_sfs_2023_gdas_gsi', 0, 2)),
    ('reanl_install3/run1', ('reanl', 3, 1)),
    ('reanl', ('reanl', 0, 0)),
])
def test_workflow_id(workflow_id, expected):
    info = parse_workflow_id(workflow_id)
    assert (info['workflow'], info['install'], info['run']) == expected


def test_cycle_points():
    parse = obs_usage_record.parse_cycle
    assert parse('20230101T12') == dt.datetime(2023, 1, 1, 12)
    assert parse('20230103T0600Z') == dt.datetime(2023, 1, 3, 6)
    assert parse('2023010318') == dt.datetime(2023, 1, 3, 18)
    with pytest.raises(ValueError):
        parse('2023-01-03')


def test_archive_keeps_versions_and_selects_latest(tmp_path, capsys):
    def append(hour, outcome, workflow_id, created):
        record = make_record(dt.datetime(2023, 1, 3, hour),
                             {(RADIANCE, 'amsua_n15'): CycleResult('assimilate', outcome)},
                             source=parse_workflow_id(workflow_id))
        append_record(tmp_path, 'expt', record | {'created': created})

    append(0, 'partial', 'flow_install0/run1', '2026-01-01T00:00:00Z')
    append(6, 'partial', 'flow_install0/run1', '2026-01-01T06:00:00Z')
    append(6, 'assimilated', 'flow_install0/run2', '2026-01-02T00:00:00Z')  # rewind
    append(6, 'not_staged', 'flow_install1/run1', '2026-01-03T00:00:00Z')  # new install
    path = month_path(tmp_path, 'expt', dt.datetime(2023, 1, 1))
    with path.open('ab') as stream:  # a writer that crashed mid-record
        stream.write(gzip.compress(b'{"cycle": "2023010312"}\n')[:-6])
    append(0, 'assimilated', 'flow_install0/run1', '2026-01-04T00:00:00Z')  # re-record
    assert 'dropping' in capsys.readouterr().out
    assert gzip.decompress(path.read_bytes()).decode().count('\n') == 5

    records = read_month(tmp_path, 'expt', dt.datetime(2023, 1, 1))

    def latest(**version):
        return {cycle.hour: (record['install'], record['run'], record['rows'][0][3])
                for cycle, record in select_latest(records, **version).items()}

    assert latest() == {0: (0, 1, 'assimilated'), 6: (1, 1, 'not_staged')}
    assert latest(upto=(0, 2)) == {0: (0, 1, 'assimilated'), 6: (0, 2, 'assimilated')}
    assert latest(as_of='2026-01-01T12:00') == {0: (0, 1, 'partial'), 6: (0, 1, 'partial')}


def test_catchup_record_supersedes_older_runs(tmp_path):
    """A catch-up workflow (its own name, install0/run1) records a cycle after
    production did, so its record wins; --as-of still finds the older one."""
    for outcome, workflow_id, created in [
            ('partial', 'reanl_install1/run3', '2026-01-01T00:00:00Z'),
            ('assimilated', 'reanl_catchup/run1', '2026-02-01T00:00:00Z')]:
        append_record(tmp_path, 'expt', make_record(
            dt.datetime(2023, 1, 3), {(RADIANCE, 'amsua_n15'): CycleResult('assimilate', outcome)},
            source=parse_workflow_id(workflow_id)) | {'created': created})
    records = read_month(tmp_path, 'expt', dt.datetime(2023, 1, 1))
    [record] = select_latest(records).values()
    assert (record['workflow'], record['rows'][0][3]) == ('reanl_catchup', 'assimilated')
    [record] = select_latest(records, as_of='2026-01-15T00:00').values()
    assert (record['workflow'], record['run']) == ('reanl', 3)


def append_cycles(archive, run):
    for hour in range(0, 60, 6):
        append_record(archive, 'expt', make_record(
            dt.datetime(2023, 1, 1) + dt.timedelta(hours=hour),
            {(RADIANCE, 'amsua_n15'): CycleResult('assimilate', 'assimilated')},
            source=parse_workflow_id(f'flow_install0/run{run}')))


def test_concurrent_appends_lose_nothing(tmp_path):
    """Cycles recorded at once (cylc runahead) all land in the month file."""
    with multiprocessing.get_context('fork').Pool(8) as pool:
        pool.starmap(append_cycles, [(tmp_path, run) for run in range(1, 9)])
    records = read_month(tmp_path, 'expt', dt.datetime(2023, 1, 1))
    assert len(records) == 80
    assert len({(record['run'], record['cycle']) for record in records}) == 80


def test_paginate_keeps_variables_whole():
    blocks = [('ps', 3), ('t', 5), ('q', 4), ('uv', 10)]
    rows = [(CONVENTIONAL, f'{name} {i}') for name, count in blocks for i in range(count)]
    parts = paginate(rows, 8)
    assert [row for part in parts for row in part] == rows
    assert [len(part) for part in parts] == [8, 4, 5, 5]  # ps + t, q, uv in halves
    assert paginate(rows[:3], 8) == [rows[:3]]


def test_wrap_breaks_wide_parts_and_cuts_long_words():
    style = FigureStyle()
    width = style.text_width('build_gsinfo-fix psl-develop')
    lines = style.wrap(['May 2015', 'build_gsinfo-fix psl-develop @ 1a2b3c4', 'x' * 80],
                       width, sep='  |  ')
    assert lines[:3] == ['May 2015', 'build_gsinfo-fix psl-develop', '@ 1a2b3c4']
    assert lines[3].endswith('\u2026') and len(lines) == 4
    assert all(style.text_width(line) <= width for line in lines)


def test_figure_text_fits_the_fonts_line_height(tmp_path, monkeypatch):
    """Text is laid out with the line height matplotlib gives the font
    (Inter's 1.45 em, not 1.2 em), so it neither overlaps nor leaves figures
    of at most 9 in. cmsy10 ships with matplotlib and has 2 em lines."""
    import matplotlib.figure
    rows = [(CONVENTIONAL, f'ps {i}') for i in range(120)]
    columns = [dt.datetime(2015, 5, 1, hour) for hour in (0, 6, 12, 18)]

    def chart(size):
        return Chart(FigureStyle(width=3.2, size=size, dpi=100, fonts=['cmsy10', 'DejaVu Sans']),
                     {row: dict.fromkeys(columns, 'used') for row in rows}, columns, 'cycle',
                     {'used': ('Intended and assimilated', '#009E73')},
                     {row: row[1] for row in rows}, title='GSI observation usage',
                     subtitle='reanl', info=['May 2015', 'build_gsinfo-fix @ 888fb45'],
                     xlabel='Day of May 2015', footnote=' '.join(['A long footnote.'] * 12))

    figures, savefig = [], matplotlib.figure.Figure.savefig

    def save_and_measure(figure, *args, **kwargs):
        savefig(figure, *args, **kwargs)
        renderer = figure.canvas.get_renderer()
        figures.append((figure, sorted(
            [text.get_window_extent(renderer) for text in figure.texts]
            + [axes.get_tightbbox(renderer) for axes in figure.axes], key=lambda box: -box.y1)))

    monkeypatch.setattr(matplotlib.figure.Figure, 'savefig', save_and_measure)
    assert len(chart(7).save(tmp_path, 'usage')) > 1
    for figure, boxes in figures:  # top to bottom
        assert figure.get_size_inches()[1] <= 9
        assert boxes[-1].y0 >= 0 and all(box.x1 <= figure.bbox.x1 for box in boxes)
        assert all(upper.y0 >= lower.y1 - 0.5 for upper, lower in zip(boxes, boxes[1:]))
    with pytest.raises(FigureSizeError):
        chart(30).save(tmp_path, 'large')


def test_source_strip_labels_cover_their_own_cycles():
    values, texts = source_strip({0: 'r1', 1: 'r1', 2: 'r2', 4: 'mixed', 5: 'r1'},
                                 range(7), str, lambda text, columns: f'{text}:{columns}')
    assert values == [1, 1, 2, 0, 3, 1, 0]
    assert texts == {0: 'r1:2', 2: 'r2:1', 5: 'r1:1'}


def test_streams_are_joined_oldest_first(tmp_path):
    """A newer stream's spin-up does not replace the older stream unless the
    newer one is given a start (NAME@START)."""
    for name, days, outcome in [('s1988', (1, 2, 3), 'assimilated'),
                                ('s1993', (2, 3, 4), 'partial')]:
        for day in days:
            append_record(tmp_path, name, make_record(
                dt.datetime(2023, 1, day),
                {(RADIANCE, 'amsua_n15'): CycleResult('assimilate', outcome)},
                source=parse_workflow_id(f'{name}_install0/run1')))

    def chart(experiments, *extra):
        out = tmp_path / 'out' / '_'.join(experiments).replace('@', '-')
        obs_usage_dashboard.main(['--archive', str(tmp_path), '--experiment', *experiments,
                                  '--start', '2023-01', '--end', '2023-01', '--cycle-hours',
                                  '24', '--output-dir', str(out), *extra])
        status = {int(row['period'][6:8]): row['status'] for row in
                  csv.DictReader((out / 'obs_usage_202301_24h_status.csv').open())
                  if row['row'] == 'amsua_n15'}
        sources = {row['source'] for row in
                   csv.DictReader((out / 'obs_usage_202301_24h_evidence.csv').open())
                   if row['row'] == 'amsua_n15'}
        return [status[day] for day in range(1, 6)], sources

    assert chart(['s1988', 's1993']) == (
        ['assimilated'] * 3 + ['partial', 'missing'], {'s1993/install0/run1'})
    assert chart(['s1988', 's1993@2023-01-03'])[0] == (
        ['assimilated'] * 2 + ['partial'] * 2 + ['missing'])
    for experiments, extra in [(['s1993@2023-01-03', 's1988@2023-01-01'], []),
                               (['s1988', 's1993'], ['--upto', 'install0/run1'])]:
        with pytest.raises(SystemExit):
            chart(experiments, *extra)


def test_record_task_and_archive_dashboard(gsinfo_repo, tmp_path, monkeypatch):
    """Record two cylc runs, the second rewinding 06Z, and chart them."""
    logs = {('run1', '20150501T00'): gsistat(),
            ('run1', '20150501T06'): gsistat(amsua_nobs=(500, 0)),
            ('run1', '20150501T12'): ['truncated\n'],
            ('run2', '20150501T06'): gsistat()}
    archive, out = tmp_path / 'archive', tmp_path / 'out'
    for variable, value in [('OBS_USAGE_ARCHIVE_DIR', archive), ('EXPERIMENT_NAME', 'expt'),
                            ('OBS_USAGE_GSINFO_URL', gsinfo_repo),
                            ('OBS_USAGE_GSINFO_REF', 'HEAD')]:
        monkeypatch.setenv(variable, str(value))

    def record(run, point, lines):
        work = tmp_path / 'cylc-run' / 'flow_install0' / run / 'work'
        task = work / point / 'store_data_gsi_obsfit'
        task.mkdir(parents=True)
        (task / f"gsistats.{point.replace('T', '')}_control").write_text(''.join(lines))
        monkeypatch.setenv('CYLC_WORKFLOW_ID', f'score-monitoring/flow_install0/{run}')
        monkeypatch.setenv('CYLC_WORKFLOW_WORK_DIR', str(work))
        return obs_usage_record.main([point])

    for (run, point), lines in logs.items():
        record(run, point, lines)
    chart = ['--archive', str(archive), '--experiment', 'expt', '--start', '2015-05',
             '--end', '2015-05', '--output-dir', str(out), '--compare-upto',
             'install0/run1']
    obs_usage_dashboard.main(chart)
    status = {(row['row'], row['period']): row['status'] for row in
              csv.DictReader((out / 'obs_usage_201505_6h_status.csv').open())}
    assert status[('amsua_n15', '2015050100')] == 'assimilated'
    assert status[('amsua_n15', '2015050106')] == 'assimilated'  # run2 replaced run1
    assert status[('amsua_n15', '2015050112')] == 'missing'  # unreadable log
    changes = list(csv.DictReader((out / 'obs_usage_201505_6h_changes.csv').open()))
    assert {row['period'] for row in changes} == {'2015050106'}
    assert ('amsua_n15', 'improved', 'partial', 'assimilated', 'install0/run2') in {
        (row['row'], row['change'], row['baseline_status'], row['current_status'],
         row['current_source']) for row in changes}

    assert obs_usage_dashboard.main(chart + ['--changed-only']) == []
    record('run2', '20150501T12', gsistat())
    assert obs_usage_dashboard.main(chart + ['--changed-only'])
