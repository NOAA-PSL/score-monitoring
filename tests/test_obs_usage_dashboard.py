"""Tests for the observation usage dashboard (scripts/obs_usage_*.py)."""

import csv
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))

import obs_usage_dashboard  # noqa: E402
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
    obs_usage_dashboard.main(['--gsistat-dir', str(logs), '--start', '2015-05',
                              '--end', '2015-05', '--gsinfo', str(gsinfo_repo),
                              '--gsinfo-ref', 'HEAD', '--output-dir', str(out)])
    assert (out / 'obs_usage_201505_6h_dashboard.png').stat().st_size > 0
    rows = {(row['row'], row['period']): row['status'] for row in
            csv.DictReader((out / 'obs_usage_201505_201505_6h_status.csv').open())}
    assert rows[('amsua_n15', '2015050100')] == 'assimilated'
    assert rows[('amsua_n15', '2015050106')] == 'partial'
    assert rows[('amsua_n15', '2015050112')] == 'missing'
    evidence = (out / 'obs_usage_201505_201505_6h_evidence.csv').read_text()
    assert 'unreadable' in evidence and '1/2 intended channels' in evidence

    obs_usage_dashboard.main(['--gsistat-dir', str(logs), '--start', '2015-05',
                              '--end', '2015-05', '--gsinfo', str(gsinfo_repo),
                              '--gsinfo-ref', 'HEAD', '--resolution', 'month',
                              '--output-dir', str(out)])
    rows = {row['row']: row['status'] for row in csv.DictReader(
        (out / 'obs_usage_201505_201505_monthly_status.csv').open())}
    assert rows['amsua_n15'] == 'partial' and rows['hirs4_n19'] == 'missing'
