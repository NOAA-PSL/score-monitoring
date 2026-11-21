#!/usr/bin/env python

"""Copyright 2026 NOAA
All rights reserved.

Compare intended observation usage (build_gsinfo control logic) with what a
GSI cycle actually did (gsistat log). Each cycle gets one dashboard status;
cycles can also be rolled up conservatively (e.g. into months).

The dashboard answers "was this instrument / data type staged and assimilated
when we intended to assimilate it, and only monitored (or not used) when we
intended that?". It is not a data-volume or QC-quality check; count time
series are covered by score-plotting.

Rows are one satellite sensor (``sensor_sat`` as in satinfo/ozinfo) or one
conventional ``variable + prepbufr type``. A single intended channel (or
intended conventional subtype that was staged) failing in a single cycle
fails the row for that cycle, and any failing cycle fails a rolled-up period.
"""

from collections import OrderedDict
from dataclasses import dataclass, field

from obs_usage_gsistat import conventional_by_type

RADIANCE, OZONE, CONVENTIONAL = 'Satellite radiance', 'Ozone', 'Conventional'
GROUPS = (RADIANCE, OZONE, CONVENTIONAL)

# conventional variables printed in gsistat fit tables -> convinfo otype
CONVENTIONAL_VARIABLES = OrderedDict(
    [('ps', 'ps'), ('t', 't'), ('q', 'q'), ('uv', 'uv'), ('gps', 'gps')])

ASSIMILATE, MONITOR, OFF = 'assimilate', 'monitor', 'off'

# cycle outcomes
OK_ASSIMILATED = 'assimilated'
PARTIAL = 'partial'
NOT_ASSIMILATED = 'not_assimilated'
NOT_STAGED = 'not_staged'
UNEXPECTED = 'unexpected'
MONITORED = 'monitored'
ABSENT = 'absent'
MISSING = 'missing'
FAILURES = (UNEXPECTED, NOT_ASSIMILATED, NOT_STAGED, PARTIAL)

# dashboard statuses, most severe first: (label, color). Black, white, gray
# and Okabe & Ito (2008) colors; every pair keeps a CIELAB difference >= 16
# when simulated for protan, deutan and tritan vision (Machado et al. 2009).
STATUSES = OrderedDict([
    ('unexpected', ('Not intended, but assimilated', '#D55E00')),
    ('not_used', ('Intended, but not assimilated (no obs staged, or none used)',
                  '#000000')),
    ('partial', ('Intended, partly assimilated (an intended channel, level, '
                 'subtype or outer iteration was not)', '#E69F00')),
    ('missing', ('gsistat log missing or unreadable', '#BBBBBB')),
    ('assimilated', ('Intended and assimilated', '#009E73')),
    ('monitored', ('Not intended for assimilation; staged and monitored',
                   '#56B4E9')),
    ('unused', ('Not intended and not used', '#FFFFFF')),
])
OUTCOME_STATUS = {
    UNEXPECTED: 'unexpected', NOT_ASSIMILATED: 'not_used', NOT_STAGED: 'not_used',
    PARTIAL: 'partial', MISSING: 'missing', OK_ASSIMILATED: 'assimilated',
    MONITORED: 'monitored', ABSENT: 'unused'}


@dataclass
class CycleResult:
    intent: str
    outcome: str
    reasons: list = field(default_factory=list)
    mismatch: list = field(default_factory=list)


def intent_from_flags(flags):
    """satinfo/ozinfo iuse: >=1 use; 0, -1 monitor; <=-2 (or absent) not used."""
    values = list((flags or {}).values())
    if any(value >= 1 for value in values):
        return ASSIMILATE
    if any(value in (0, -1) for value in values):
        return MONITOR
    return OFF


def intent_from_convinfo(subtypes):
    """convinfo iuse: 1 use; 0 (do not use) and -1 monitor; -3 skip reading."""
    values = list((subtypes or {}).values())
    if any(value >= 1 for value in values):
        return ASSIMILATE
    if any(value in (0, -1) for value in values):
        return MONITOR
    return OFF


def convinfo_iuse(subtypes, styp):
    """GSI convinfo matching: an exact subtype entry, else the subtype 0 entry."""
    if styp in subtypes:
        return subtypes[styp]
    return subtypes.get(0)


def required_iterations(reported, policy):
    reported = sorted(reported)
    if not reported:
        return []
    if policy == 'first':
        return reported[:1]
    if policy == 'last':
        return reported[-1:]
    return reported


def _compact(values, limit=12):
    values = sorted(values)
    text = ','.join(str(value) for value in values[:limit])
    return text + (f',... ({len(values)} total)' if len(values) > limit else '')


def flag_mismatch(intended, logged):
    """Describe channels/levels whose run usage flag differs from build_gsinfo."""
    intended, logged = intended or {}, logged or {}
    if not intended and not logged:
        return []
    if not logged:
        return ['not in the run info file']
    if not intended:
        return ['not in build_gsinfo']
    differ = [key for key in set(intended) | set(logged)
              if intended.get(key) != logged.get(key)]
    if not differ:
        return []
    pairs = sorted((key, intended.get(key), logged.get(key)) for key in differ)
    detail = ';'.join(f'{key}:{a}->{b}' for key, a, b in pairs[:8])
    more = f';... ({len(pairs)} total)' if len(pairs) > 8 else ''
    return [f'usage flags differ (build_gsinfo->run) {detail}{more}']


def _not_intended(intent, staged, assimilated, mismatch):
    if assimilated:
        return CycleResult(intent, UNEXPECTED, ['obs assimilated'], mismatch)
    return CycleResult(intent, MONITORED if staged else ABSENT, [], mismatch)


def summary_lookup(summary, sensor, aliases):
    """Find a sensor's ``o-g`` summary rows: ``{iteration: (read, keep, assim)}``."""
    rows = {}
    for it, table in summary.items():
        for (sat, inst), counts in table.items():
            name = aliases.get((sat, inst), f'{inst}_{sat}')
            if name == sensor:
                rows[it] = counts
    return rows


def evaluate_radiance(sensor, intended, usage, policy, aliases):
    logged = usage.rad_config.get(sensor)
    intent = intent_from_flags(intended)
    mismatch = flag_mismatch(intended, logged)
    rows = summary_lookup(usage.rad_summary, sensor, aliases)
    staged = any(counts[0] > 0 for counts in rows.values())
    iterations = required_iterations(
        [it for it, table in usage.rad_summary.items() if table], policy)
    if intent != ASSIMILATE:
        assimilated = any(counts[2] > 0 for counts in rows.values())
        return _not_intended(intent, staged, assimilated, mismatch)
    if not staged:
        return CycleResult(intent, NOT_STAGED, ['no obs read'], mismatch)

    channels = sorted(chan for chan, use in intended.items() if use >= 1)
    disabled = [chan for chan in channels if (logged or {}).get(chan, -9) < 1]
    failed = {}
    for it in iterations:
        nobs = usage.rad_channels.get(it, {})
        for chan in channels:
            if chan in disabled or nobs.get((sensor, chan), 0) <= 0:
                failed.setdefault(chan, []).append(it)
    if not failed:
        return CycleResult(intent, OK_ASSIMILATED, [], mismatch)
    used = [chan for chan in channels if chan not in disabled and any(
        usage.rad_channels.get(it, {}).get((sensor, chan), 0) > 0
        for it in iterations)]
    reasons = []
    if not used:
        reasons.append('no intended channel assimilated')
    else:
        reasons.append(f'{len(failed)}/{len(channels)} intended channels not '
                       f'assimilated: {_compact(failed)}')
    if disabled:
        reasons.append(f'run usage flag < 1 for intended channels: '
                       f'{_compact(disabled)}')
    return CycleResult(intent, PARTIAL if used else NOT_ASSIMILATED, reasons,
                       mismatch)


def evaluate_ozone(sensor, intended, usage, policy, aliases):
    logged = usage.oz_config.get(sensor)
    intent = intent_from_flags(intended)
    mismatch = flag_mismatch(intended, logged)
    rows = summary_lookup(usage.oz_summary, sensor, aliases)
    staged = any(counts[0] > 0 for counts in rows.values())
    if intent != ASSIMILATE:
        assimilated = any(counts[2] > 0 for counts in rows.values())
        return _not_intended(intent, staged, assimilated, mismatch)
    if not staged:
        return CycleResult(intent, NOT_STAGED, ['no obs read'], mismatch)
    # ozone summaries are not printed for every outer iteration
    iterations = required_iterations(
        [it for it, table in usage.oz_summary.items() if table], policy)
    failed = [it for it in iterations if rows.get(it, (0, 0, 0))[2] <= 0]
    if not failed:
        return CycleResult(intent, OK_ASSIMILATED, [], mismatch)
    reasons = [f'0 assimilated in outer iteration(s) {_compact(failed)}']
    disabled = [lev for lev, use in intended.items()
                if use >= 1 and (logged or {}).get(lev, -9) < 1]
    if disabled:
        reasons.append(f'run usage flag < 1 for intended levels: {_compact(disabled)}')
    outcome = PARTIAL if len(failed) < len(iterations) else NOT_ASSIMILATED
    return CycleResult(intent, outcome, reasons, mismatch)


def evaluate_conventional(key, subtypes, counts, var_iterations, policy):
    """Evaluate one conventional ``(variable, type)`` row for one cycle.

    ``subtypes`` is the convinfo ``{subtype: iuse}`` for the type and
    ``counts`` is the type's entry from ``conventional_by_type``.
    """
    intent = intent_from_convinfo(subtypes)
    counts = counts or {}
    staged = any(entry['asm'] + entry['rej'] + entry['mon'] > 0
                 for entry in counts.values())
    if intent != ASSIMILATE:
        assimilated = any(entry['asm'] > 0 for entry in counts.values())
        return _not_intended(intent, staged, assimilated, [])
    if not staged:
        return CycleResult(intent, NOT_STAGED, ['no obs read'], [])
    reasons = []
    iterations = required_iterations(var_iterations, policy)
    failed = [it for it in iterations if counts.get(it, {}).get('asm', 0) <= 0]
    if failed:
        reasons.append(f'0 assimilated in outer iteration(s) {_compact(failed)}')
    # an intended subtype that was staged but not assimilated fails the type;
    # convinfo lists types for platforms that may not exist yet (it is undated
    # before 2022), so intended subtypes that were never staged do not fail
    gaps, unexpected = set(), set()
    for it in iterations:
        for styp, entry in counts.get(it, {}).get('subtypes', {}).items():
            iuse = convinfo_iuse(subtypes, styp)
            total = entry['asm'] + entry['rej'] + entry['mon']
            if iuse is not None and iuse >= 1 and total > 0 and entry['asm'] <= 0:
                gaps.add(styp)
            if (iuse is None or iuse < 1) and entry['asm'] > 0:
                unexpected.add(styp)
    if unexpected:
        return CycleResult(intent, UNEXPECTED, [
            f'subtype(s) assimilated without convinfo iuse=1: {_compact(unexpected)}'])
    if gaps:
        reasons.append(f'intended subtype(s) staged but not assimilated: {_compact(gaps)}')
    if reasons:
        used = any(counts.get(it, {}).get('asm', 0) > 0 for it in iterations)
        return CycleResult(intent, PARTIAL if used else NOT_ASSIMILATED, reasons, [])
    return CycleResult(intent, OK_ASSIMILATED, [], [])


def evaluate_cycle(usage, satinfo, ozinfo, convinfo, aliases, policy='all'):
    """Evaluate every row for one valid cycle.

    Returns ``{(group, name): CycleResult}`` including rows that only appear in
    the log (so unintended assimilation is never hidden).
    """
    results = {}
    rad_names = set(satinfo) | set(usage.rad_config) | {
        aliases.get(key, f'{key[1]}_{key[0]}')
        for table in usage.rad_summary.values() for key in table}
    for sensor in rad_names:
        results[(RADIANCE, sensor)] = evaluate_radiance(
            sensor, satinfo.get(sensor), usage, policy, aliases)

    oz_names = set(ozinfo) | set(usage.oz_config) | {
        aliases.get(key, f'{key[1]}_{key[0]}')
        for table in usage.oz_summary.values() for key in table}
    for sensor in oz_names:
        results[(OZONE, sensor)] = evaluate_ozone(
            sensor, ozinfo.get(sensor), usage, policy, aliases)

    by_type = conventional_by_type(usage)
    var_iterations = {}
    for it, table in usage.conventional.items():
        for var, *_ in table:
            var_iterations.setdefault(var, set()).add(it)
    otypes = {otype: var for var, otype in CONVENTIONAL_VARIABLES.items()}
    keys = {(otypes[otype], typ) for otype, typ in convinfo if otype in otypes}
    keys |= {key for key in by_type if key[0] in CONVENTIONAL_VARIABLES}
    for var, typ in keys:
        results[(CONVENTIONAL, f'{var} {typ}')] = evaluate_conventional(
            (var, typ), convinfo.get((CONVENTIONAL_VARIABLES[var], typ), {}),
            by_type.get((var, typ)), var_iterations.get(var, ()), policy)
    return results


@dataclass
class CellStatus:
    status: str
    expected: int = 0
    present: int = 0
    counts: dict = field(default_factory=dict)
    mismatch: bool = False
    intents: tuple = ()


def aggregate_cycles(results):
    """Roll cycle results (``None`` = missing cycle) into one dashboard status.

    For a single cycle this is that cycle's status. For a longer period it is
    conservative: the most severe cycle status in ``STATUSES`` order wins, so
    any failing or missing cycle prevents green.
    """
    counts = {}
    for result in results:
        outcome = MISSING if result is None else result.outcome
        counts[outcome] = counts.get(outcome, 0) + 1
    present = [result for result in results if result is not None]
    found = {OUTCOME_STATUS[outcome] for outcome in counts}
    status = next((key for key in STATUSES if key in found), 'unused')
    return CellStatus(status, len(results), len(present), counts,
                      any(result.mismatch for result in present),
                      tuple(sorted({result.intent for result in present})))
