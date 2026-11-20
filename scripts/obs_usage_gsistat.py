#!/usr/bin/env python

"""Copyright 2026 NOAA
All rights reserved.

Parse one GSI gsistat log (a single 6-hourly cycle) into the observation
usage evidence needed by the observation usage dashboard.

Only the parts of the log that show whether observations were staged (read)
and assimilated are kept:

* RADINFO_READ / OZINFO_READ usage flags that the run actually used
* radiance per-channel diagnostics (nobs_used per outer iteration)
* radiance and ozone ``o-g NN rad|oz sat inst read keep assim ...`` summaries
* conventional fit tables (``o-g NN var asm|rej|mon typ styp ... count``)
"""

import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

USAGE_FLAGS = ('asm', 'rej', 'mon')
CONVENTIONAL_STATS = ('bias', 'rms', 'cpen', 'qcpen')

RAD_CONFIG = re.compile(r'^\s*RADINFO_READ:\s+jpch_rad=\s*(\d+)')
OZ_CONFIG = re.compile(r'^\s*OZINFO_READ:\s+jpch_oz=\s*(\d+)')
RAD_CHANNEL = re.compile(r'^\s*(\d+)\s+(\S+)\s+chan=\s*(\d+)\b.*?\buse=\s*(-?\d+)')
OZ_LEVEL = re.compile(r'^\s*(\d+)\s+(\S+)\s+lev\s*=\s*(\d+)\s+use\s*=\s*(-?\d+)')
SUMMARY = re.compile(
    r'^\s*o-g\s+(\d+)\s+(rad|oz)\s+(\S+)\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\b')
RAD_SUMMARY_HEADER = re.compile(r'satellite\s+instrument\s+#\s*read')
CONVENTIONAL_HEADER = re.compile(r'^\s*current v?fit of ')


@dataclass
class CycleUsage:
    """Observation usage evidence from one gsistat file.

    All sensor/instrument/satellite names are stored lower case so that
    names from the log, build_gsinfo and obs_input can be matched.
    """
    path: str = ''
    rad_config: dict = field(default_factory=dict)  # sensor -> {chan: use}
    oz_config: dict = field(default_factory=dict)  # sensor -> {level: use}
    rad_summary: dict = field(default_factory=dict)  # it -> {(sat, inst): (read, keep, assim)}
    oz_summary: dict = field(default_factory=dict)  # it -> {(sat, inst): (read, keep, assim)}
    rad_channels: dict = field(default_factory=dict)  # it -> {(sensor, chan): nobs_used}
    conventional: dict = field(default_factory=dict)  # it -> {(var, use, typ, styp): count}
    rad_summary_headers: int = 0
    conventional_headers: int = 0
    warnings: list = field(default_factory=list)

    @property
    def valid(self):
        """True when the log contains the sections the dashboard relies on.

        A truncated or failed GSI log is treated like a missing cycle rather
        than as evidence that observations were not used.
        """
        return (self.conventional_headers > 0 and self.rad_summary_headers > 0
                and bool(self.rad_config))


def _conventional_count(fields):
    """Return the total count of a conventional fit row, or None."""
    if len(fields) < 7 or fields[3] not in USAGE_FLAGS:
        return None
    if not (fields[4].isdigit() and fields[5].isdigit()):
        return None
    if fields[6] == 'count':
        # multi-level tables: the last column spans the full column (0-2000 hPa)
        return int(float(fields[-1]))
    if fields[6] in CONVENTIONAL_STATS:
        return None
    try:
        return int(fields[6])  # single-level (surface pressure) tables
    except ValueError:
        return None


def parse_lines(lines, path=''):
    usage = CycleUsage(path=str(path))
    rad_index = {}
    rad_remaining = oz_remaining = 0
    pending_channels = []

    for line in lines:
        if rad_remaining:
            rad_remaining -= 1
            match = RAD_CHANNEL.match(line)
            if match:
                index, sensor, chan, use = match.groups()
                sensor = sensor.lower()
                usage.rad_config.setdefault(sensor, {})[int(chan)] = int(use)
                rad_index[int(index)] = (sensor, int(chan))
            else:
                usage.warnings.append(f'unexpected RADINFO line: {line.strip()}')
            continue
        if oz_remaining:
            oz_remaining -= 1
            match = OZ_LEVEL.match(line)
            if match:
                _, sensor, level, use = match.groups()
                usage.oz_config.setdefault(sensor.lower(), {})[int(level)] = int(use)
            else:
                usage.warnings.append(f'unexpected OZINFO line: {line.strip()}')
            continue

        stripped = line.lstrip()
        if stripped.startswith('o-g'):
            fields = stripped.split()
            match = SUMMARY.match(line)
            if match:
                it, kind, sat, inst, read, keep, assim = match.groups()
                it = int(it)
                table = usage.rad_summary if kind == 'rad' else usage.oz_summary
                table.setdefault(it, {})[(sat.lower(), inst.lower())] = (
                    int(read), int(keep), int(assim))
                if kind == 'rad' and pending_channels:
                    channels = usage.rad_channels.setdefault(it, {})
                    for key, nobs in pending_channels:
                        channels[key] = nobs
                    pending_channels = []
                continue
            count = _conventional_count(fields)
            if count is not None and fields[1].isdigit():
                key = (fields[2].lower(), fields[3], int(fields[4]), int(fields[5]))
                table = usage.conventional.setdefault(int(fields[1]), {})
                table[key] = table.get(key, 0) + count
            continue

        if CONVENTIONAL_HEADER.match(line):
            usage.conventional_headers += 1
            continue
        match = RAD_CONFIG.match(line)
        if match:
            rad_remaining = int(match.group(1))
            continue
        match = OZ_CONFIG.match(line)
        if match:
            oz_remaining = int(match.group(1))
            continue
        if RAD_SUMMARY_HEADER.search(line):
            usage.rad_summary_headers += 1
            continue

        # radiance per-channel diagnostics (statsrad): 11 columns
        #   index chan sensor nobs_used nobs_tossed var bias_pre bias_post ...
        # rows are accepted only when index/chan/sensor match RADINFO, which
        # excludes the similarly formatted ozone per-level diagnostics.
        if rad_index and stripped[:1].isdigit():
            fields = stripped.split()
            if len(fields) == 11 and fields[0].isdigit() and fields[1].isdigit():
                expected = rad_index.get(int(fields[0]))
                if expected == (fields[2].lower(), int(fields[1])):
                    try:
                        pending_channels.append((expected, int(fields[3])))
                    except ValueError:
                        pass

    if rad_remaining or oz_remaining:
        usage.warnings.append('usage flag table truncated')
    if pending_channels:
        usage.warnings.append('radiance channel diagnostics without summary')
    return usage


def parse_gsistat(path):
    with open(path, errors='replace') as stream:
        return parse_lines(stream, path=Path(path))


def conventional_by_type(usage):
    """Aggregate conventional counts.

    Returns ``{(var, typ): {iteration: {'asm'|'rej'|'mon': count,
    'subtypes': {styp: {'asm'|'rej'|'mon': count}}}}}``.
    """
    result = defaultdict(dict)
    for it, table in usage.conventional.items():
        for (var, use, typ, styp), count in table.items():
            entry = result[(var, typ)].setdefault(
                it, {'asm': 0, 'rej': 0, 'mon': 0, 'subtypes': {}})
            entry[use] += count
            sub = entry['subtypes'].setdefault(styp, {'asm': 0, 'rej': 0, 'mon': 0})
            sub[use] += count
    return dict(result)
