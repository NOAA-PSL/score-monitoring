#!/usr/bin/env python

"""Copyright 2026 NOAA
All rights reserved.

Record one cycle's observation usage (build_gsinfo intent vs. what GSI did)
in the append-only obs usage archive read by obs_usage_dashboard.py. It does
not use score-db.

Run as a cylc task after the task that downloads the gsistat log::

    obs_usage_record.py $CYLC_TASK_CYCLE_POINT {{ ENV_PATH }}

The gsistat log is taken, in order, from ``--gsistat``, from a sibling task
of the same cycle that already downloaded it (``$OBS_USAGE_GSISTAT_TASKS``,
default store_data_gsi_obsfit,store_data_gsi_conv_obsfit), or downloaded
from the S3 location in the environment file (and deleted afterwards).

Environment (from the environment file or the shell):
    OBS_USAGE_ARCHIVE_DIR   archive root (required; outside cylc-run so
                            ``cylc clean`` does not remove it; ~ and $VARS
                            are expanded)
    EXPERIMENT_NAME         archive sub-directory for this experiment
                            (keep it the same for every install and run of
                            a stream so the dashboard sees all versions)
    OBS_USAGE_GSINFO_URL    build_gsinfo repository (URL or local clone)
    OBS_USAGE_GSINFO_REF    build_gsinfo branch, tag or commit
    OBS_USAGE_ITERATIONS    all|first|last outer iterations that must
                            assimilate (default all)
    OBS_USAGE_CACHE         build_gsinfo clone cache directory
    OBS_USAGE_GSINFO_REFRESH_MINUTES  minimum time between git fetches (60)

The install and run (``..._installN/runM``) come from $CYLC_WORKFLOW_ID and
the score-monitoring version from the run's log/version/vcs.json; both can be
given on the command line to record logs outside cylc (e.g. a backfill).
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from obs_usage_archive import append_record, make_record, parse_workflow_id  # noqa: E402
from obs_usage_gsinfo import (DEFAULT_GSINFO_REF, DEFAULT_GSINFO_URL,  # noqa: E402
                              GsinfoInventory, GsinfoSource)
from obs_usage_gsistat import parse_gsistat  # noqa: E402
from obs_usage_status import evaluate_cycle  # noqa: E402

DEFAULT_GSISTAT_TASKS = 'store_data_gsi_obsfit,store_data_gsi_conv_obsfit'


def parse_cycle(text):
    """Cylc cycle point (20230103T06, 20230103T0600Z) or YYYYMMDDHH."""
    match = re.fullmatch(r'(\d{8})T?(\d{2})(\d{2})?Z?', text.strip())
    if not match:
        raise ValueError(f'unrecognised cycle point {text!r}')
    return dt.datetime.strptime(match.group(1) + match.group(2), '%Y%m%d%H')


def load_env(env_file):
    if not env_file:
        return
    path = Path(env_file)
    if not path.is_absolute() and not path.exists():
        # same convention as the db_*.py scripts: relative to the workflow dir
        path = Path(__file__).resolve().parent.parent / env_file
    if not path.is_file():
        raise FileNotFoundError(f'environment file {env_file} not found')
    from dotenv import load_dotenv
    load_dotenv(path)


def sibling_gsistat(cycle_point, cycle):
    """gsistat log already downloaded by another task of this cycle."""
    work = os.environ.get('CYLC_WORKFLOW_WORK_DIR')
    if not work:
        return None
    tasks = os.environ.get('OBS_USAGE_GSISTAT_TASKS', DEFAULT_GSISTAT_TASKS)
    for task in filter(None, (task.strip() for task in tasks.split(','))):
        found = sorted(Path(work, cycle_point, task).glob(
            f'gsistats.{cycle:%Y%m%d%H}_*'), key=lambda p: not p.name.endswith('_control'))
        if found:
            return found[0]
    return None


def download_gsistat(cycle, directory):
    """Download the log from S3 as db_gsi_obsfit.py does."""
    import boto3
    from botocore import UNSIGNED
    from botocore.client import Config

    name_format = os.getenv('GSI_FIT_FILE_NAME_FORMAT')
    if not name_format:
        raise ValueError('GSI_FIT_FILE_NAME_FORMAT is not set')
    key_id = os.getenv('AWS_ACCESS_KEY_ID')
    s3 = boto3.resource('s3', aws_access_key_id=key_id or None,
                        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY') or None,
                        config=Config(signature_version='s3v4' if key_id else UNSIGNED))
    prefix = os.getenv('STORAGE_LOCATION_KEY', '')
    if os.getenv('GSI_FIT_FILE_KEY'):
        prefix += '/' + os.getenv('GSI_FIT_FILE_KEY')
    key = cycle.strftime(f'{prefix}/{name_format}')
    path = Path(directory) / f'gsistats.{cycle:%Y%m%d%H}_control'
    s3.Bucket(os.getenv('STORAGE_LOCATION_BUCKET')).download_file(key, str(path))
    return path, f"s3://{os.getenv('STORAGE_LOCATION_BUCKET')}/{key}"


def score_monitoring_version(run_dir):
    """Commit of the workflow source recorded by cylc install."""
    if not run_dir:
        return {}
    path = Path(run_dir) / 'log' / 'version' / 'vcs.json'
    try:
        info = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return {'commit': info.get('commit', ''),
            'dirty': str(info.get('repository version', '')).endswith('-dirty')}


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__.split('\n\n')[1],
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('cycle', help='cycle point, e.g. 20230103T06 or 2023010306')
    parser.add_argument('env_file', nargs='?', default=None,
                        help='environment file, relative to the workflow directory')
    parser.add_argument('--gsistat', help='gsistat log to record (skips the lookup)')
    parser.add_argument('--archive', help='archive root (default: $OBS_USAGE_ARCHIVE_DIR)')
    parser.add_argument('--experiment', help='default: $EXPERIMENT_NAME')
    parser.add_argument('--workflow-id', help='default: $CYLC_WORKFLOW_ID, e.g. '
                        'score-monitoring/name_install1/run3')
    parser.add_argument('--run-dir', help='workflow run directory holding '
                        'log/version/vcs.json (default: $CYLC_WORKFLOW_RUN_DIR)')
    parser.add_argument('--dry-run', action='store_true',
                        help='print the record instead of appending it')
    args = parser.parse_args(argv)

    load_env(args.env_file)
    cycle = parse_cycle(args.cycle)
    archive = args.archive or os.getenv('OBS_USAGE_ARCHIVE_DIR')
    archive = archive and os.path.expandvars(os.path.expanduser(archive))
    experiment = (args.experiment or os.getenv('EXPERIMENT_NAME') or '').strip()
    if not args.dry_run and not archive:
        parser.error('set OBS_USAGE_ARCHIVE_DIR or --archive')
    if not experiment:
        parser.error('set EXPERIMENT_NAME or --experiment')
    workflow_id = args.workflow_id or os.getenv('CYLC_WORKFLOW_ID', '')
    if not workflow_id:
        parser.error('set CYLC_WORKFLOW_ID or --workflow-id')

    downloaded = False
    if args.gsistat:
        path, origin = Path(args.gsistat), str(args.gsistat)
    else:
        path = sibling_gsistat(args.cycle, cycle)
        origin = str(path) if path else None
        if path is None:
            directory = os.getenv('CYLC_TASK_WORK_DIR') or os.getcwd()
            path, origin = download_gsistat(cycle, directory)
            downloaded = True
    if not path.is_file():
        raise FileNotFoundError(f'gsistat log {path} not found')

    try:
        usage = parse_gsistat(path)
        source = GsinfoSource(os.getenv('OBS_USAGE_GSINFO_URL') or DEFAULT_GSINFO_URL,
                              os.getenv('OBS_USAGE_GSINFO_REF', DEFAULT_GSINFO_REF))
        inventory = GsinfoInventory(source)
        iterations = os.getenv('OBS_USAGE_ITERATIONS') or 'all'
        results, convinfo_path = None, None
        if usage.valid:
            convinfo, convinfo_path = inventory.convinfo(cycle)
            results = evaluate_cycle(usage, inventory.satinfo(cycle)[0],
                                     inventory.ozinfo(cycle)[0], convinfo,
                                     inventory.obs_input_names(), iterations)
        record = make_record(
            cycle, results,
            source=parse_workflow_id(workflow_id)
            | {'submit': int(os.getenv('CYLC_TASK_SUBMIT_NUMBER') or 1)},
            valid=usage.valid,
            warnings=usage.warnings or ([] if usage.valid else
                                        ['required gsistat sections not found']),
            iterations=iterations,
            gsistat={'origin': origin, 'sha256': sha256(path),
                     'bytes': path.stat().st_size},
            build_gsinfo=source.describe() | {'convinfo': convinfo_path},
            score_monitoring=score_monitoring_version(
                args.run_dir or os.getenv('CYLC_WORKFLOW_RUN_DIR')))
    finally:
        if downloaded:
            path.unlink(missing_ok=True)

    if args.dry_run:
        print(json.dumps(record, indent=1))
        return record
    written = append_record(archive, experiment, record)
    status = 'recorded' if record['valid'] else 'recorded UNREADABLE log'
    print(f"{status} {record['cycle']} install{record['install']}/run{record['run']} "
          f"({len(record['rows'])} rows) in {written}")
    return record


if __name__ == '__main__':
    main()
