"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to facilitate file/object retrieval

"""
import os
from pathlib import Path
import re


def is_valid_readable_file(filepath):
    """
    Method to ensure that the filename/path is valid, exists, contains data,
    and the user has sufficient permissions to read it.
    """
    # look for invalid characters in filename/path
    try:
        m_search = re.search(r'[^A-Za-z0-9\._\-\/]', filepath)
        if m_search is not None and m_search.group(0) is not None:
            print(
                'Only a-z A-Z 0-9 and - . / _ characters allowed in filepath')
            raise ValueError(
                f'Invalid characters found in file path: {filepath}')
    except Exception as err:
        raise ValueError(f'Invalid file path: {err}') from err

    try:
        path = Path(filepath)
        if not path.is_file():
            raise ValueError(f'Path: {filepath} does not exist')
    except Exception as err:

        raise ValueError(f'Invalid file path: {err}') from err

    # check permissions on file
    status = os.stat(filepath, follow_symlinks=True)
    print(f'status.st_size: {status.st_size}')
    if status.st_size == 0:
        print(f'if block caught 0 byte file {status}')
        raise ValueError(f'Invalid file. File {filepath} is empty.')

    permissions = oct(status.st_mode)[-3:]
    read_access = os.access(filepath, os.R_OK)
    if read_access is False:
        raise ValueError(
            f'Insufficient permissions on file "{filepath}" - {permissions}.'
        )
