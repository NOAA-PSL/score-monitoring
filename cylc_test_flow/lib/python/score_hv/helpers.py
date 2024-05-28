"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to help substitute yaml data base on env vars

"""

import os
import re
import yaml

# local imports

def envvar_constructor(node):
    ''' method to help substitute parent directory for a yaml env var '''
    return os.path.expandvars(node.value)


def load_yaml_file(yaml_file):
    """
    this yaml load function will allow a user to specify an environment
    variable to substitute in a yaml file if the tag '!ENVVAR' exists
    this will help developers create absolute system paths that are related
    to the install path of the score-hv package.
    """

    try:
        loader = yaml.SafeLoader
        loader.add_implicit_resolver(
            '!ENVVAR',
            re.compile(r'.*\$\{([^}^{]+)\}.*'),
            None
        )

        loader.add_constructor('!ENVVAR', envvar_constructor)
        yaml_dict = None
        with open(yaml_file, 'r', encoding='utf8') as ymlfile:
            yaml_dict = yaml.load(ymlfile, Loader=loader)

    except Exception as err:
        msg = f'Errors encountered loading  yaml file: {yaml_file}, error: {err}'
        raise TypeError(msg) from err

    return yaml_dict
