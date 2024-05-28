"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to facilitate file/object retrieval

"""
import argparse

from score_hv.yaml_utils import YamlLoader
from score_hv import hv_registry as hvr
from score_hv import file_utils

def harvest(harvest_config):
    """
    Gets harvester config as either a yaml file or dict and returns
    a list of tuples

    Parameters
    ----------
    harvest_config: str
        The dict or yaml file containing the harvester configuration

    Returns
    -------
    requested_data: list
        A list of tuples where each tuple's contents is defined by the
        harvester
    """

    # Convert incoming config (either dictionary or file) to dictionary
    if isinstance(harvest_config, dict):
        harvest_dict = harvest_config
    else:
        # Create dictionary from the input file
        harvest_dict = YamlLoader(harvest_config).load()[0]

    # Determine which harvester to use: note 'harvester_name' must exist
    # in the harvester yaml/dict config and should point to one of the
    # registered harvesters (each harvester should be registered in
    # src/score_hv/hv_registry.py, see hv_registry.py for example registered
    # harvesters).
    try:
        harvester_name = harvest_dict.get('harvester_name')
        print(f'harvester_name: {harvester_name}')
        print(f'harvester_registry: {hvr.harvester_registry}, ' \
                f'type(harvester_registry): {type(hvr.harvester_registry)}')
        harvester = hvr.harvester_registry.get(harvester_name)
    except Exception as err:
        msg = f'could not find harvester from config: {harvest_dict}'
        raise KeyError(msg) from err

    print(f'harvester_name: {harvester_name}')
    config = harvester.config_handler(harvest_dict)
    print(f'type(config): {type(config)}')
    return harvester.data_parser(config).get_data()



# --------------------------------------------------------------------------------------------------


def main():
    """
    If the harvester app is kicked off from command line, this is the entry
    point.

    Parameters
    ----------
    args: a list of arguments - in this case only one argument is allowed
    and must be a yaml file containing the configuration
    """

    # Arguments
    # ---------
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', type=str, help='Configuration ' \
                        'YAML file for driving the harvest.')

    # Get the configuation file
    args = parser.parse_args()
    config_file = args.config_file

    file_utils.is_valid_readable_file(config_file)

    # Run the harvest
    harvest(config_file)


# --------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
