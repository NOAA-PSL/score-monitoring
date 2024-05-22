"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to facilitate handling of score db requests

"""
import argparse

from yaml_utils import YamlLoader
import db_request_registry as dbrr
import file_utils

def handle_request(request_info):
    """
    Gets db request as either a yaml file or dict and returns
    a tuple with the results of the request

    Parameters
    ----------
    request_info: dict or str
        The dict or yaml file containing the db GET, POST, PUT, PATCH, DELETE
        request.

    Returns
    -------
    db_response: tuple (need to define a standard response dataclass)
        A tuple containing result of request and return payload if the
        request was a data query
    """

    # Convert incoming query (either dictionary or file) to dictionary
    if isinstance(request_info, dict):
        db_request_dict = request_info
    else:
        # Create dictionary from the input file
        db_request_dict = YamlLoader(request_info).load()[0]
    
    print(f'db_request_dict: {db_request_dict}')

    # Determine which request to use: note 'request_name' must exist
    # in the db_request yaml/dict and should point to one of the
    # registered requests (each request should be registered in
    # src/db_request_registry.py, see db_request_registry.py for example registered
    # requests).
    try:
        request_name = db_request_dict.get('db_request_name')
        print(f'db_request_name: {request_name}')
        print(f'db_request_registry: {dbrr.request_registry}, ' \
                f'type(request_registry): {type(dbrr.request_registry)}')
        db_request_handler = dbrr.request_registry.get(request_name)
    except Exception as err:
        msg = f'could not find request from request_dict: {db_request_dict}'
        raise KeyError(msg) from err

    print(f'db_request_handler.description: {db_request_handler.description}')
    db_request = db_request_handler.request(db_request_dict)
    print(f'type(request_meta): {type(db_request)}')
    response = db_request.submit()
    # check type of response.  Must be a specific dataclass defined in registry
    return response



# --------------------------------------------------------------------------------------------------


def main():
    """
    If the db request app is kicked off from command line, this is the entry
    point.

    Parameters
    ----------
    args: a list of arguments - in this case only one argument is allowed
    and must be a yaml file containing the db request
    """

    # Arguments
    # ---------
    parser = argparse.ArgumentParser()
    parser.add_argument('request_yaml', type=str, help='Request ' \
                        'YAML file for describing the request.')

    # Get the configuation file
    args = parser.parse_args()
    request_yaml = args.request_yaml

    file_utils.is_valid_readable_file(request_yaml)

    # Submit the score db request
    return handle_request(request_yaml)


# --------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    main()
