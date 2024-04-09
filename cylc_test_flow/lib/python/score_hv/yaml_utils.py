"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to facilitate file/object retrieval

"""
from dataclasses import dataclass, field
import pathlib
import yaml


def validate_yaml(value):
    ''' ensure yaml file exists and has the correct extension '''
    valid_exts = ['.yml', '.yaml']
    try:
        ext = pathlib.Path(value).suffix
    except Exception as err:
        raise ValueError(f'Invalid path: {value}, error: {err}') from err

    if ext not in valid_exts:
        raise TypeError(f'Not a recognized yaml extension: {ext}') from err


def is_expected_return_type(value, instance_type):
    ''' ensure the returned value is the expected type '''
    actual_type = type(value)
    if not isinstance(value, instance_type):
        msg = f'Wrong instance type, expected: {instance_type}, found: ' \
              f'{actual_type}'
        raise ValueError(msg)


@dataclass
class YamlLoader:
    """
    class to help load and parse yaml files
    """

    yaml_file: str
    multiple_docs: bool = field(default=False)

    def __post_init__(self):
        validate_yaml(self.yaml_file)


    def load(self):
        ''' load yaml data '''
        print(f'multiple_docs: {self.multiple_docs}')
        try:
            print(f'loading yaml file: {self.yaml_file}')
            with open(self.yaml_file, 'r', encoding="utf-8") as yaml_stream:
                documents = list(
                    yaml.load_all(yaml_stream, Loader=yaml.SafeLoader)
                )

                if not self.multiple_docs and len(documents) > 1:
                    msg = f'No documents loaded: loaded ' \
                           f'document count: {len(documents)}'
                    raise ValueError(msg)
                if len(documents) == 0:
                    msg = f'Too few documents loaded: loaded ' \
                          f'document count: {len(documents)}'
                    raise ValueError(msg)

        except yaml.YAMLError as err:
            msg = f'Cannot load yaml file: {self.yaml_file}, {err}'
            raise TypeError(msg) from err
        except Exception as err:
            msg = f'Unkown error when parsing {self.yaml_file}, err: {err}'
            raise ValueError(msg) from err

        return documents


    def get_value(self, key, document, return_type):
        """Lookup a key in a nested list of documents, return all matches"""

        found_keys = list(self._get_nested_key(key, document))

        print(f'values found: {found_keys}')

        if len(found_keys) > 1:
            msg = f'Key "{key}" found multiple times. Result ambiguous.'
            raise ValueError(msg)

        if len(found_keys) == 0:
            msg = f'Key "{key}" was not found in data: {document}.'
            raise ValueError(msg)

        is_expected_return_type(found_keys[0], return_type)
        return found_keys[0]


    def _get_nested_key(self, key, documents):
        if isinstance(documents, list):
            for doc in documents:
                for result in self._get_nested_key(key, doc):
                    yield result

        if isinstance(documents, dict):
            for k, value in documents.items():
                if key == k:
                    yield value
                if isinstance(value, dict):
                    for result in self._get_nested_key(key, value):
                        yield result
                elif isinstance(value, list):
                    for doc in value:
                        for result in self._get_nested_key(key, doc):
                            yield result
