"""
Copyright 2022 NOAA
All rights reserved.

Collection of methods to collect and provide information for score
statistics related to rnr experiments

"""
from abc import ABCMeta, abstractmethod


class ConfigInterface(metaclass=ABCMeta):
    """
    Config interface which enforces the implementation of
    a load and parse method
    """

    def __init__(self, config_dict):
        self.config_dict = config_dict

    @abstractmethod
    def set_config(self):
        """
        Function to be implemented by harvester config functions
        """

    def validate(self):
        """
        Function to be implemented by harvester config functions
        """
