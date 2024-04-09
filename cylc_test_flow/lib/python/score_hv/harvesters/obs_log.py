"""
Work by Jessica Knezha

Collection of methods to facilitate observation information retrieval from log files

"""

from collections import namedtuple
from dataclasses import dataclass, field
import os

from score_hv.config_base import ConfigInterface

valid_variables = [
    'PRESSURE',
    'SPECIFIC HUMIDITY',
    'TEMPERATURE',
    'HEIGHT',
    'WIND COMPONENTS',
    'PRECIPITABLE H2O',
    'RELATIVE HUMIDITY'
]

HARVESTER_NAME = 'obs_info_log'

HarvestedData = namedtuple(
    'HarvestedData',
    [
        'file_name',
        'cycletime',
        'variable',
        'instrument_type',
        'number_obs',
        'number_obs_qc_0to3'
    ],
)


@dataclass
class ObsInfoCfg(ConfigInterface):
    """
    Dataclass to hold and provide configuration information
    pertaining to how the harvester should read log file

    Parameters:
    -----------
    config_data: dict - contains configuration data parsed from
                either an input yaml file or input dict

    harvest_variable: string name of variable to parse from config

    harvest_filename: string name of file to parse found in config

    """
    config_data: dict = field(default_factory=dict)
    harvest_variable: str = ''
    harvest_filename: str = ''

    def __post_init__(self):
         self.set_config()
         self.validate()

    # function to set configuration variables from given dictionary
    def set_config(self):
        try:
            self.harvest_variable = self.config_data.get('variable')
        except Exception as err:
            msg = f'\'variable\' key missing, must be one of ' \
                  f'({valid_variables}) - err: {err}'
            raise KeyError(msg) from err

        try:
            self.harvest_filename = self.config_data.get('filename')
        except Exception as err:
            msg = f'\'filename\' key missing, must be included for log file' \
                f' - err: {err}'
            raise KeyError(msg) from err

    # function to validate the values of the variable and filename for harvest
    # empty values raise a key error
    # invalid values raise a value error
    def validate(self):
        if self.harvest_variable is None:
            msg = f'\'variable\' key is empty, must be one of ' \
                  f'({valid_variables})'
            raise KeyError(msg)

        if self.harvest_filename is None:
            msg = f'\'filename\' key is empty, must be included for log file'
            raise KeyError(msg)

        if self.harvest_variable.upper() not in valid_variables :
            msg = f'given variable {self.harvest_variable} is ' \
                      f'not a valid variable type for harvester. valid options: {valid_variables}'
            raise ValueError(msg)

        if not os.path.exists(self.harvest_filename) :
            msg = f'given filename {self.harvest_filename} is not valid. ' \
                  f'valid filename required for harvester'
            raise ValueError(msg)


@dataclass
class ObsInfoHv:
    """
    Harvester dataclass used to parse observation information data stored in
    log files

    Parameters:
    -----------
    config: ObsInfoCfg object containing information used to determine
            which variable to parse the log file for

    Methods:
    --------
    get_data: parses observation information from log files based on input config
              file, returns a list of tuples containing specific data
    """
    config: ObsInfoCfg = field(default_factory=ObsInfoCfg)

    def get_data(self):
        """
        Harvests observation information (instrument type, number of observations,
        and number of high quality observations) for a specified variable of the
        set {Pressure, Specific Humidity, Temperature, Height, Wind Components,
        Precipitable H2O, Relative Humidity}. Input file is a log text file.

        Returns
        -------
        harvested_data: list of HarvestedData tuples each consisting of
                        file_name, cycletime, variable, instrument type,
                        number_obs, number_obs_qc_0to3
        """
        harvested_data = []

        with open(self.config.harvest_filename) as log_file:
            data_valid_line = log_file.readline().split()
            cycletime = data_valid_line.pop()  # last item represents the cycletime for the log file

            on_variable = False
            for line in log_file:
                # skip any lines which are all whitespace
                if line.isspace():
                    continue

                cleaned_line = line.strip() # strip to remove whitespace so calls can be to first character of line
                # skip lines which are all ----
                if cleaned_line[0] is '-':
                    continue

                # skip lines which are data for a different variable
                # assumes all data lines start with integer for instrument type per guidelines
                if not on_variable and cleaned_line[0].isdigit():
                    continue

                # either on a heading row for the variable or reached the next variable in the list
                if on_variable and cleaned_line[0].isalpha():
                    if cleaned_line[0:3] == 'typ':
                        continue
                    break

                # reached the beginning of the section for the variable to harvest, set flag
                if not on_variable and cleaned_line.upper() == self.config.harvest_variable.upper():
                    on_variable = True

                # harvest line of data and save to list
                if on_variable and line[0].isdigit():
                    split = cleaned_line.split()
                    type = split[0].replace('|', '')
                    num_obs = split[1].replace('|', '')
                    num_obs_qc = split[2].replace('|', '')
                    item = HarvestedData(
                        self.config.harvest_filename,
                        cycletime,
                        self.config.harvest_variable,
                        type,
                        num_obs,
                        num_obs_qc
                    )
                    harvested_data.append(item)

        return harvested_data
