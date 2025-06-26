#!/usr/bin/env python

"""store programs and data (request dictionaries) to interact with the
reanalysis experiment metrics database
"""

import json
from datetime import datetime

import numpy as np

from score_db import score_db_base

def get_request_dict():
    
    request_dict = {
        'db_request_name': 'expt_file_counts',
        'method': 'GET',
    #    'params': {'filters':
    #                  {'experiment':
    #                      {'experiment_name':
    #                          {'exact':
    #                             'gefsv13replay_0.25d_v0.1'}}}
        #}
    }

    return request_dict

def get_request_dict2(db_request_name):
    request_dict = {
        'db_request_name': db_request_name,
        'method': 'GET',
        'params':{'filters':
                      {'experiment':
                          {'experiment_name':
                              {'exact':
                                 'scout_runs_gsi3dvar_1979stream'}}}
        }
    }
    return request_dict

def get_experiments(experiment_names=list()):
    experiment_data = list()
    for experiment in experiment_names:
        request_dict = {
         'db_request_name': 'experiment',
         'method': 'GET',
         'params': {
            'filters': {

                #'cycle_start': {
                #    'from': '2015-01-01 00:00:00',
                #    'to': '2018-01-01 00:00:00'
                #},
                #'cycle_stop': {
                #    'from': '2015-01-01 00:00:00',
                #    'to': '2018-01-01 00:00:00'
                #},
                #'owner_id': {
                #    'exact': 'first.last@noaa.gov'
                #},
                'name': {
                    'exact': experiment
                },
                #'platform': {
                #    'exact': 'pw_awv1'
                #},
                #'wallclock_start': {
                #    'from': '2022-01-01 00:00:00',
                #    'to': '2022-07-01 00:00:00'
                #},

            },
            #'ordering': [
            #    {'name': 'group_id', 'order_by': 'desc'},
            #    {'name': 'created_at', 'order_by': 'desc'}
            #],
            #'record_limit': 4
          }
        }
        experiment_data.append(score_db_base.handle_request(request_dict))
    if len(experiment_names)==0:
        experiment_data.append(score_db_base.handle_request(
                                              {'db_request_name': 'experiment',
                                               'method': 'GET'}))
    return experiment_data

def put_new_experiment(name, cycle_start='1978100100', cycle_stop='2025093018',
                       owner_id='Adam.Schneider', group_id='gsienkf',
                       experiment_type='replay_observer_diagnostic',
                       platform = "pw_awv2",
                       wall_clock_start='2025-04-15 00:00:00',
                       wall_clock_end='2025-10-01 00:00:00',
                       description=None):
 
    """ put experiment meta data into database

    cycle_start = 'YYYYmmddHH'
    cycle_stop = 'YYYYmmddHH'

    wall_clock_start = 'YYYY-mm-dd HH:MM:SS'
    wall_clock_end = 'YYYY-mm-dd HH:MM:SS'
    """
    datetime_start = datetime.strptime(cycle_start,'%Y%m%d%H')
    datetime_stop = datetime.strptime(cycle_stop, '%Y%m%d%H')

    datetime_wall_clock_start = datetime.strptime(wall_clock_start, '%Y-%m-%d %H:%M:%S')
    datetime_wall_clock_end = datetime.strptime(wall_clock_end, '%Y-%m-%d %H:%M:%S')
    
    if description != None:
        json_description = json.dumps({"experiment configuration": description})
    else:
        json_description = json.dumps({"experiment configuration": experiment_type})
    
    datestr_format = '%Y-%m-%d %H:%M:%S'
    request_dict = {
        'db_request_name': 'experiment',
        'method': 'PUT',
        'body': {
            'name': name,
            'datestr_format': datestr_format,
            'cycle_start': datetime_start.strftime(datestr_format),
            'cycle_stop': datetime_stop.strftime(datestr_format),
            'owner_id': owner_id,
            'group_id': group_id,
            'experiment_type': experiment_type,
            'platform': platform,
            'wallclock_start': datetime_wall_clock_start.strftime(datestr_format),
            'wallclock_end': datetime_wall_clock_end.strftime(datestr_format),
            'description': json_description
        }
    }
    
    return score_db_base.handle_request(request_dict)

def put_array_metric_type(name, measurement_type,
                          coordinate_labels,
                          coordinate_values,
                          coordinate_units,
                          coordinate_lengths,
                          instrument=None, obs_platform=None,
                          long_name=None,
                          measurement_units=None, stat_type=None,
                          description=None):
    request_dict = {
        'db_request_name': 'array_metric_types',
        'method': 'PUT',
        'body': {'instrument_meta_name': instrument,
                 'obs_platform': obs_platform,
                 'name': name,
                 'long_name': long_name,
                 'measurement_type': measurement_type,
                 'measurement_units': measurement_units,
                 'stat_type': stat_type,
                 'array_coord_labels': coordinate_labels,
                 'array_coord_units': coordinate_units,
                 'array_index_values': coordinate_values,
                 'array_dimensions': coordinate_lengths,
                 'description': json.dumps({"description": description})
        }}
    return score_db_base.handle_request(request_dict)

def put_scalar_metric_type(name, measurement_type,
                           instrument_meta_name=None,
                           stage=None,
                           obs_platform=None,
                           long_name=None,
                           measurement_units=None,
                           stat_type=None,
                           description=None):
    request_dict = {
        'db_request_name': 'metric_types',
        'method': 'PUT',
        'body': {'name': name,
                 'obs_platform': obs_platform,
                 'name': name,
                 'long_name': long_name,
                 'stat_type': stat_type,
                 'measurement_type': measurement_type,
                 'measurement_units': measurement_units,
                 'stat_type': stat_type,
                 'stage' : stage,
                 'instrument_meta_name': instrument_meta_name,
                 'description': json.dumps({"description": description})
        }}
    return score_db_base.handle_request(request_dict)

def register_instrument_meta(instrument_name, num_channels, scan_angle=None):
    request_dict = {'db_request_name': 'instrument_meta', 'method': 'PUT',
        'body': {'name': instrument_name,
                 'num_channels': num_channels
        }
    }
    return score_db_base.handle_request(request_dict)

def get_instrument_meta():
    request_dict = {'db_request_name': 'instrument_meta', 'method': 'GET'}
    return score_db_base.handle_request(request_dict)

def register_sat_meta(name, sat_id=None, long_name=None, short_name=None):
    request_dict = {'db_request_name': 'sat_meta', 'method': 'PUT',
        'body': {'name': name,
                 'sat_id': sat_id,
                 'sat_name': long_name,
                 'short_name': short_name,
                 #'description': json.dumps({"description": None})
        }
    }
    return score_db_base.handle_request(request_dict)

def put_these_data():
    """
    """
    my_instruments = {#'abi': 10,
                      #'ahi': 10,
                      #'airs': 281,
                      #'amsre':12, # NASA
                      #'amsr2':14, # NASA
                      #'amsua': 15,
                      #'amsub': 5,
                      #'atms': 22,
                      #'avhrr2': 3,
                      #'avhrr3': 3,
                      #'cris': 399,
                      #'cris-fsr': 431,
                      #'gmi': 13,
                      #'hirs2': 19,
                      #'hirs3': 19,
                      #'hirs4': 19,
                      #'iasi': 616,
                      #'mhs': 5,
                      #'msu': 4,
                      #'saphir': 6,
                      #'seviri': 8,
                      #'sndrD1': 18,
                      #'sndrD2': 18,
                      #'sndrD3': 18,
                      #'sndrD4': 18,
                      #'sndr': 18,
                      #'ssmi': 7,
                      #'ssmis': 24,
                      #'ssu': 3,
                      #'tmi':9 # NASA,
                      #'imgr':4,
                      #'ctd':None,
                      #'xbt': None,
                      'mbt': None,
                      'osd': None,
                  }
    for instrument, num_channels in my_instruments.items():
        register_instrument_meta(instrument, num_channels)

def put_these_sats():
    """
    """
    my_sats = {
    #"NOAA 5": {"sat_id": 705, "short_name": "n05"},
    #"NOAA 6": {"sat_id": 706, "short_name": "n06"},
    #"NOAA 7": {"sat_id": 707, "short_name": "n07"},
    #"NOAA 8": {"sat_id": 708, "short_name": "n08"},
    #"NOAA 9": {"sat_id": 709, "short_name": "n09"},
    #"NOAA 10": {"sat_id": 710, "short_name": "n10"},
    #"NOAA 11": {"sat_id": 711, "short_name": "n11"},
    #"NOAA 12": {"sat_id": 712, "short_name": "n12"},
    #"NOAA 13": {"sat_id": 713, "short_name": "n13"},
    #"NOAA 14": {"sat_id": 714, "short_name": "n14"},
    #"NOAA 15": {"sat_id": 715, "short_name": "n15"},
    #"NOAA 16": {"sat_id": 716, "short_name": "n16"},
    #"NOAA 17": {"sat_id": 717, "short_name": "n17"},
    #"NOAA 18": {"sat_id": 718, "short_name": "n18"},
    #"NOAA 19": {"sat_id": 719, "short_name": "n19"},
    #"NOAA 20": {"sat_id": 720, "short_name": "n20"},
    #"NOAA 21": {"sat_id": 721, "short_name": "n21"},
    "MetOp-B": {"sat_id": 3, "short_name": "metop-b"},
    "MetOp-A": {"sat_id": 4, "short_name": "metop-a"},
    "MetOp-C": {"sat_id": 5, "short_name": "metop-c"},
    #"METOP-1 (Metop-A": {"sat_id": 4, "short_name": "metop-b"},
    #"METOP-2 (Metop-B": {"sat_id": 3, "short_name": "metop-a"},
    #"METOP-3 (Metop-C": {"sat_id": 5, "short_name": "metop-c"},
    #"AQUA": {"sat_id": 784, "short_name": "aqua"},
    #"NPP": {"sat_id": 224, "short_name": "npp"},
    #"GOES 7": {"sat_id": 251, "short_name": "g07"},
    #"GOES 8": {"sat_id": 252, "short_name": "g08"},
    #"GOES 9": {"sat_id": 253, "short_name": "g09"},
    #"GOES 10": {"sat_id": 254, "short_name": "g10"},
    #"GOES 11": {"sat_id": 255, "short_name": "g11"},
    #"GOES 12": {"sat_id": 256, "short_name": "g12"},
    #"GOES 13": {"sat_id": 257, "short_name": "g13"},
    #"GOES 14": {"sat_id": 258, "short_name": "g14"},
    #"GOES 15": {"sat_id": 259, "short_name": "g15"},
    #"GOES 16": {"sat_id": 270, "short_name": "g16"},
    #"GOES 17": {"sat_id": 271, "short_name": "g17"},
    #"GOES 18": {"sat_id": 272, "short_name": "g18"},
    #"MTSAT-2": {"sat_id": 172, "short_name": "MTSAT-2"},
    #"MTSAT-1R": {"sat_id": 171, "short_name": "MTSAT-1R"},
    #"METEOSAT 2": {"sat_id": 59, "short_name": "m02"},
    #"METEOSAT 3": {"sat_id": 51, "short_name": "m03"},
    #"METEOSAT 4": {"sat_id": 52, "short_name": "m04"},
    #"METEOSAT 5": {"sat_id": 53, "short_name": "m05"},
    #"METEOSAT 6": {"sat_id": 54, "short_name": "m06"},
    #"METEOSAT 7": {"sat_id": 55, "short_name": "m07"},
    #"METEOSAT 8": {"sat_id": 56, "short_name": "m08"},
    #"METEOSAT 9": {"sat_id": 57, "short_name": "m09"},
    #"METEOSAT 10": {"sat_id": 67, "short_name": "m10"},
    #"METEOSAT 11": {"sat_id": 70, "short_name": "m11"},
    #"DMSP 8": {"sat_id": 241, "short_name": "f08"},
    #"DMSP 9": {"sat_id": 242, "short_name": "f09"},
    #"DMSP 10": {"sat_id": 243, "short_name": "f10"},
    #"DMSP 11": {"sat_id": 244, "short_name": "f11"},
    #"DMSP 12": {"sat_id": 245, "short_name": "f12"},
    #"DMSP 13": {"sat_id": 246, "short_name": "f13"},
    #"DMSP 14": {"sat_id": 247, "short_name": "f14"},
    #"DMSP 15": {"sat_id": 248, "short_name": "f15"},
    #"DMSP 16": {"sat_id": 249, "short_name": "f16"},
    #"DMSP17": {"sat_id": 285, "short_name": "f17"},
    #"DMSP18": {"sat_id": 286, "short_name": "f18"},
    #"DMSP19": {"sat_id": 39630, "short_name": "f19"},
    #"DMSP20": {"sat_id": 41705, "short_name": "f20"},
    #"CHAMP": {"sat_id": 41, "short_name": "CHAMP"},
    #"COSMIC-1": {"sat_id": 740, "short_name": "COSMIC-1"},
    #"COSMIC-2": {"sat_id": 741, "short_name": "COSMIC-2"},
    #"COSMIC-3": {"sat_id": 742, "short_name": "COSMIC-3"},
    #"COSMIC-4": {"sat_id": 743, "short_name": "COSMIC-4"},
    #"COSMIC-5": {"sat_id": 744, "short_name": "COSMIC-5"},
    #"COSMIC-6": {"sat_id": 745, "short_name": "COSMIC-6"},
    #"COSMIC-2 E1": {"sat_id": 750, "short_name": "COSMIC-2 E1"},
    #"COSMIC-2 E2": {"sat_id": 751, "short_name": "COSMIC-2 E2"},
    #"COSMIC-2 E3": {"sat_id": 752, "short_name": "COSMIC-2 E3"},
    #"COSMIC-2 E4": {"sat_id": 753, "short_name": "COSMIC-2 E4"},
    #"COSMIC-2 E5": {"sat_id": 754, "short_name": "COSMIC-2 E5"},
    #"COSMIC-2 E6": {"sat_id": 755, "short_name": "COSMIC-2 E6"},
    #"GRACE A": {"sat_id": 722, "short_name": "GRACE A"},
    #"GRACE B": {"sat_id": 723, "short_name": "GRACE B"},
    #"GRACE C (GRACE-F": {"sat_id": 803, "short_name": "GRACE C"},
    #"GRACE D (GRACE-F": {"sat_id": 804, "short_name": "GRACE D"},
    #"SAC-C": {"sat_id": 820, "short_name": "SAC C"},
    #"TerraSAR-X": {"sat_id": 42, "short_name": "TerraSAR-X"},
    #"TERRA": {"sat_id": 783, "short_name": "TERRA"},
    #"ERS 2": {"sat_id": 2, "short_name": "ERS 2"},
    #"GMS 3": {"sat_id": 150, "short_name": "GMS 3"},
    #"GMS 4": {"sat_id": 151, "short_name": "GMS 4"},
    #"GMS 5": {"sat_id": 152, "short_name": "GMS 5"},
    #"INSAT 3A": {"sat_id": 470, "short_name": "INSAT 3A"},
    #"INSAT 3D": {"sat_id": 471, "short_name": "INSAT 3D"},
    #"INSAT 3DR": {"sat_id": 472, "short_name": "INSAT 3DR"},
    #"TIROS-N": {"sat_id": 254, "short_name": "tirosn"},
    #"Megha-Tropiques": {"sat_id": 367, "short_name": "meghat"},
    #"TanDEM-X": {"sat_id": 551, "short_name": "TanDEM-X"},
    #"PAZ": {"sat_id": 431, "short_name": "PAZ"},
    #"KOMPSAT-5": {"sat_id": 536, "short_name": "KOMPSAT-5"},
    #"LANDSAT 5": {"sat_id": 207, "short_name": "LANDSAT 5"},
    #"GPM-core": {"sat_id": 371, "short_name": "gpm"},
    #"TRMM": {"sat_id": 241, "short_name": "TRMM"},
    #"Himawari-8": {"sat_id": 370, "short_name": "himawari8"},
    #"Himawari-9": {"sat_id": 372, "short_name": "himawari9"},
    #"Spire Lemur 3U C": {"sat_id": 409, "short_name": "Spire L3UC"},
    #"Sentinel 6A": {"sat_id": 835, "short_name": "Sentinel 6A"},
    #"PlanetiQ GNOMES-": {"sat_id": 687, "short_name": "PlanetiQ GNOMES"},
    #"AURA": {"sat_id": 296, "short_name": "AURA"},
    #"NIMBUS 7": {"sat_id": 16, "short_name": "nim07"}
}
    for name, sat_dict in my_sats.items():
        register_sat_meta(name, sat_id=sat_dict['sat_id'], long_name=name, short_name=sat_dict['short_name'])
        #print(name, sat_dict['sat_id'], name, sat_dict['short_name'])
def put_these_data2():
    my_instruments = get_instrument_channels()
    my_stats = ('var',
                'varch_cld',
                'use',
                'ermax',
                'b_rad',
                'pg_rad',
                'icld_det',
                'icloud',
                'iaeros',
                'nobs_used',
                'nobs_tossed',
                'variance',
                'bias_pre_corr',
                'bias_post_corr',
                'penalty',
                'sqrt_bias',
                'std')
    #its = (1, 2, 'None')
    #its = [1,2,3,'None']
    its = [None]

    for instrument, channel_list in my_instruments.items():
        channels = np.array(channel_list)
        for stat in my_stats:
            for gsi_stage in its:
                name = instrument + "_" + stat + "_GSIstage_" + str(gsi_stage)
                put_array_metric_type(name, 'brightness temperature',
                          ['channel'], channels,
                          ['number'],
                          [channels.size],
                          instrument=instrument, obs_platform='satellite',
                          long_name=None,
                          measurement_units='K', stat_type=stat,
                          description=None)

def put_these_data3():
    my_instruments = [#'ctd', 'xbt',
                      'mbt',
                      'osd'
                      ]
    my_stats = ['rms',
                'mean',
                'median',
                'StdDev',
                'minimum',
                'maximum',
                'count']
    my_vars = ['waterTemperature']
    groups = ('ObsValue','oman','ombg', 'ObsError')

    for instrument in my_instruments:
        if instrument == ' ctd':
            long_name = 'conductivity, temperature, and depth'
        elif instrument == 'xbt':
            long_name = 'expendable bathythermograph'
        elif instrument == 'mbt':
            long_name = 'Mechanical bathyhermograph'
        elif instrument == 'osd':
            long_name = "ocean station data"
        else:
            long_name = None
        for stat in my_stats:
            for var in my_vars:
                for group in groups:
                    name = f"{stat}_{var}_{instrument}_{group}"
                    put_scalar_metric_type(
                        name,
                        'water temperature',
                        instrument_meta_name=instrument,
                        obs_platform='ship',
                        long_name=long_name,
                        measurement_units='C', stat_type=stat,
                        description=None
                    )

def run(request='array_metric_types'):
    return score_db_base.handle_request(get_request_dict2(request))

def main():
    run()

if __name__=='__main__':
    main()

def get_instrument_channels():
    instrument_channels = {
        #'abi': [7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        #'ahi': [7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        #'airs': [1, 6, 7, 10, 11, 15, 16, 17, 20, 21, 22, 24, 27, 28, 30, 36, 39, 40, 42, 51, 52, 54, 55, 56, 59, 62, 63, 68, 69, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 82, 83, 84, 86, 92, 93, 98, 99, 101, 104, 105, 108, 110, 111, 113, 116, 117, 123, 124, 128, 129, 138, 139, 144, 145, 150, 151, 156, 157, 159, 162, 165, 168, 169, 170, 172, 173, 174, 175, 177, 179, 180, 182, 185, 186, 190, 192, 198, 201, 204, 207, 210, 215, 216, 221, 226, 227, 232, 252, 253, 256, 257, 261, 262, 267, 272, 295, 299, 300, 305, 310, 321, 325, 333, 338, 355, 362, 375, 453, 475, 484, 497, 528, 587, 672, 787, 791, 843, 870, 914, 950, 1003, 1012, 1019, 1024, 1030, 1038, 1048, 1069, 1079, 1082, 1083, 1088, 1090, 1092, 1095, 1104, 1111, 1115, 1116, 1119, 1120, 1123, 1130, 1138, 1142, 1178, 1199, 1206, 1221, 1237, 1252, 1260, 1263, 1266, 1285, 1301, 1304, 1329, 1371, 1382, 1415, 1424, 1449, 1455, 1466, 1477, 1500, 1519, 1538, 1545, 1565, 1574, 1583, 1593, 1614, 1627, 1636, 1644, 1652, 1669, 1674, 1681, 1694, 1708, 1717, 1723, 1740, 1748, 1751, 1756, 1763, 1766, 1771, 1777, 1780, 1783, 1794, 1800, 1803, 1806, 1812, 1826, 1843, 1852, 1865, 1866, 1868, 1869, 1872, 1873, 1876, 1881, 1882, 1883, 1911, 1917, 1918, 1924, 1928, 1937, 1941, 2099, 2100, 2101, 2103, 2104, 2106, 2107, 2108, 2109, 2110, 2111, 2112, 2113, 2114, 2115, 2116, 2117, 2118, 2119, 2120, 2121, 2122, 2123, 2128, 2134, 2141, 2145, 2149, 2153, 2164, 2189, 2197, 2209, 2226, 2234, 2280, 2318, 2321, 2325, 2328, 2333, 2339, 2348, 2353, 2355, 2357, 2363, 2370, 2371, 2377],
        #'amsre' : [1,2,3,4,5,6,7,8,9,10,11,12],
        #'amsr2': [1,2,3,4,5,6,7,8,9,10,11,12,13,14],
        #'amsua': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        #'amsub': [1, 2, 3, 4, 5],
        #'atms': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22],
        #'avhrr2': [3, 4, 5],
        #'avhrr3': [3, 4, 5],
        #'cris': [27, 28, 31, 32, 33, 37, 49, 51, 53, 59, 61, 63, 64, 65, 67, 69, 71, 73, 75, 79, 80, 81, 83, 85, 87, 88, 89, 93, 95, 96, 99, 101, 102, 104, 106, 107, 111, 113, 116, 120, 123, 124, 125, 126, 130, 132, 133, 136, 137, 138, 142, 143, 144, 145, 147, 148, 150, 151, 153, 154, 155, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 168, 170, 171, 173, 175, 181, 183, 198, 208, 211, 216, 224, 228, 236, 238, 242, 248, 266, 268, 279, 283, 311, 317, 330, 333, 334, 338, 340, 341, 342, 349, 352, 358, 361, 364, 366, 367, 368, 378, 390, 391, 392, 394, 395, 396, 397, 398, 399, 404, 427, 447, 464, 473, 482, 484, 501, 529, 556, 557, 558, 560, 561, 562, 564, 565, 566, 569, 573, 574, 577, 580, 581, 584, 585, 587, 590, 591, 594, 597, 598, 601, 604, 607, 611, 614, 616, 617, 619, 622, 626, 628, 634, 637, 638, 640, 641, 642, 644, 646, 647, 650, 651, 652, 654, 655, 657, 659, 663, 667, 670, 707, 710, 713, 716, 730, 735, 736, 739, 743, 744, 746, 748, 751, 754, 755, 756, 757, 758, 760, 761, 762, 763, 766, 767, 768, 771, 772, 773, 776, 777, 778, 779, 780, 782, 783, 784, 785, 786, 787, 788, 789, 790, 791, 792, 794, 796, 798, 800, 802, 803, 804, 806, 807, 808, 809, 811, 812, 814, 816, 819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832, 833, 834, 835, 836, 838, 839, 840, 842, 843, 844, 845, 846, 847, 848, 849, 850, 851, 852, 853, 854, 856, 861, 862, 864, 865, 866, 867, 869, 871, 872, 874, 876, 878, 879, 880, 884, 886, 887, 888, 889, 890, 900, 921, 924, 927, 945, 991, 994, 1007, 1015, 1030, 1094, 1106, 1130, 1132, 1133, 1135, 1142, 1147, 1148, 1149, 1150, 1151, 1152, 1153, 1154, 1155, 1156, 1157, 1158, 1159, 1160, 1161, 1162, 1163, 1164, 1165, 1166, 1167, 1168, 1169, 1170, 1171, 1172, 1173, 1174, 1175, 1177, 1178, 1179, 1180, 1181, 1187, 1189, 1190, 1192, 1193, 1194, 1196, 1197, 1198, 1199, 1200, 1202, 1203, 1204, 1206, 1207, 1208, 1210, 1212, 1214, 1215, 1217, 1218, 1220, 1222, 1224, 1226, 1228, 1229, 1231, 1232, 1234, 1235, 1236, 1237, 1238, 1239, 1241, 1242, 1243, 1244, 1245, 1247, 1250, 1270, 1271, 1282, 1285, 1288, 1290, 1293, 1298, 1301],
        #'cris-fsr': [19, 24, 26, 27, 28, 31, 32, 33, 37, 39, 42, 44, 47, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 208, 211, 216, 224, 234, 236, 238, 239, 242, 246, 248, 255, 264, 266, 268, 275, 279, 283, 285, 291, 295, 301, 305, 311, 332, 342, 389, 400, 402, 404, 406, 410, 427, 439, 440, 441, 445, 449, 455, 458, 461, 464, 467, 470, 473, 475, 482, 486, 487, 490, 493, 496, 499, 501, 503, 505, 511, 513, 514, 518, 519, 520, 522, 529, 534, 563, 568, 575, 592, 594, 596, 598, 600, 602, 604, 611, 614, 616, 618, 620, 622, 626, 631, 638, 646, 648, 652, 659, 673, 675, 678, 684, 688, 694, 700, 707, 710, 713, 714, 718, 720, 722, 725, 728, 735, 742, 748, 753, 762, 780, 784, 798, 849, 860, 862, 866, 874, 882, 890, 898, 906, 907, 908, 914, 937, 972, 973, 978, 980, 981, 988, 995, 998, 1000, 1003, 1008, 1009, 1010, 1014, 1017, 1018, 1020, 1022, 1024, 1026, 1029, 1030, 1032, 1034, 1037, 1038, 1041, 1042, 1044, 1046, 1049, 1050, 1053, 1054, 1058, 1060, 1062, 1064, 1066, 1069, 1076, 1077, 1080, 1086, 1091, 1095, 1101, 1109, 1112, 1121, 1128, 1133, 1163, 1172, 1187, 1189, 1205, 1211, 1219, 1231, 1245, 1271, 1289, 1300, 1313, 1316, 1325, 1329, 1346, 1347, 1473, 1474, 1491, 1499, 1553, 1570, 1596, 1602, 1619, 1624, 1635, 1939, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947, 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965, 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 2119, 2140, 2143, 2147, 2153, 2158, 2161, 2168, 2171, 2175, 2182],
        #'gmi': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        #'hirs2': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        #'hirs3': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        #'hirs4': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        #'iasi': [16, 29, 32, 35, 38, 41, 44, 47, 49, 50, 51, 53, 55, 56, 57, 59, 61, 62, 63, 66, 68, 70, 72, 74, 76, 78, 79, 81, 82, 83, 84, 85, 86, 87, 89, 92, 93, 95, 97, 99, 101, 103, 104, 106, 109, 110, 111, 113, 116, 119, 122, 125, 128, 131, 133, 135, 138, 141, 144, 146, 148, 150, 151, 154, 157, 159, 160, 161, 163, 167, 170, 173, 176, 179, 180, 185, 187, 191, 193, 197, 199, 200, 202, 203, 205, 207, 210, 212, 213, 214, 217, 218, 219, 222, 224, 225, 226, 228, 230, 231, 232, 236, 237, 239, 243, 246, 249, 252, 254, 259, 260, 262, 265, 267, 269, 275, 279, 282, 285, 294, 296, 299, 300, 303, 306, 309, 313, 320, 323, 326, 327, 329, 332, 335, 345, 347, 350, 354, 356, 360, 363, 366, 371, 372, 373, 375, 377, 379, 381, 383, 386, 389, 398, 401, 404, 405, 407, 408, 410, 411, 414, 416, 418, 423, 426, 428, 432, 433, 434, 439, 442, 445, 450, 457, 459, 472, 477, 483, 509, 515, 546, 552, 559, 566, 571, 573, 578, 584, 594, 625, 646, 662, 668, 705, 739, 756, 797, 867, 906, 921, 1027, 1046, 1090, 1098, 1121, 1133, 1173, 1191, 1194, 1222, 1271, 1283, 1338, 1409, 1414, 1420, 1424, 1427, 1430, 1434, 1440, 1442, 1445, 1450, 1454, 1460, 1463, 1469, 1474, 1479, 1483, 1487, 1494, 1496, 1502, 1505, 1509, 1510, 1513, 1518, 1521, 1526, 1529, 1532, 1536, 1537, 1541, 1545, 1548, 1553, 1560, 1568, 1574, 1579, 1583, 1585, 1587, 1606, 1626, 1639, 1643, 1652, 1658, 1659, 1666, 1671, 1675, 1681, 1694, 1697, 1710, 1786, 1791, 1805, 1839, 1884, 1913, 1946, 1947, 1991, 2019, 2094, 2119, 2213, 2239, 2271, 2289, 2321, 2333, 2346, 2349, 2352, 2359, 2367, 2374, 2398, 2426, 2562, 2701, 2741, 2745, 2760, 2819, 2889, 2907, 2910, 2919, 2921, 2939, 2944, 2945, 2948, 2951, 2958, 2971, 2977, 2985, 2988, 2990, 2991, 2993, 3002, 3008, 3014, 3027, 3029, 3030, 3036, 3047, 3049, 3052, 3053, 3055, 3058, 3064, 3069, 3087, 3093, 3098, 3105, 3107, 3110, 3116, 3127, 3129, 3136, 3146, 3151, 3160, 3165, 3168, 3175, 3178, 3189, 3207, 3228, 3244, 3248, 3252, 3256, 3263, 3281, 3295, 3303, 3309, 3312, 3322, 3326, 3354, 3366, 3375, 3378, 3411, 3416, 3432, 3438, 3440, 3442, 3444, 3446, 3448, 3450, 3452, 3454, 3458, 3467, 3476, 3484, 3491, 3497, 3499, 3504, 3506, 3509, 3518, 3527, 3555, 3575, 3577, 3580, 3582, 3586, 3589, 3599, 3610, 3626, 3638, 3646, 3653, 3658, 3661, 3673, 3689, 3700, 3710, 3726, 3763, 3814, 3841, 3888, 4032, 4059, 4068, 4082, 4095, 4160, 4234, 4257, 4411, 4498, 4520, 4552, 4567, 4608, 4646, 4698, 4808, 4849, 4920, 4939, 4947, 4967, 4991, 4996, 5015, 5028, 5056, 5128, 5130, 5144, 5170, 5178, 5183, 5188, 5191, 5368, 5371, 5379, 5381, 5383, 5397, 5399, 5401, 5403, 5405, 5446, 5455, 5472, 5480, 5483, 5485, 5492, 5497, 5502, 5507, 5509, 5517, 5528, 5558, 5697, 5714, 5749, 5766, 5785, 5798, 5799, 5801, 5817, 5833, 5834, 5836, 5849, 5851, 5852, 5865, 5869, 5881, 5884, 5897, 5900, 5916, 5932, 5948, 5963, 5968, 5978, 5988, 5992, 5994, 5997, 6003, 6008, 6023, 6026, 6039, 6053, 6056, 6067, 6071, 6082, 6085, 6098, 6112, 6126, 6135, 6140, 6149, 6154, 6158, 6161, 6168, 6174, 6182, 6187, 6205, 6209, 6213, 6317, 6339, 6342, 6366, 6381, 6391, 6489, 6962, 6966, 6970, 6975, 6977, 6982, 6985, 6987, 6989, 6991, 6993, 6995, 6997, 6999, 7000, 7004, 7008, 7013, 7016, 7021, 7024, 7027, 7029, 7032, 7038, 7043, 7046, 7049, 7069, 7072, 7076, 7081, 7084, 7089, 7099, 7209, 7222, 7231, 7235, 7247, 7267, 7269, 7284, 7389, 7419, 7423, 7424, 7426, 7428, 7431, 7436, 7444, 7475, 7549, 7584, 7665, 7666, 7831, 7836, 7853, 7865, 7885, 7888, 7912, 7950, 7972, 7980, 7995, 8007, 8015, 8055, 8078],
        #'mhs': [1, 2, 3, 4, 5],
        #'msu': [1, 2, 3, 4],
        #'saphir': [1, 2, 3, 4, 5, 6],
        #'seviri': [4, 5, 6, 7, 8, 9, 10, 11],
        #'sndrD1': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
        #'sndrD2': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
        #'sndrD3': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
        #'sndrD4': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
        #'sndr': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
        #'ssmi': [1, 2, 3, 4, 5, 6, 7],
        #'ssmis': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24],
        #'ssu': [1, 2, 3],
        #'tmi': [1, 2, 3, 4, 5, 6, 7,8,9], # NASA
        'imgr': [2,3,4,5], # GDAS 
    }
    return instrument_channels
