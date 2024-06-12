#!/home/argoserver/anaconda3/bin/python
#!/usr/local/anaconda3/bin/python
#
# H. Frenzel, UW-CICOES // NOAA-PMEL
#
# First version: March 22, 2024


import argparse
import glob
import os
import re
#from erddapy import ERDDAP

import pandas as pd

import pdb

BASE_PATH_NAVIS = '/home/argoserver/deploy'
PMEL_ERDDAP = 'https://data.pmel.noaa.gov/pmel/erddap/',
PARK_PRESSURE = 1000

def get_float_ids_erddap():
    '''The first filter will retrieve the internal floatid values
    for all Navis floats that have met the specified criteria at
    least once over the last year.'''
    e = ERDDAP(
        server = PMEL_ERDDAP,
        protocol = "tabledap"
        )
    e.response = "csv"
    e.dataset_id = "argo_eng_navis"
    e.contraints = {
        "time>'now-1year'",
        "ParkPressure0=1000",
        "DeepProfilePressure=2000",
        "DownTime=14400", # "PnPCycleLen=1"
        }
    e.variables = [
        "floatid"
        ]

    df = e.to_pandas()
    return df['floatid'].values
    

def get_float_dirs():
    '''Determine the serial numbers of all core Navis floats from the
    directories on argoserver.'''
    all_dirs = glob.glob(BASE_PATH_NAVIS + '/navis????')
    for dir in all_dirs:
        if not os.path.isdir(dir):
            print(f'not a dir:{dir}') # FIXME case not yet handled
    return [dir.split('/')[-1] for dir in all_dirs]

def check_mission_cfg(floats):
    '''Only floats whose current mission.cfg is the "vanilla" version
    should use the mission to adjust the time-of-day issue.
    This is the only non-empty line of a vanilla mission.cfg:
    Verbosity(2)
    '''
    vanilla_floats = []
    regex = re.compile(r'[^\s]') # anything other than whitespace
    for float_dir in floats:
        full_path = f'{BASE_PATH_NAVIS}/{float_dir}/mission.cfg'
        if not os.path.exists(full_path):
            print(f'WARNING: {full_path} not found (or readable)!')
            pdb.set_trace()
            continue
        with open(full_path, 'r') as file:
            lines = file.readlines()
        is_vanilla = False
        for line in lines:
            line = line.strip() # delete whitespace from both ends
            if line == 'Verbosity(2)':
                is_vanilla = True
            else:
                match_obj = regex.search(line)
                if match_obj:
                    is_vanilla = False
                    break # do not consider other lines
        if is_vanilla:
            floatid = float_dir.replace('navis', '')
            vanilla_floats.append(floatid)
    return vanilla_floats

         
def check_latest_mission_erddap(floatid):
    '''Check if the latest mission of one float has the
    required settings.'''
    e = ERDDAP(
        server = PMEL_ERDDAP,
        protocol = "tabledap"
        )
    e.response = "csv"
    e.dataset_id = "argo_eng_navis"
    # retrieve all data from the last year
    e.contraints = {
        "time>'now-1year'"
        }
    e.variables = [
        "floatid",
        "ParkPressure0",
        "DeepProfilePressure",
        "PnPCycleLen",
        "DownTime"
        ]
    df = e.to_pandas()
    pdb.set_trace()


def get_latest_msg_file(floatid):
    '''Determine the most recent msg file for one float based on the
    profile number in the file name.'''
    all_msg_files = glob.glob(BASE_PATH_NAVIS + '/navis' + floatid
                                        + f'/{floatid}.*.msg')
    if not all_msg_files:
        return None
    regex_prof = re.compile(re.escape(floatid) + r'\.(\d+)\.msg')
    all_profiles = []
    for msg_file in all_msg_files:
        match_obj = regex_prof.search(msg_file)
        if match_obj:
            all_profiles.append(int(match_obj.group(1)))
        else:
            raise ValueError('unexpected')
    return all_msg_files[pd.Series(all_profiles).idxmax()]


def check_mission_param(filename, param_name, param_value,
                        param_value2=None):
    '''Read the msg file named filename, check if parameter
    with the given name has the given value.
    Return True/False or raise an IOError if the file could
    not be read.'''
    try:
        with open(filename, encoding='utf-8') as f:
            lines = f.readlines()
    except:
        raise IOError(f'File "{filename}" could not be read')
    # not all parameters may be followed by a unit    
    regex_param = re.compile(re.escape(param_name) + r'\((\d+)\)')
    for line in lines:
        match_obj = regex_param.search(line)
        if match_obj:
            if param_value2:
                return (int(match_obj.group(1)) == param_value or
                        int(match_obj.group(1)) == param_value2)
            else:
                return int(match_obj.group(1)) == param_value

def parse_input_args():
    '''Parse the command line arguments and return them as an object.'''
    parser = argparse.ArgumentParser()
    # required argument:
    #parser.add_argument('filename_in', help='name of the input file')
    # option:
    parser.add_argument('--fn_out', default = None,
                        help='name of the output file')
    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_input_args()
    # 1045 has a non-vanilla mission.cfg and should NOT be changed!!!
    float_dirs = get_float_dirs()
    print(f'{len(float_dirs)} total floats found')
    floats = check_mission_cfg(float_dirs)
    print(f'vanilla mission: {len(floats)} floats')
    # the rest should be refactored into a new function
    print(f'The following floats need ToD adjustments:')
    adjust_count = 0
    for floatid in floats:
        #print(f'Detailed check for {floatid}')
        latest_msg = get_latest_msg_file(floatid)
        if not latest_msg:
            continue
        if (check_mission_param(latest_msg, 'ParkPressure', 1000) and
            check_mission_param(latest_msg, 'DeepProfilePressure', 2000) and
            check_mission_param(latest_msg, 'PnPCycleLen', 1) and
            check_mission_param(latest_msg, 'DownTime', 12960, 14400)):
            print(floatid)
            adjust_count += 1
    print(f'{adjust_count} floats need ToD adjustments.')        
