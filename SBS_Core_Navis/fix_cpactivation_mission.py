#!/home/argoserver/anaconda3/bin/python
#!/usr/local/anaconda3/bin/python
#
# H. Frenzel, UW-CICOES // NOAA-PMEL
#
# First version: January 14, 2025

'''
This script change the Cp activation depth for selected core Navis floats.
For all floats that meet certain criteria, the previous mission.cfg
is replaced with a mission.cfg that changes the Cp activation pressure
to 1700 dbar.
A spreadsheet is created to keep track of the fixes.
'''

import glob
import os
import re
import shutil
import time
from datetime import datetime
import pandas as pd


BASE_PATH_NAVIS = '/home/argoserver/deploy/'
MAX_AGE_MSG = 90 # days
SPREADSHEET = 'need_CP_reset.xlsx'
NEW_COLUMNS = ['mission_cfg_ToD', 'meets_criteria', 'date_change_cp_mission']
CHANGE_MISSION_CFG = 'CPactivation_1700.cfg'


def get_float_dirs():
    '''Determine the serial numbers of all core Navis floats from the
    directories on argoserver.'''
    all_dirs = glob.glob(BASE_PATH_NAVIS + 'navis????')
    for this_dir in all_dirs:
        if not os.path.isdir(this_dir):
            raise FileNotFoundError(f'not a dir:{this_dir}')
    return [this_dir.split('/')[-1] for this_dir in all_dirs]


def read_spreadsheet():
    '''If the spreadsheet does not exist yet, raise an IOError.
    If it exists already, check if all required rows are present,
    and add them if needed.'''
    if not os.path.exists(SPREADSHEET):
        raise IOError(f'{SPREADSHEET} not found!')

    df = pd.read_excel(SPREADSHEET)
    for col in NEW_COLUMNS:
        df[col] = False
    return df


def check_mission_cfg(df):
    '''Only floats whose current mission.cfg is the ToD change
    should use the mission to adjust the CP activation pressure.
    These are the only non-empty line of that mission.cfg:
    DownTime(14328)                 [0x2986]
    TimeOfDay(-1)                   [0x6c1c]
    '''
    regex = re.compile(r'[^\s]') # anything other than whitespace
    for serial_no in df['SN']:
        this_row = df[df['SN'] == serial_no]
        if this_row[' Vanilla'].values[0] != 1:
            print(f'Mission was already changed for {serial_no}')
            continue
        full_path = f'{BASE_PATH_NAVIS}navis{serial_no:04d}/mission.cfg'
        if not os.path.exists(full_path):
            print(f'WARNING: {full_path} not found (or readable)!')
            continue
        with open(full_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        line1_found = False
        line1 = 'DownTime(14328)                 [0x2986]'
        line2_found = False
        line2 = 'TimeOfDay(-1)                   [0x6c1c]'
        for line in lines:
            line = line.strip() # delete whitespace from both ends
            if line == line1:
                line1_found = True
            elif line == line2:
                line2_found = True
            else:
                match_obj = regex.search(line)
                if match_obj:
                    line1_found = False
                    break # do not consider other lines
        df.loc[df['SN'] == serial_no, 'mission_cfg_ToD'] = \
            line1_found & line2_found
    return df


def get_latest_msg_file(serial_no):
    '''Determine the most recent msg file for one float based on the
    profile number in the file name.'''
    floatid = f'{serial_no:04d}'
    all_msg_files = glob.glob(BASE_PATH_NAVIS + 'navis' + floatid
                                        + f'/{floatid}.???.msg')
    if not all_msg_files:
        return None

    regex_prof = re.compile(re.escape(floatid) + r'\.(\d+)\.msg')
    all_profiles = []
    for msg_file in all_msg_files:
        match_obj = regex_prof.search(msg_file)
        if match_obj:
            all_profiles.append(int(match_obj.group(1)))
        else:
            print(f'SKIPPING unexpected msg file: {msg_file}')
    return all_msg_files[pd.Series(all_profiles).idxmax()]


def check_mission_param(filename, param_name, param_value,
                        param_value2=None):
    '''Read the msg file named filename, check if parameter
    with the given name has the given value.
    Return True/False or raise an IOError if the file could
    not be read.'''
    try:
        with open(filename, encoding='utf-8') as f_ptr:
            lines = f_ptr.readlines()
    except IOError:
        print(f'File "{filename}" could not be read')
        return False
    # not all parameters may be followed by a unit
    regex_param = re.compile(re.escape(param_name) + r'\((\d+)\)')
    for line in lines:
        match_obj = regex_param.search(line)
        if match_obj:
            if param_value2:
                return (int(match_obj.group(1)) == param_value or
                        int(match_obj.group(1)) == param_value2)
            return int(match_obj.group(1)) == param_value
    print(f'WARNING: Parameter {param_name} not found in {filename}')
    return False


def check_latest_msg_files(df):
    '''Check if the most recent msg file for any float matches
    the criteria for requiring adjustments of CP activation depth.'''
    # get current time in epoch seconds for comparison with mtime
    epoch_time = time.time()
    for serial_no in df['SN']:
        this_row = df[df['SN'] == serial_no]
        if not this_row['mission_cfg_ToD'].values[0]:
            print(f'Mission will not be changed for {serial_no}')
            continue
        latest_msg = get_latest_msg_file(serial_no)
        if not latest_msg:
            print(f'WARNING: No msg file found for {serial_no}!')
        else:
            # check if latest msg file is not too old
            stats = os.stat(latest_msg)
            mtime = stats.st_mtime
            file_age = (epoch_time - mtime) / 86400 # in days
            if file_age > MAX_AGE_MSG:
                print(f'{latest_msg} is {file_age:.1f} days old')
            elif check_mission_param(latest_msg, 'CpActivationP', 2100):
                df.loc[df['SN'] == serial_no,'meets_criteria'] = True

    return df


def change_mission_cfg(df):
    '''Change the mission.cfg to change the Cp activation pressure and 
    note today's date in the "date_change_cp_mission" column
    of the dataframe.'''
    today = datetime.today()
    # Format today's date as MM/DD/YYYY
    formatted_date = today.strftime('%m/%d/%Y')

    for serial_no in df['SN']:
        this_row = df[df['SN'] == serial_no]
        fn_current = BASE_PATH_NAVIS + 'navis' f'{serial_no:04d}' + '/mission.cfg'
        if not os.path.exists(fn_current):
            print(f'WARNING: {fn_current} does not exist!!')
            continue
        if (this_row['mission_cfg_ToD'].all() and this_row['meets_criteria'].all()):
            print(f'Changing mission for {serial_no} with new Cp activation depth!')
            fn_backup = fn_current + '.OLD_CP'
            os.rename(fn_current, fn_backup)
            shutil.copyfile(CHANGE_MISSION_CFG, fn_current)
            df.loc[df['SN'] == serial_no,
                   'date_change_cp_mission'] = formatted_date
    return df


if __name__ == '__main__':
    if not os.path.exists(CHANGE_MISSION_CFG):
        raise FileNotFoundError('modified mission.cfg not found')
    all_float_dirs = get_float_dirs()
    print(f'{len(all_float_dirs)} total floats found')
    sheet = read_spreadsheet()
    sheet = check_mission_cfg(sheet)
    sheet = check_latest_msg_files(sheet)
    sheet = change_mission_cfg(sheet)
    sheet.to_excel(SPREADSHEET, index=False)
