#!/usr/local/anaconda3/bin/python
#!/home/argoserver/anaconda3/bin/python
#
# Author: H. Frenzel, CICOES, UW // NOAA-PMEL
#
# Current version: January 5, 2024
#
# First version:   September 14, 2023

'''
This script reads in a spreadsheet that contains metadata for Argo floats,
typically downloaded from Google Drive.
It also reads in a template metafile. It fills in all available data
from the spreadsheet and creates a new metafile either for one specified
float or all floats from the spreadsheet.
'''

import argparse
import datetime
import math
import os
import re
import pandas as pd

WIDTH_COLUMN1 = 40 # width of left column in output (meta) file
PHY_DIR = 'PHY_files'
# tolerance for lon/lat difference between launch position from
# spreadsheet and start position from phy0 file:
TOL_LON_LAT = 0.1 # in degrees
# tolerance for time difference between launch time from
# spreadsheet and start time from phy0 file:
TOL_TIME = 600 # in seconds
DUMMY_TIME = '99 99 9999 99 99'


def check_sn(table, serial_no):
    '''Check if the float with the specified serial number is present in
    the spreadsheet. Raises a ValueError if not.'''
    matching_float = table[table['serialNumber'] == serial_no]
    if matching_float.empty:
        raise ValueError('Float with specified SN not present in spreadsheet')


def check_wmo(table, wmoid):
    '''Check if the float with the specified WMO ID is present in
    the spreadsheet. Raises a ValueError if not.'''
    matching_float = table[table['WMO'] == wmoid]
    if matching_float.empty:
        raise ValueError('Float with specified WMO not present in spreadsheet')


def create_lookup():
    '''Create a lookup dictionary. The keys are the fields
    as they appear in the left column of the template file,
    the values are the common headers of the spreadsheet.'''
    lookup = {}
    lookup['internal ID number'] = 'AOML'
    lookup['float serial number'] = 'serialNumber'
    lookup['CPU serial number'] = 'serialNumber' # as suggested by Elizabeth
    lookup['conductivity calibration date'] = 'conductivityCalDate' # 'TS cal date'
    lookup['temperature calibration date'] = 'tempCalDate' # 'TS cal date'
    lookup['pressure calibration date'] = 'pressureCalDate'
    lookup['pressure sensor serial number'] = 'pressureSensorSerialNumber'
    lookup['temperature sensor serial number'] = 'CTDSerialNumber'
    lookup['conductivity sensor serial number'] = 'CTDSerialNumber'
    lookup['transmission ID number'] = 'serialNumber'
    lookup['transmission IMEI number'] = 'IMEI'
    lookup['float deployer'] = 'deployer'
    lookup['float deployer address'] = 'deployerAddress'
    lookup['deployment platform'] = 'deploymentPlatform'
    lookup['deployment cruise id'] = 'deploymentCruise'
    lookup['board serial number'] = 'boardSerialNumber'
    lookup['board type'] = 'boardVersion'
    lookup['ROM version'] = 'ROMVersion'
    lookup['PI'] = 'PI'
    lookup['principal investigator address'] = 'PIAddress'
    lookup['WMO ID number'] = 'WMO'
    lookup['ref table PROJECT_NAME'] = 'ProjectName'
    return lookup


def read_template_file(fn_templ):
    '''Read the template file with the given name.
    Return the information as a dictionary (left column
    values are the keys, right column values are the
    values).'''
    template = []
    with open(fn_templ, 'r', encoding='utf-8') as f_templ:
        lines = f_templ.read().splitlines()

    for line in lines:
        lhs = line[0:WIDTH_COLUMN1].rstrip()
        rhs = line[WIDTH_COLUMN1:]
        template.append((lhs, rhs))
    return template


def convert_int_columns(df):
    '''Convert the values in pre-defined columns of a dataframe to int64.
    If there are missing values in a column, this conversion cannot
    be done and a warning is issued.'''
    # convert some columns from float to int
    int_cols = ['serialNumber', 'AOML', 'WMO', 'pressureSensorSerialNumber',
                'CTDSerialNumber', 'IMEI']
    for col in int_cols:
        if any(df.loc[:,col].isna()):
            print(f'\nWARNING: missing values found in column "{col}"!\n')
        else:
            df[col] = df[col].astype('int64')
    return df


def read_phy0_file(fn_phy):
    '''Extract the start time from the first phy file for a given float.
    This function does not check that it is a '_000.phy' file.
    The starting position and time is returned as a dictionary.'''
    if ARGS.verbose:
        print(f'reading {fn_phy}')
    with open(fn_phy, 'r', encoding='utf-8') as f_phy:
        lines = f_phy.read().splitlines()

    regex1 = re.compile(r'LATITUDE  LONGITUDE')
    regex2 = re.compile(r'([\-\+]?[\d\.]+)\s+([\-\+]?[\d\.]+)\s+' +
                        r'(\d{4}/\d\d/\d\d\s+\d\d:\d\d:\d\d)')
    read_pos_time = False

    for line in lines:
        if read_pos_time:
            match_obj = regex2.search(line)
            if match_obj:
                lat = float(match_obj.group(1))
                lon = float(match_obj.group(2))
                date = datetime.datetime.strptime(match_obj.group(3),
                                                  '%Y/%m/%d %H:%M:%S')
                break # this is all we need from this file
            read_pos_time = False
        match_obj = regex1.search(line)
        if match_obj:
            read_pos_time = True
    if not read_pos_time:
        raise ValueError('line with position and date was not found')

    return {'lon': lon, 'lat': lat, 'time': date}


def parse_phy_file(fn_phy0):
    '''Extract and return starting lon/lat/time from the first PHY file.
    Return -999. values if the file does not exist.'''
    if not os.path.exists(fn_phy0):
        # try a descending profile instead
        fn_phy0 = fn_phy0.replace('000.phy', '000D.phy')
    if os.path.exists(fn_phy0):
        return read_phy0_file(fn_phy0)
    print(f'First PHY file ("{fn_phy0}") not found!')
    print('Using "n/a" and "99s" for the start position and time')
    return {'lon': -999., 'lat': -999., 'time': pd.to_datetime('1900-01-01')}


def det_launch_pos(this_row, start):
    '''Determine the launch position and compare it to the start
    position from the first phy file. Return the position formatted
    as string if the two positions match, None otherwise.'''
    lat = this_row['lat'].values[0]
    # if a phy0 file exists, lon/lat values must match between that
    # and the launch position in the spreadsheet
    if start['lat'] > -900 and abs(lat - start['lat']) > TOL_LON_LAT:
        print(f'START LAT - table: {lat} vs phy0: {start["lat"]}')
        return None

    lon = this_row['lon'].values[0]
    if start['lon'] > -900 and abs(lon - start['lon']) > TOL_LON_LAT:
        print(f'START LON - table: {lon} vs phy0: {start["lon"]}')
        return None
    if (pd.isnull(this_row['lon'].values[0]) or
        pd.isnull(this_row['lat'].values[0])):
        print('Launch position cannot be determined!')
        return None
    latd = int(math.trunc(lat))
    latm = (lat - latd) * 60 # minutes, not decimal degress
    lond = int(math.trunc(lon))
    lonm = (lon - lond) * 60 # minutes
    return f'{latd} {latm:.3f} {lond} {lonm:.3f}'


def determine_rhs(line, this_row, lookup, start, fn_out):
    '''Evaluate the given left hand side of the string and return
    the appropriate string for the right hand side of the output string.'''
    unknowns = ['board battery serial number',
                'pump battery serial number']
    drop_lines = ['battery details', 'ref table DEPLOYMENT_PLATFORM_ID']
    int_columns = ['board serial number']
    lhs = line[0]
    if lhs in lookup:
        key = lookup[lhs]
        rhs = this_row[key].values[0]
        if 'calibration date' in lhs:
            cal_date = pd.to_datetime(str(rhs))
            rhs = cal_date.strftime('%d %m %Y')
    elif lhs.startswith('launch position'):
        rhs = det_launch_pos(this_row, start)
        if not rhs:
            print(f'not creating meta file "{fn_out}"!')
            return None
    elif lhs.startswith('start time'):
        if not pd.isnull(this_row['started'].values[0]):
            start_time = pd.to_datetime(this_row['started'].values[0])
            if (start['time'].year > 1990. and
                abs(start_time - start['time']).total_seconds() > TOL_TIME):
                print(f'START TIME - table: {start_time} vs phy0: {start["time"]}')
                print(f'not creating meta file "{fn_out}"!')
                return None
            rhs = start_time.strftime('%d %m %Y %H %M')
        else:
            rhs = DUMMY_TIME
    elif lhs.startswith('launch time'):
        if not pd.isnull(this_row['deployed'].values[0]):
            deploy_time = pd.to_datetime(this_row['deployed'].values[0])
            rhs = deploy_time.strftime('%d %m %Y %H %M')
        else:
            rhs = DUMMY_TIME
    elif lhs == 'status of start time':
        rhs = 'as transmitted'
    elif lhs.startswith('status of launch'): # time and position
        if not pd.isnull(this_row['deployed'].values[0]):
            rhs = 'as recorded'
        else:
            rhs = 'n/a'
    elif lhs in unknowns:
        rhs = 'n/a'
    elif lhs in drop_lines:
        rhs = '__DROP_LINE__' # completely drop this line from the output
    else:
        rhs = line[1]
    if lhs in int_columns:
        rhs = int(rhs) # enforce the output format as integer
    # a special case of formatting
    if lhs == 'transmission ID number':
        rhs = f'{rhs:06d}'
    return rhs


def create_output_files(template, table, lookup):
    '''Create output files from the given template and values
    in the table. If a serial number (first priority) or WMO ID 
    (second priority) were specified as input arguments, an output file
    is created for this float only. Otherwise, output files are created for 
    floats that have SN and WMO defined in the table.'''
    if ARGS.sn > 0:
        floats = table[table['serialNumber'] == ARGS.sn].copy()
    elif ARGS.wmo > 0:
        floats = table[table['WMO'] == ARGS.wmo].copy()
    else:
        floats = table[(table['WMO'] > 1e4) & (table['serialNumber'] > 0)].copy()
    floats = convert_int_columns(floats)
    for ser_no in floats['serialNumber'].values.tolist():
        this_row = floats.loc[table['serialNumber'] == ser_no,:]
        aoml_number = this_row['AOML'].values[0]
        fn_phy0 = f'{PHY_DIR}/{ser_no}/{aoml_number}_{ser_no:06d}_000.phy'
        if ARGS.format.lower() == 'p':
            fn_out = f'MET{ser_no}'
        elif ARGS.format.lower() == 'a':
            fn_out = f'{aoml_number}_{ser_no:06d}.meta'
        else:
            raise ValueError(f'Unknown file name format: {ARGS.format}')
        if ARGS.verbose:
            print(f'Creating metafile {fn_out} for float with S/N {ser_no}')
        delete = 0
        with open(fn_out, 'w', encoding='utf-8') as f_out:
            start = parse_phy_file(fn_phy0)
            for line in template:
                rhs = determine_rhs(line, this_row, lookup, start, fn_out)
                if rhs == '__DROP_LINE__':
                    continue # skip this line and continue
                if not rhs and not isinstance(rhs, str):
                    delete = 1
                    break # return value was None; do not create the output file
                f_out.write(f'{line[0]:<{WIDTH_COLUMN1}}{rhs}\n')
        if delete: # delete a partially written file
            os.remove(fn_out)


def parse_input_args():
    '''Parse the command line arguments and return them as an object.'''
    parser = argparse.ArgumentParser()
    # required arguments:
    parser.add_argument('template', help='name of the template file')
    parser.add_argument('spreadsheet', help='name of the spreadsheet file')
    # options:
    parser.add_argument('-f', '--format', default='p', type=str,
                        help='name format for output, p for PMEL (default) or a for AOML')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='if set, display more progress updates')
    parser.add_argument('-s', '--sn', type=int, default=-999,
                        help='process selected SN id only')
    parser.add_argument('-w', '--wmo', type=int, default=-999,
                        help='process selected WMO id only (ignored if -s is used)')
    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_input_args()
    TABLE = pd.read_excel(ARGS.spreadsheet)
    if ARGS.sn > 0:
        check_sn(TABLE, ARGS.sn)
    elif ARGS.wmo > 0:
        check_wmo(TABLE, ARGS.wmo)
    TEMPLATE = read_template_file(ARGS.template)
    LOOKUP = create_lookup()
    create_output_files(TEMPLATE, TABLE, LOOKUP)
