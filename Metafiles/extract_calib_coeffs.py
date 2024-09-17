#!/usr/bin/env python
#
# Author: H. Frenzel, CICOES, UW // NOAA-PMEL
#
# Current version: January 26, 2024
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

import pdb


PHY_DIR = 'PHY_files'
# tolerance for lon/lat difference between launch position from
# spreadsheet and start position from phy0 file:
TOL_LON_LAT = 0.1 # in degrees
# tolerance for time difference between launch time from
# spreadsheet and start time from phy0 file:
TOL_TIME = 600 # in seconds
DUMMY_TIME = '99 99 9999 99 99'

MONTHS = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec']


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

def create_calib_to_table(calib, table):
    '''Create a dictionary that uses the keys of the CALIB dictionary as keys
    and the column names of the spreadsheet as values.
    Only those columns that have different names between the CALIB dictionary and
    the spreadsheet are listed here. Some columns with calibration coefficients
    (e.g., 'ph_f1') are the same in both.'''
    c2t = {}
    #print(calib.keys())
    c2t['ctd_SERIALNO'] = 'CTDSerialNumber'
    c2t['ctd_TCALDATE'] = 'tempCalDate'
    c2t['ctd_CCALDATE'] = 'conductivityCalDate'
    c2t['ctd_PCALDATE'] = 'pressureCalDate'
    c2t['oxy_INSTRUMENT_TYPE'] = 'oxygenSensorType'
    c2t['oxy_SERIALNO'] = 'oxygenSensorSerialNumber'
    c2t['oxyCalDate'] = 'oxygenCalDate'
    c2t['oxy_SetA0'] = 'oxygen_A0'
    c2t['oxy_SetA1'] = 'oxygen_A1'
    c2t['oxy_SetA2'] = 'oxygen_A2'
    c2t['oxy_SetB0'] = 'oxygen_B0'
    c2t['oxy_SetB1'] = 'oxygen_B1'
    c2t['oxy_SetC0'] = 'oxygen_C0'
    c2t['oxy_SetC1'] = 'oxygen_C1'
    c2t['oxy_SetC2'] = 'oxygen_C2'
    c2t['oxy_SetE'] = 'oxygen_E'
    c2t['oxy_SetTA0'] = 'oxygen_TA0'
    c2t['oxy_SetTA1'] = 'oxygen_TA1'
    c2t['oxy_SetTA2'] = 'oxygen_TA2'
    c2t['oxy_SetTA3'] = 'oxygen_TA3'
    #FIXME oxygenCalDate
    c2t['chla_ser_no'] = 'ecoSensorSerialNumber'
    c2t['CHL_DC'] = 'chl_DarkCount'
    c2t['CHL_Scale'] = 'chl_Scale'
    c2t['BBP700_DC'] = 'bbp700_DarkCount'
    c2t['BBP700_Scale'] = 'bbp700_Scale'
    c2t['CDOM_DC'] = 'cdom_DarkCount'
    c2t['CDOM_Scale'] = 'cdom_Scale'
    c2t['ChlDC'] = 'chl_DarkCount'
    c2t['ChlScale'] = 'chl_Scale'
    c2t['Betab700DC'] = 'bbp700_DarkCount'
    c2t['Betab700Scale'] = 'bbp700_Scale'
    c2t['FDOMDC'] = 'cdom_DarkCount'
    c2t['FDOMScale'] = 'cdom_Scale'
    c2t['ph_k2f0'] = 'ph_k2'
    c2t['ph_date'] = 'phCalDate'
    c2t['ocr_ser_no'] = 'ocrSerialNumber'
    c2t['suna_version'] = 'nitrateSensorVersion'
    c2t['suna_ser_no'] = 'nitrateSensorSerialNumber'
    c2t['ph_ser_no'] = 'phSensorSerialNumber'
    
    return c2t
    
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

"""
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
    if ARGS.type == 'f':
        print(f'First PHY file ("{fn_phy0}") not found!')
        print('Using "n/a" and "99s" for the start position and time')
    return {'lon': -999., 'lat': -999., 'time': pd.to_datetime('1900-01-01')}


def det_launch_pos(this_row, start):
    '''Determine the launch position and compare it to the start
    position from the first phy file (except for pre-deployment files). 
    Return the position formatted as string if the two positions match, 
    None otherwise.'''
    if ARGS.type.lower() == 'p':
        lat = this_row['targetLatitude'].values[0]
        lon = this_row['targetLongitude'].values[0]
        msg = 'Creating pre-deployment metafile with target position and date'
        pos_type = 'Target'
    else:
        lat = this_row['lat'].values[0]
        lon = this_row['lon'].values[0]
        msg = 'Creating final metafile with reported launch position and date'
        pos_type = 'Launch'

    if pd.isnull(lon) or pd.isnull(lon):
        print(f'{pos_type} position cannot be determined!')
        return None
    if ARGS.verbose:
        print(msg)
    # if a phy0 file exists, lon/lat values must match between that
    # and the launch position in the spreadsheet
    if start['lat'] > -900 and abs(lat - start['lat']) > TOL_LON_LAT:
        print(f'START LAT - table: {lat} vs phy0: {start["lat"]}')
        return None
    if start['lon'] > -900 and abs(lon - start['lon']) > TOL_LON_LAT:
        print(f'START LON - table: {lon} vs phy0: {start["lon"]}')
        return None
    latd = int(math.trunc(lat))
    latm = (lat - latd) * 60 # minutes, not decimal degress
    lond = int(math.trunc(lon))
    lonm = (lon - lond) * 60 # minutes
    return f'{latd} {latm:.3f} {lond} {lonm:.3f}'

#def determine_rhs(line, this_row, lookup, start, fn_out):
def determine_rhs(line, this_row, lookup, fn_out):
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
            pdb.set_trace()
            rhs = cal_date.strftime('%d %m %Y')
    elif lhs.startswith('launch position'):
        rhs = 'CHANGE MANUALLY!' #FIXME det_launch_pos(this_row, start)
        if not rhs:
            print(f'not creating meta file "{fn_out}"!')
            return None
    elif lhs.startswith('launch time'):
        if ARGS.type.lower() == 'f' and not pd.isnull(this_row['deployed'].values[0]):
            deploy_time = pd.to_datetime(this_row['deployed'].values[0])
            rhs = deploy_time.strftime('%d %m %Y %H %M')
        elif ARGS.type.lower() == 'p' and not pd.isnull(this_row['targetDate'].values[0]):
            deploy_time = pd.to_datetime(this_row['targetDate'].values[0])
            rhs = deploy_time.strftime('%d %m %Y %H %M')
        else:
            rhs = DUMMY_TIME
    elif lhs.startswith('status of launch'): # time and position # *** HGH
        if ARGS.type.lower() == 'f' and not pd.isnull(this_row['deployed'].values[0]):
            rhs = 'as recorded'
        elif ARGS.type.lower() == 'p' and not pd.isnull(this_row['targetDate'].values[0]):
            rhs = 'as targeted'
        else:
            rhs = 'n/a'
    elif lhs.startswith('start time'):
        if not pd.isnull(this_row['started'].values[0]):
            start_time = pd.to_datetime(this_row['started'].values[0])
            pdb.set_trace()
            # if a phy0 file exists, start time value must match between that
            # and the start time in the spreadsheet
            if (start['time'].year > 1990. and
                abs(start_time - start['time']).total_seconds() > TOL_TIME):
                print(f'START TIME - table: {start_time} vs phy0: {start["time"]}')
                print(f'not creating meta file "{fn_out}"!')
                return None
            rhs = start_time.strftime('%d %m %Y %H %M')
        else:
            rhs = DUMMY_TIME
    elif lhs == 'status of start time':
        rhs = 'as transmitted'
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
    if pd.isnull(rhs):
        print(f'WARNING: {lhs} -> {rhs}')
    return rhs
"""


def get_lines_cal_file(fn_calib):
    '''Reads the file with the given name and returns its contents as
    a list of lines. Raises an IOError if the file could not be read.'''
    if not os.path.exists(fn_calib):
        raise IOError(f'WARNING: {fn_calib} could not be read!')
    with open(fn_calib, encoding='utf-8') as file:
        lines = file.readlines()
    return lines


def read_calibration(fn_calib, calib, sensor_type):
    '''Read the calibration information from the file with the
    specified name. Add information to the "calib" dictionary and return it.'''
    # this file has a simple format with lines like this:
    # SERIALNO=1855
    lines = get_lines_cal_file(fn_calib)
    regex_date = re.compile(r'(\d{2})\-(\w{3})\-(\d{2})')
    for line in lines:
        contents = line.strip().split('=')
        if 'caldate' in contents[0].lower():
            match_obj = regex_date.search(contents[1].strip())
            if match_obj:
                if sensor_type == 'ctd':
                    if contents[0] == 'TCALDATE':
                        var_name = 'tempCalDate'
                    elif contents[0] == 'CCALDATE':
                        var_name = 'conductivityCalDate'
                    elif contents[0] == 'PCALDATE':
                        var_name = 'pressureCalDate'                        
                else:
                    var_name = f'{sensor_type}CalDate'
                month = MONTHS.index(match_obj.group(2)[0:3].lower()) + 1
                # store internally as MM/DD/YYYY, input is DD-MON-YY
                calib[var_name] = f'{month:02}/' + \
                    f'{int(match_obj.group(1)[-2:]):02}' + \
                    f'/{int(match_obj.group(3)) + 2000}'
        else:
            calib[f'{sensor_type}_{contents[0].strip()}'] = contents[1].strip()
    return calib


def read_calibration_eco(fn_calib, calib):
    '''Read the ECO (MCOMS) sensor calibration information from the file with the
    specified name. 
    Add information to the "calib" dictionary and return it.'''
    lines = get_lines_cal_file(fn_calib)
    regex_mcoms = re.compile(r'MCOMS.*\(MCOMS\s+(\d+)\)\s+\[([\w\d]+)\s+' + \
                             r'([\w\d]+)\],(\d+),([\d\.eE\-+]+)')
    for line in lines:
        if line.startswith('ECO'):
            contents = line.strip().split() # could be tabs and/or spaces
            contents2 = contents[1].split('-')
            calib['ecoSensorSerialNumber'] = contents2[1]
        elif 'created on' in line.lower():
            contents = line.strip().split(':')
            contents2 = contents[1].split('/')
            mth = int(contents2[0])
            day = int(contents2[1])
            yr = int(contents2[2])
            if yr < 100:
                yr += 2000 # remember Y2K?
            # store internally as MM/DD/YYYY
            calib['ecoCalDate'] = f'{mth:02}/{day:02}/{yr}'    
        elif ('=' in line and not line.startswith('N/U')
              and not 'columns' in line.lower()):
            contents = line.strip().split('=')
            contents2 = contents[1].split()
            # first element of contents2 is the channel, which is not used
            if contents[0] == 'lambda':
                wavelength = int(contents2[3])
                var_name = f'BBP{wavelength}'
            else:
                var_name = contents[0]
            calib[f'{var_name}_DC'] = round(float(contents2[2]))
            calib[f'{var_name}_Scale'] = float(contents2[1])
        elif line.startswith('MCOMS'):
            match_obj = regex_mcoms.search(line)
            calib['ecoSensorSerialNumber'] = match_obj.group(1)
            calib[match_obj.group(2)] = int(match_obj.group(4))
            calib[match_obj.group(3)] = float(match_obj.group(5))            
    return calib


def read_calibration_ocr(fn_calib, calib):
    '''Read the OCR sensor calibration information from the file with the
    specified name. 
    Add information to the "calib" dictionary and return it.'''
    lines = get_lines_cal_file(fn_calib)
    regex_date = re.compile(r'#\s*(\d{4})\-(\d{2})\-(\d{2})\s+')
    for idx, line in enumerate(lines):
        if line.startswith('#') or not line.strip():
            match_obj = regex_date.search(line)
            if match_obj:
                # input format is YYYY-MM-DD
                # store internally as MM/DD/YYYY
                calib['ocrCalDate'] = f'{match_obj.group(2)}/' + \
                     f'{match_obj.group(3)}/' + f'{match_obj.group(1)}' 
            else:
                continue # skip comment and empty lines
        contents = line.split()
        if line.startswith('SN '):
            calib['ocr_ser_no'] = contents[1]
        elif line.startswith('ED '):
            wavelength = round(float(contents[1]))
            # WARNING this is a hack! For 4005, reported wavelength
            # in calibration file is 489.23, but it should be 490
            if wavelength == 489:
                wavelength = 490
                print(f'Wavelength adjusted to {wavelength}')
            next_contents = lines[idx+1].split()
            var_name = f'irrad{wavelength}' # base name
            calib[f'{var_name}_a0'] = next_contents[0]
            calib[f'{var_name}_a1'] = next_contents[1]
            calib[f'{var_name}_im'] = next_contents[2]
        elif line.startswith('PAR '):
            next_contents = lines[idx+1].split()
            calib[f'irradPAR_a0'] = next_contents[0]
            calib[f'irradPAR_a1'] = next_contents[1]
            calib[f'irradPAR_im'] = next_contents[2]
    return calib


def read_calibration_suna(fn_calib, calib):
    '''Read the SUNA sensor calibration information from the file with the
    specified name. (Only sensor model and serial number as well
    as the calibration date are extracted from this file.)
    Add information to the "calib" dictionary and return it.'''
    lines = get_lines_cal_file(fn_calib)
    # this is what the "Date" line looks like:
    # /* Date: Tue Dec 27 16:26:12 PST 2022     */
    regex_sn = re.compile(r'SUNA\s+([\w\d]+)\s+#?(\d+)')
    regex_date = re.compile(r'(\w{3}\s+\d{2}).*(\d{4})\s+')
    for line in lines:
        if 'SUNA' in line:
            match_obj = regex_sn.search(line)
            calib['suna_version'] = match_obj.group(1)
            calib['suna_ser_no'] = match_obj.group(2)
        elif 'Date' in line:
            match_obj = regex_date.search(line)
            month = MONTHS.index(match_obj.group(1)[0:3].lower()) + 1
            # store internally as MM/DD/YYYY
            calib['nitrateCalDate'] = (f'{month:02}/' +
                                      f'{int(match_obj.group(1)[-2:]):02}' +
                                      f'/{match_obj.group(2)}')
    return calib


def read_calibration_ph(fn_calib, calib):
    '''Read the pH sensor calibration information from the file with the
    specified name.
    Add information to the "calib" dictionary and return it.'''
    lines = get_lines_cal_file(fn_calib)
    last_line = ''
    for line in lines:
        # skip comment and empty lines
        if line.startswith('#') or len(line.strip()) == 0:
            continue
        # there are different formats of this file, either:
        # k0: -1.31063    or
        # K0 = -1.495962
        if ':' in line:
            contents = line.split(':')
        else:
            contents = line.split('=')
        # some calibration files wrap e.g. serial numbers in double quotes
        if len(contents) > 1:
            contents[1] = contents[1].replace('"','')    
        # in some files, this entry is called 'calibration_date',
        # but in others just 'date'
        if 'date' in contents[0]:
            # store internally as MM/DD/YYYY;
            # input is either DD MM YYYY or YYYY-MM-DD
            if '-' in contents[1]:
                # assuming it's YYYY-MM-DD
                contents2 = contents[1].split('-')
                calib['phCalDate'] = f'{contents2[1].strip():02}/' + \
                    f'{contents2[2].strip():02}/{contents2[0].strip()}'
            else:    
                contents2 = contents[1].split()
                calib['phCalDate'] = f'{contents2[1]:02}/{contents2[0]:02}/' + \
                    contents2[2]
        elif 'poly_order' in line:
            if 'fp' in last_line.lower():
                calib['ph_fp_poly_order' ] = contents[1].strip()
            elif 'k2p' in last_line.lower():
                calib['ph_k2p_poly_order' ] = contents[1].strip()
        elif len(contents) > 1:
            calib[f'ph_{contents[0].lower().strip()}'] = contents[1].strip()
        last_line = line # needed for poly_order
    if ('ph_serial_number' in calib.keys() and
        'ph_isfet_serial_number' in calib.keys()):
        calib['phSensorSerialNumber'] = calib.pop('ph_serial_number') + '-' + \
            calib.pop('ph_isfet_serial_number')
    return calib

def fill_spreadsheet(calib, table, calib_to_table):
    '''Create an output file from the given template and values
    in the table. A serial number (first priority) or WMO ID 
    (second priority) must have been specified as input arguments.'''
    #table = convert_int_columns(table)
    if ARGS.sn > 0:
        idx = table.index[table['serialNumber'] == ARGS.sn].values[0]
    elif ARGS.wmo > 0:
        idx = table.index[table['WMO'] == ARGS.wmo].values[0]
    if not idx:
        raise ValueError('No matching S/N or WMO found')
    ser_no = table.loc[idx, 'serialNumber']
    if ARGS.verbose:
        print(f'Adding information to {ARGS.spreadsheet} for float with S/N {ser_no}')
    aoml_number = table.loc[idx, 'AOML']
    #FIXME fn_phy0 = f'{PHY_DIR}/{ser_no}/{aoml_number}_{ser_no:06d}_000.phy'
    for ckey, value in calib.items():
        # some keys have different names in the calibration files and in the spreadsheet
        if ckey in calib_to_table:
            ckey = calib_to_table[ckey]
        # other keys are the same, they can be used as is
        if ckey not in table.columns:
            print(f'Not in table: {ckey}')
            continue
        if not pd.isna(table.loc[idx, ckey]) and value != table.loc[idx, ckey]:
            is_diff = True
            try:
                float_value = float(value)
                if abs(float_value - table.loc[idx, ckey]) < 1e-6*abs(float_value):
                    is_diff = False
            except:
                pass # nothing to change
            if is_diff:
                print(f'Mismatching values for "{ckey}":')
                print(f'OLD VALUE: {table.loc[idx, ckey]}, NEW: {value}')
                if ARGS.confirm:
                    answer = input('Do you want to change it (y/n)? ')
                    if not answer.lower().startswith('y'):
                        continue
                print('WARNING, overwriting value!')
                try:
                    value = float(value)
                except:
                    pass # keep it as a string
                table.loc[idx, ckey] = value
            else:
                print(f'NOT CHANGING: {ckey} value is {value}')
        else:
            table.loc[idx, ckey] = value
    table.to_excel(ARGS.spreadsheet, index=False)
    

def parse_input_args():
    '''Parse the command line arguments and return them as an object.'''
    parser = argparse.ArgumentParser()
    # required arguments:
    #parser.add_argument('template', help='name of the template file')
    parser.add_argument('spreadsheet', help='name of the spreadsheet file')
    # calibration file CTD
    parser.add_argument('calibration_ctd', help='name of the CTD calibration file')
    # calibration file OXY
    parser.add_argument('calibration_oxy', help='name of the OXY calibration file')
    # calibration file ECO
    parser.add_argument('calibration_eco', help='name of the ECO calibration file')
    # calibration file SUNA
    parser.add_argument('calibration_suna', help='name of the SUNA calibration file')
    # calibration file pH
    parser.add_argument('calibration_ph', help='name of the pH calibration file')
    # calibration file OCR
    parser.add_argument('calibration_ocr', nargs='?',
                        help='name of the OCR calibration file')
    # options:
    parser.add_argument('-c', '--confirm', default=False, action='store_true',
                        help='if set, ask for confirmation before overwriting existing values')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='if set, display more progress updates')
    parser.add_argument('-s', '--sn', type=int, default=-999,
                        help='process float with selected serial number')
    parser.add_argument('-w', '--wmo', type=int, default=-999,
                        help='process float with selected WMO id (ignored if -s is used)')
    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_input_args()
    TABLE = pd.read_excel(ARGS.spreadsheet)
    if ARGS.sn > 0:
        check_sn(TABLE, ARGS.sn)
    elif ARGS.wmo > 0:
        check_wmo(TABLE, ARGS.wmo)
    else:
        raise ValueError('You must specify either serial or WMO number!')
    # TEMPLATE = read_template_file(ARGS.template)
    CALIB = read_calibration(ARGS.calibration_ctd, {}, 'ctd')
    CALIB = read_calibration(ARGS.calibration_oxy, CALIB, 'oxy')
    CALIB = read_calibration_eco(ARGS.calibration_eco, CALIB)
    CALIB = read_calibration_suna(ARGS.calibration_suna, CALIB)
    CALIB = read_calibration_ph(ARGS.calibration_ph, CALIB)
    if ARGS.calibration_ocr:
        CALIB = read_calibration_ocr(ARGS.calibration_ocr, CALIB)
    calib_to_table = create_calib_to_table(CALIB, TABLE)
    fill_spreadsheet(CALIB, TABLE, calib_to_table)
