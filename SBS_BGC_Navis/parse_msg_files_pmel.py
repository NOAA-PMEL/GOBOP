#!/home/argoserver/frenzel/anaconda3/bin/python
#
# This script parses at least one msg file from an Argo float (core or BGC)
# for its engineering data and creates one netcdf output file per float
# (e.g., for use with ERDDAP).
#
# H. Frenzel, CICOES, UW // NOAA-PMEL
#
# Current version: October 24, 2023
#
# First version: April 27, 2021

import argparse
import datetime
import hashlib
import os
import re
import netCDF4 as nc
import numpy as np
import pandas as pd

import pdb

# currently only used for date conversion for comparison with Matlab:
from datetime import datetime as dt


def get_float_ids(filename):
    '''Read float information from the given csv file, extract the
    internal (serial number) and external (WMO) IDs from the appropriate columns
    and return a dictionary with the internal IDs as keys and the WMO IDs
    as the values.'''
    float_info = pd.read_csv(filename)
    internal_ids = float_info['Float ID'].values
    wmo_ids = float_info['Float WMO'].values
    return dict(map(lambda i,j: (i,j), internal_ids, wmo_ids))


def parse_filename(filename):
    '''This function splits the given filename, which may include the
    full path, into path and filename, and further splits the filename into
    floatid, profile, and file type ('msg' etc.).
    Returns file name (without the path), floatid (as int), profile (as int),
    and file type.'''
    fname = os.path.basename(filename) # without the path
    parts = fname.split('.')
    floatid = int(parts[0])
    profile = int(parts[1])
    ftype = parts[2]
    return fname, floatid, profile, ftype


def check_conv_need_file(filename, filename_log):
    '''This function checks if the file with the given name
    has been processed before.
    If so, it checks if the file is identical to the previously
    processed file. Returns True in that case, False otherwise.'''
    log = pd.read_csv(filename_log)
    # look for an exact match including the path and file name
    this_row = log.loc[log['Filename'] == filename]
    if this_row.size:
        # always use the most recently processed version of this file
        # as a comparison
        if this_row['Checksum'].values[-1] == get_checksum(filename):
            # file is identical to previously processed file
            if ARGS.verbose:
                print(f'unchanged: {filename}')
            return False
    else:
        # no exact match (path and filename) was found
        fname, floatid, profile, ftype = parse_filename(filename)
        this_row = log.loc[(log['FloatID'] == floatid) &
                           (log['Profile'] == profile) &
                           (log['Type'] == ftype)]
        if not this_row.size: # no matching lines found
            return True
        # file path is different; check if file contents are identical
        if this_row['Checksum'].values[-1] == get_checksum(filename):
            if ARGS.verbose:
                print(f'{filename} is identical to ' +
                      f'{this_row["Filename"].values[-1]}')
            return False
    return True


def check_conv_need(filename, all_file_types, filename_log):
    '''This function checks if the file with the given name or any of
    the other raw files for this float and profile have been processed before.
    If so, it checks if the files are identical to the previously used ones.
    It returns True if at least one file is new or changed, False otherwise.'''
    ftype = parse_filename(filename)[3]
    for type in all_file_types:
        this_file = filename.replace(ftype, type)
        if (os.path.exists(this_file) and
            check_conv_need_file(this_file, filename_log)):
            return True
    return False


def get_filename_out(filename_in, output_dir, file_type):
    '''Determine and return the corresponding name of the netcdf output file for
    the given name and file_type ('eng' or 'sci') of the input file. Issue a
    warning message and return None if the input filename does not conform to
    the expected naming standard.'''
    regex = re.compile(r'(\d+)\.\d+.msg')
    match_obj = regex.search(filename_in)
    if match_obj:
        return f'{output_dir}/{file_type}_{match_obj.group(1)}.nc'
    else:
        print('File "{0:s}" has an unexpected name'.format(filename_in))
        return None


def get_fwrev(line):
    '''Extract the firmware name and revision number from the given line and
    return them as strings.'''
    regex = re.compile(r'([AN]pf\d*i?).*FwRev[\s=]((ARGO )?\d+)')
    match_obj = regex.search(line)
    if match_obj:
        fw = match_obj.group(1)
        rev = match_obj.group(2)
        if 'FwRev' not in fw:
            fw += 'FwRev'        
        return fw, rev
    else:
        # for Navis BGC floats
        regex_n = re.compile(r'(Npf.*)\(.*FwRev\s*\w+\s*(\d+)')
        match_obj = regex_n.search(line)
        if match_obj:
            fw = match_obj.group(1)
            rev = match_obj.group(2)
            if 'FwRev' not in fw:
                fw += 'FwRev'        
                return fw, rev
        else: # alternate scheme for Navis BGC floats
            # e.g.: NpfFwRev=BGCi_SUNA_PH_ICE 170607
            regex_n2 = re.compile(r'NpfFwRev=(.*)\s+(\d+)')
            match_obj = regex_n2.search(line)
            if match_obj:
                fw = match_obj.group(1)
                rev = match_obj.group(2)
                return fw, rev
            else:
                print('FwRev not found in line:')
                print(line)
                return None, None


def get_time_string(days_since_1950):
    '''Convert the given days since 1/1/1950 to a string formatted
    as, e.g., "04/28/2014 185130" and return it.'''
    datetime_obj = (datetime.datetime(1950, 1, 1, 0, 0, 0) +
                            datetime.timedelta(days=days_since_1950))
    return datetime.datetime.strftime(datetime_obj, '%m/%d/%Y %H%M%S')


def get_profile_id(filename):
    '''Extract the profile ID from the middle part of the given filename
    and return it as an integer. Raise a ValueException if the filename
    doesn't match the expected format of <floatid>.<profileid>.<filetype>.'''
    regex = re.compile(r'(\d+)\.(\d+)\.(\w+)')
    match_obj = regex.search(filename)
    if match_obj:
        return int(match_obj.group(2))
    else:
        raise ValueError('unexpected filename: {0:s}'.format(filename))


def check_airsystem(vars):
    '''Check if air system variables exist in dictionary vars. If not, 
    create them with a value of np.nan.'''
    # create a dictionary where the values are the units
    airsys_vars = {'AirSystemBarometer': '', 'AirSystemBarometerVal': 'inHg',
                   'AirSystemBattery': '', 'AirSystemBatteryVal': 'V',
                   'AirSystemCurrent': '', 'AirSystemCurrentVal': 'mA'}
    for var in airsys_vars:
        if var not in vars:
            vars[var] = (np.nan, airsys_vars[var])


def assign_time(vars, coords):
    '''Assign the 'time' entry in the coords dictionary from a time in
    the vars dictionary.'''
    if 'time' in coords:
        print('already  has time in coords')
        pdb.set_trace()
    elif 'Profile_time' in coords:
        #vars['MessageTime'] = (get_time_string(coords['Profile_time']),
        #                       'GMT')
        coords['time'] = coords['Profile_time']
    elif 'Fix_time' in coords:
        #DEBUG pdb.set_trace()
        coords['time'] = coords['Fix_time']
    elif 'MessageTime' in vars:
        coords['time'] = get_seconds_since_1970(vars['MessageTime'][0])
    else:
        # this can happen when a msg file is highly incomplete,
        # cut off before the "profile .. terminated" line
        print('Warning: no time available')
        coords['time'] = np.nan

        
def parse_airsystem_line(line, vars):
    '''Parse the line that contains the AirSystem information.
    Return True or False, depending on whether the line could be parsed
    correctly.'''
    # perform the parsing in two steps, similar to Willa's web pages
    regex = re.compile(r'AirSystem\(\)\s+Battery\s*(\[.+,\s*[\d\.]+V\])\s*' +
                       'Current\s*(\[.+,\s*[\d\.]+mA\])\s*' +
                       'Barometer\s*(\[.+,\s*[\d\.]+"Hg\])')
    match_obj = regex.search(line)
    if match_obj:
        vars['AirSystemBattery'] = (match_obj.group(1), '')
        vars['AirSystemCurrent'] = (match_obj.group(2), '')
        vars['AirSystemBarometer'] = (match_obj.group(3), '')
        # second step of parsing: discard the first part with the cnt,
        # extract the value from the second part
        regex1 = re.compile(r',\s*([\d\.]+)([a-zA-Z"]+)\]')
        match_obj1 = regex1.search(match_obj.group(1))
        if match_obj1:
            vars['AirSystemBatteryVal'] = (match_obj1.group(1),
                                           match_obj1.group(2))
        match_obj2 = regex1.search(match_obj.group(2))
        if match_obj2:
            vars['AirSystemCurrentVal'] = (match_obj2.group(1),
                                           match_obj2.group(2))
        match_obj3 = regex1.search(match_obj.group(3))
        if match_obj3:
            vars['AirSystemBarometerVal'] = (match_obj3.group(1),
                                           'inHg') # matches Willa's pages
        return True    
    else:
        # some lines contain the string "AirSystem" without the variables
        # the actual information could be in the next line
        return False


def parse_airsystem_line_alt(line, vars, fp):
    '''Parsing the line that contains "AirSystem" using a different format.
    If that is used, parse the next two lines as well, using the given
    file pointer fp. Add entries to the vars dictionary.'''
    if 'Battery' in line:
        regex = re.compile(r'Battery Min/Avg/Max\s*(\[.+,\s*[\d\./]+V\])')
        match_obj = regex.search(line)
        if match_obj:
            vars['AirSystemBattery'] = (match_obj.group(1), '')
        else:
            return False
        # second step of parsing: discard the first part with the cnt,
        # extract the value from the second part
        # (do it step by step in case that only the Battery line is good)
        # there are three values (min/avg/max)?, take the middle one
        regex1 = re.compile(r',\s*[\d\.]+/([\d\.]+)/[\d\.]+\s*([a-zA-Z]+)\]')
        match_obj1 = regex1.search(match_obj.group(1))
        if match_obj1:
            vars['AirSystemBatteryVal'] = (match_obj1.group(1),
                                           match_obj1.group(2))
        # else: no new entry in vars, but keep going
        line = fp.readline()
        regex = re.compile(r'Current Min/Avg/Max\s*(\[.+,\s*[\d\./]+\s*mA\])')
        match_obj = regex.search(line)
        if match_obj:
            vars['AirSystemCurrent'] = [match_obj.group(1), '']
        else:
            return True # because one good line was found
        match_obj1 = regex1.search(match_obj.group(1))
        if match_obj1:
            vars['AirSystemCurrentVal'] = (match_obj1.group(1),
                                           match_obj1.group(2))
            # FIXME: this was left out for compatibility with Willa's output
            vars['AirSystemCurrent'][0] = \
                vars['AirSystemCurrent'][0].replace(' mA]', '')
        # else: no new entry in vars, but keep going
        line = fp.readline()
        regex = re.compile(r'Barometer\s*(\[.+,\s*[\-\d\./]+"Hg\])')
        match_obj = regex.search(line)
        if match_obj:
            vars['AirSystemBarometer'] = (match_obj.group(1), '')
        else:
            return True # because two good lines were found
        regex2 = re.compile(r'.*,\s*([\-\d\.]+)"Hg')
        match_obj1 = regex2.search(match_obj.group(1))
        if match_obj1:
            vars['AirSystemBarometerVal'] = (match_obj1.group(1),
                                             'inHg') # matches Willa's pages
        return True
    else:
        return False


def parse_log_line(line, vars):
    '''Parse the given line from a log file. If a variable can be extracted,
    add it to the vars dictionary.'''
    if 'Mission configuration' in line:
        for fw in firmware:
            if fw in vars:
                break
        else:    
            print(vars.keys())
            print('missing config found in log file:')
            print(line)
            pdb.set_trace()
    else:
        regex = re.compile(r'LogConfiguration\(\)\s*([\w]+)\((.+)\)(?:\s+\[([\w\-/]+)\])?')
        match_obj = regex.search(line)
        if match_obj:
            if (match_obj.group(1) not in vars or
                vars[match_obj.group(1)][0] == 'null'):
                # do not write the following variables to output
                # FIXME most of these are in here only for comparison with Willa's pages
                # do not expose security-related information to public output files,
                # so add 'Pwd' and 'User'
                #LEFTOVER WILLA COMPARISON skip_vars = ['DeepProfileBuoyancyPos',
                #             'DeepProfilePistonPos', 'ParkBuoyancyPos', 'ParkPistonPos',
                #             'CompensatorHyperRetraction', 'ConnectTimeOut',
                #             'HpvEmfK', 'HpvRes',
                #             'PActivationPistonPosition', 'TimeOfDay',
                #             'Pwd', 'User']
                if match_obj.group(1) in skip_vars:
                    return
                elif match_obj.group(1) == 'ParkPressure':
                    # ParkPressure is derived from the "Park Sample" line
                    vars['ParkPressure0'] = (match_obj.group(2),match_obj.group(3))
                elif match_obj.group(3):
                    vars[match_obj.group(1)] = (match_obj.group(2),
                                                match_obj.group(3))
                else:
                    vars[match_obj.group(1)] = (match_obj.group(2), '')


def parse_log_file(filename, vars, coords):
    '''Parse the log file that corresponds to the msg file with the given
    filename. Add entries to the dictionaries vars and coords.
    Returns -1 if file wasn't found, 1 if <EOT> was found in input file,
    0 if <EOT> was not found.'''
    success = 0
    vars['logEOT'] = ('0', '') # change if <EOT> is found
    if not os.path.exists(fn_log):
        print('file not found: {0:s}'.format(fn_log))
        return -1
    elif ARGS.verbose:
        print(fn_log)
    if '.000.log' in fn_log:
        zero_log = True
    else:
        zero_log = False
    airsystem_found = False
    fp = open(fn_log, errors='replace')
    line = fp.readline()
    while line:
        if not airsystem_found and 'AirSystem' in line:
            airsystem_found = parse_airsystem_line(line, vars)
            if not airsystem_found:
                airsystem_found = parse_airsystem_line_alt(line, vars, fp)
        elif ('lon' not in coords and 'GpsServices' in line and
              'fix obtained' in line):
            # first determine if it is from the current profile
            # (log files typically contain information from the previous
            # profile as well)
            regex = re.compile(r'Profile\s+(\d+)\s+GPS fix')
            match_obj = regex.search(line)
            if match_obj:
                prof_nr = int(match_obj.group(1))
                if 'ProfileId' in vars:
                    prof_current = int(vars['ProfileId'][0])
                else:
                    prof_current = get_profile_id(filename)
                if prof_nr == prof_current:
                    print(prof_nr)
                    print(vars['ProfileId'])
                    # FIXME read two lines
                    regex = re.compile(r'Fix:\s+([\d\.\-]+)\s+([\d\.\-]+)\s+(\d+/\d+/\d+\s+\d+)')
                    match_obj = regex.search(line)
                    if match_obj:
                        print('gps from log; unexpected case')
                        pdb.set_trace()
                        coords['lon'] = float(match_obj.group(1))
                        coords['lat'] = float(match_obj.group(2))
                        coords['Log_Fix_time'] = get_days_since_1950(match_obj.group(3))
                        vars['Log_GPS_Time'] = (match_obj.group(3), 'GMT')
                else:
                    # this is the expected behavior - in all known cases, a log file's GPS
                    # fix describes the previous profile's coordinates
                    print('Cannot determine GPS fix time from log file')
                    print(f'Log file describes GPS fix for profile {prof_nr}')
                    print(f'Current profile index is {prof_current}')
                    
        elif 'ProfileInit' in line:
            regex = re.compile(r'Pressure:([\d\.]+)dbar')
            match_obj = regex.search(line)
            if match_obj:
                vars['DeepProfilePressure_actual'] = (match_obj.group(1), 'dbar')
        elif zero_log and 'LogConfiguration' in line:
                parse_log_line(line, vars)
        elif '<EOT>' in line:
            vars['logEOT'] = ('1', '')
            success = 1
        line = fp.readline()
    fp.close()
    return success


def parse_msg_file(filename):
    '''Parse the given msg file and return a dictionary with 
    engineering variables as the keys and its values and units as
    the values. Also return a dictionary with time, lon, lat values
    and another one with the high-res p/T/S data. Empty dictionaries are
    returned if the file doesn't exist or if its size is 0.'''
    vars = dict()
    coords = dict()
    if not os.path.exists(filename) or not os.path.getsize(filename):
        return vars, coords, None, None, None
    # set defaults first
    # only changed if IsusInit or DuraInit is found in file, or if firmware
    # identifier contains 'BGC' or 'SUNA'
    vars['Program'] = ('Core', '')
    vars['Float_type'] = ('Unknown', '')
    vars['msgEOT'] = ('0','') # change if found
    vars['Firmware'] = ('Unknown', '')
    fp = open(filename)
    # the line with "GPS fix obtained" comes first in these files from
    # the core program, but not the BGC program
    if '000.msg' in filename:
        # at this point we don't know yet if this is a core float or
        # BGC float, which has a different order of segments
        line = fp.readline()
        while line == '\n': # skip past empty lines that may be there
            line = fp.readline()
        fp.seek(0) # reset to beginning of file FIXME is this always correct here?
        if not 'Mission configuration' in line:
            # this must be a core (APEX or Navis) float or Navis BGC float
            parse_msg_gps_fix(fp, vars, coords) # do not use return value
            # if the GPS fix failed, parse_msg_gps_fix returns False,
            # but the footer needs to be parsed anyway
            parse_msg_footer(fp, vars, coords)
            fp.close()
            if 'NpfFwRev' in vars and 'bgc' in vars['NpfFwRev'][0].lower():
                vars['Program'] = ('BGC', '')
            return vars, coords, None, None, None
    # the header is defined here as all lines with engineering variables
    # that start with a '$'; it ends with a line that only has a '$'
    parse_msg_header(fp, vars)
    # the middle part includes low-res discrete data, park data, high-res data,
    # and optode air calibration values (not all float types have all of these)
    if '000.msg' not in filename:
        discrete, pts, park, opt = parse_msg_middle(fp, vars, coords)
    else:
        discrete, pts, park, opt = None, None, None, None
        
    parse_msg_gps_fix(fp, vars, coords) # see comment above regarding return value
    parse_msg_footer(fp, vars, coords)
    fp.close()
    #DEBUG print(vars['Program'])
    #pdb.set_trace()
    return vars, coords, pts, discrete, park


def parse_msg_gps_fix(fp, vars, coords):
    '''Extract lon/lat/time information from the file with the given pointer fp.
    Dictionary coords is modified by adding the extracted information.
    Return True/False whether the information was found.'''
    gps_found = False # default until good fix was found
    last_pos = fp.tell()
    line = fp.readline()
    if line == '': # EOF was reached
        return gps_found
    # skip empty lines and those with failed GPS fix attempts
    regex_empty = re.compile(r'^\s*\n')
    regex_fail = re.compile(r'# Attempt to get GPS fix failed')
    match_obj_empty = regex_empty.search(line)
    match_obj_fail = regex_fail.search(line)
    while match_obj_empty or match_obj_fail:
        last_pos = fp.tell()
        line = fp.readline()
        match_obj_empty = regex_empty.search(line)
        match_obj_fail = regex_fail.search(line)

    if 'GPS fix obtained' in line:
        line = fp.readline() # only contains header line
        line = fp.readline() # this is the line of interest
        if line:
            regex = re.compile(r'^Fix:\s+([\d\.\-]+)\s+([\d\.\-]+)\s+(\d+/\d+/\d+\s+\d+)')
            match_obj = regex.search(line)
            if match_obj:
                coords['lon'] = float(match_obj.group(1))
                coords['lat'] = float(match_obj.group(2))
                coords['Fix_time'] = get_seconds_since_1970(match_obj.group(3))
                if 'MessageTime' in vars and vars['MessageTime'][0]:
                    print('already have MessageTime in vars:')
                    print(vars['MessageTime'])
                else:
                    vars['MessageTime'] = (match_obj.group(3), 'GMT')
                #FIXME reset to beginning of first line after IridiumGeo/Fix
                #FIXME fp.seek(last_pos)
                gps_found = True
    else:
        coords['lon'] = np.nan
        coords['lat'] = np.nan
        coords['Fix_time'] = np.nan
        if 'MessageTime' in vars and len(vars['MessageTime'][0]):
            print('MessageTime exists in vars already')
            pdb.set_trace()
        else:
            vars['MessageTime'] = ('', '')
        fp.seek(last_pos)
    
    # BGC msg files have additional lines whose information
    # is currently not used
    last_pos = fp.tell()
    line = fp.readline()
    while line and line.startswith('Iridium'):
        last_pos = fp.tell()
        line = fp.readline()
        # FIXME should I store this info??
    fp.seek(last_pos) # go back to before the last line read in
    return gps_found


def parse_msg_header(fp, vars):
    '''Parse the header of the msg file with file pointer fp and fill
    dictionary vars with all successfully parsed variable names and their
    values and units.
    A line that contains only a dollar sign signals the end of the header.'''
    # FIXME do I need to return eof True/False?
    last_pos = fp.tell()
    line = fp.readline()
    while line == '\n': # some files have empty lines at this place
        last_pos = fp.tell()
        line = fp.readline()
    # some files start with park points without any engineering data first
    if line.startswith('ParkPt'):
        # reset to previous position and return without reading any more lines
        fp.seek(last_pos)
        return
    
    regex1 = re.compile(r'\$\s+([\w]+)\((.+)\)\s+\[([\w\-/]+)\]') # with units
    regex2 = re.compile(r'\$\s+([\w]+)\((.+)\)') # no units
    # do not write the following variables to output
    # FIXME these are in here only for comparison with Willa's pages!!!
    #WILLA_COMP skip_vars = ['DeepProfileBuoyancyPos',
    #             'DeepProfilePistonPos', 'ParkBuoyancyPos', 'ParkPistonPos',
    #             'CompensatorHyperRetraction', 'ConnectTimeOut',
    #             'HpvEmfK', 'HpvRes',
    #             'PActivationPistonPosition', 'TimeOfDay']
    # A line with only a '$' at its beginning marks the end of the header
    while line.strip() != '$' and line and '<EOT>' not in line:
        if 'IsusInit' in line or 'DuraInit' in line:
            vars['Program'] = ('BGC', '')
        match_obj = regex1.search(line)
        if match_obj:
            # some variables need to be treated differently
            # FIXME this is a very kludgy setup to mimic Willa's output -
            # it should be completely revised before deployment
            # this variable should be named something like
            # "ParkPressure_target", and the value from the Park Sample
            # should be "ParkPressure_actual" or so
            if match_obj.group(1) == 'ParkPressure':
                # ParkPressure is derived from the "Park Sample" line
                vars['ParkPressure0'] = (match_obj.group(2), match_obj.group(3))
            elif match_obj.group(1) in skip_vars:
                pass # don't add them to the vars dictionary
            else:
                vars[match_obj.group(1)] = (match_obj.group(2),
                                            match_obj.group(3))
        else:
            match_obj = regex2.search(line)
            if match_obj:
                vars[match_obj.group(1)] = (match_obj.group(2), '')
            elif 'FwRev' in line:
                #DEBUG pdb.set_trace()
                if 'Apf' in line:
                    vars['Float_type'] = ('APEX', '')
                elif 'Npf' in line:
                    vars['Float_type'] = ('Navis', '')
                fw, rev = get_fwrev(line)
                if fw:
                    vars['Firmware'] = (fw, '')
                    vars[fw] = (rev, '')
            else: # FIXME should be written to an error log file
                print(line)
                print('NO MATCH (header)') # DEBUG
                #DEBUG
                pdb.set_trace()
        line = fp.readline()

# for comparison with Matlab datetime only!
# https://newbedev.com/equivalent-function-of-datenum-datestring-of-matlab-in-python
def datenum(d):
    return 366 + d.toordinal() + \
        (d - dt.fromordinal(d.toordinal())).total_seconds()/(24*60*60)

def parse_park_points(fp, park_str, program):
    '''Parse the section of an msg file that contains lines starting
    with the given string park_str ('ParkPtFlbb' for APEX BGC,
    ParkObs for Navis BGC or 'ParkPts' for core).
    File pointer fp is set back to the beginning of the first line after the
    park points, presumably the "Profile ... terminated" line.
    Return True/False whether all park points lines could be read. (If there
    are no lines with park points, True is returned.)'''
    last_pos = fp.tell()
    line = fp.readline() # for Navis floats, this line contains variable names
    park = dict()
    if 'Date' in line:
        # this is a Navis BGC msg file
        park['var_names'] = line[1:].split() # exclude '$' in first column
        line = fp.readline() # first actual data line
    else:
        #print('case not yet handled')
        if program == 'Core':
            park['var_names'] = ['Date', 'days_since_1950', 'count', 'p', 't',
                                 's']
        else: # BGC
            park['var_names'] = ['Date', 'days_since_1950', 'count', 'p', 't',
                                 'FSig', 'BbSig', 'TSig']
        #pdb.set_trace()
    for var in park['var_names']:
        park[var] = list()
    park['incomplete'] = False # default assumption    
    while line and park_str in line:
        last_pos = fp.tell()
        all_fields = line.split()
        # the first field should be "park_str" with a colon
        if park_str in all_fields[0]:
            first_col = 1
        else:
            first_col = 0
            print('Warning: unexpected format in line with park obs.')
            pdb.set_trace()
        ncols = len(all_fields)
        if ncols > first_col + 3:
            date_str = ' '.join(all_fields[first_col:first_col+4])
            date_val = get_days_since_1950(date_str)
            # FIXME for comparison with Matlab datetime only!
            # https://newbedev.com/equivalent-function-of-datenum-datestring-of-matlab-in-python
            d = dt.strptime(date_str, '%b %d %Y %H:%M:%S')
            dn = datenum(d)
            park['Date'].append(dn) # FIXME (date_val)
        else:
            park['incomplete'] = True
            break
        for i in range(first_col+4, len(all_fields)):
            #DEBUG print(i)
            #DEBUG print(all_fields[i])
            park[park['var_names'][i-first_col-3]].append(float(all_fields[i]))
        if len(all_fields) < len(park['var_names']) + first_col + 3:
            for i in range(len(all_fields)-first_col-3,len(park['var_names'])):
                park[park['var_names'][i]].append(np.nan)
            park['incomplete'] = True
            break        
        line = fp.readline()
    if line and not park['incomplete']: # first line after the park data was read
        fp.seek(last_pos) # reset file pointer to that line
    return park


def parse_profile_terminated(fp, coords):
    '''Parse the line that presumably contains "Profile ... terminated ..."
    from the file with the given file pointer fp.
    Add 'Profile_time' to the coords dictionary.
    Return True/False whether the profile time was found.'''
    last_pos = fp.tell()
    line = fp.readline()
    while line and not line.strip(): # skip empty lines, break out of loop at EOF
        line = fp.readline()
    if not line:
        return False
    regex = re.compile(r'Profile.*terminated:\s*(.+)')
    match_obj = regex.search(line)
    if match_obj:
        coords['Profile_time'] = get_seconds_since_1970(match_obj.group(1))
        return True
    else:
        print(line)
        print('Error/Note: profile time not found in the line just shown')
        pdb.set_trace()
        fp.seek(last_pos)
        return False


def parse_discrete_samples(fp):
    '''Parse the section of the msg file with the discrete samples. Create and
    return a dictionary that contains lists with names and values for each of
    the variables (as named in the line before the samples and a list with
    True/False values whether it is a Park Sample or not.
    Also return True/False whether the end of the file was reached 
    prematurely.'''
    discrete = dict()
    last_pos = fp.tell()
    line = fp.readline()
    regex = re.compile(r'\$\s+Discrete\s+samples:\s*(\d+)')
    match_obj = regex.search(line)
    if not match_obj: # includes cases of empty or partial lines
        print('nsamp not found')
        #pdb.set_trace()
        fp.seek(last_pos)
        return discrete, True
    nsamp = int(match_obj.group(1))
    if not nsamp:
        curr_pos = fp.tell()
        eof_pos = fp.seek(0, 2)
        fp.seek(curr_pos) # reset file pointer
        return None, curr_pos == eof_pos
    # the next line contains the variable names with a leading '$'
    line = fp.readline().replace('$', '')
    discrete['var_names'] = line.split()
    nvars = len(discrete['var_names'])
    for var in discrete['var_names']:
        discrete[var] = list()
    discrete['park_sample'] = list()    
    for i in range(nsamp):
        line = fp.readline()
        values = line.split()
        # this happens if line is incomplete
        if len(values) < nvars:
            print(f'Incomplete line for discrete samples: "{line}"')
            return discrete, True
        if '(Park Sample)' in line:
            discrete['park_sample'].append(True)
        else:
            discrete['park_sample'].append(False)
        for i in range(nvars):
            discrete[discrete['var_names'][i]].append(values[i])
    return discrete, False


def copy_park_sample(vars, discrete):
    '''If discrete contains at least one park sample, copy the last one
    and its pTS values to the vars dictionary.'''
    if discrete and 's' in discrete:
        nsamples = len(discrete['s'])
        vars['NDiscreteSamples'] = (nsamples, '')
        n_ps = sum(bool(x) for x in discrete['park_sample'])
        if n_ps == 1:
            idx = next((i for i, j in enumerate(discrete['park_sample'])
                        if j), None)
            vars['ParkPressure'] = (float(discrete['p'][idx]), 'dbar')
            vars['ParkTemperature'] = (float(discrete['t'][idx]), 'degC')
            vars['ParkSalinity'] = (float(discrete['s'][idx]), 'PSU')
        elif n_ps == 0:
            vars['ParkPressure'] = (np.nan, 'dbar')
            vars['ParkTemperature'] = (np.nan, 'degC')
            vars['ParkSalinity'] = (np.nan, 'PSU')
        else:
            print('more than one park sample found, unexpected')
            pdb.set_trace()

def parse_sbe_line(fp, vars):
    '''Read one line from the file with the given file pointer fp.
    Extract information from a line between the true header and the high-res
    data lines that contains the SBE serial number, the number of total samples,
    and the number of bins. If found, add these to the vars dictionary.
    Return True if the line before the high-res data lines was found, False
    otherwise.'''
    last_pos = fp.tell()
    line = fp.readline()
    while line == '\n': # some files have empty lines at this place
        line = fp.readline()
    # FIXME is it always this model? should it be generalized by capturing
    # the name in another group
    regex_sbe = re.compile(r'Sbe41cpSerNo\[(\d+)\]\s+NSample\[(\d+)\]\s+NBin\[(\d+)\]')
    match_obj = regex_sbe.search(line)
    if match_obj:
        vars['Sbe41cpSerNo'] = (match_obj.group(1), '')
        vars['NSample'] = (int(match_obj.group(2)), '')
        #DEBUG print(f'target nsamples: {vars["NSample"]}')
        vars['NBin'] = (int(match_obj.group(3)), '')
        return True
    else:
        fp.seek(last_pos) # go back to beginning of line
        print('no match in parse_sbe_line:')
        print(line)
        #pdb.set_trace()
        vars['Sbe41cpSerNo'] = ('Unknown', '')
        return False


def parse_high_res_pts(fp, nbin, nsamples, var_names, navis_bgc=False):
    '''Parse the high-resolution pTS data section of the file with the given
    file pointer fp. nbin is the expected number of bins.
    FIXME
    Return a dictionary that contains the number of high-resolution data
    points, a list with these data, and a boolean that indicates whether
    the end of the file was reached prematurely.'''
    # the repeat value at the end of the line (e.g., [2]) is not mandatory
    regex0 = re.compile(r'^00000000[0-9A-F]+(?:\[(\d+)\])?')
    pts = dict()
    if navis_bgc:
        fp.readline() # an empty line
        # the second line looks like this:
        # ser1: SBE63, 1947  ser2: MCOMS, 160 pH: 34 DualTherm: yes
        # its content is currently not parsed or stored
        # FIXME it should be! they are the serial numbers of O2, MCOMS, and pH sensors!!
        fp.readline()
    last_pos = fp.tell()
    line = fp.readline().strip()
    # the first line of this set typically contains zeroes for pTS
    match_obj = regex0.search(line)
    if match_obj:
        # the total number of values differs by float type and program
        # the number of repetitions is always enclosed in brackets
        # at the end of the line
        if match_obj.group(1):
            pts['nrep'] = int(match_obj.group(1))
        else:
            pts['nrep'] = 1 # a single "zeroes line" without repetition
    else:
        pts['nrep'] = 0   # no "zeroes line" at all
        fp.seek(last_pos) # read the line again in the loop below
        #print('no leading zeroes in high-res line!')
        #pdb.set_trace()

    # do not allow anything besides hex numbers and [] in the matching pattern
    regex = re.compile(r'^[0-9A-F\[\]]+$')
    #FIXME pts['data'] = list()
    pts['nhighres'] = 0
    #UNUSED pts['tot_samp'] = pts['nrep'] # FIXME is this right???
    nlines = nbin - pts['nrep']
    index = dict()
    if navis_bgc:
        # the following code is based on MBARI's parse_NAVISmsg4ARGO.m
        # 0-offset indices for pTS, O2 Phase&T, MCOMS (3 channels):
        index['hex'] = [0, 1, 2, 4, 5, 7, 8, 9]
        index['nbin'] = [3, 6, 10] # 0-offset indices for nbins for pTS, O2, MCOMS
        nbin_hdr = ['nbin ctd', 'nbin oxygen', 'nbin MCOMS'] # put bins at end
    else: # APEX
        #pdb.set_trace()
        index['hex'] = [0, 1, 2]
        index['nbin'] = [3]
    index['conv'] = [0, 1, 2] # 0-offset column indices for pTS
    hex_conv = np.array([ [32768, 10],     # p 
                          [61440, 1000],   # T
                          [61440, 1000] ]) # S
    #pdb.set_trace()
    if navis_bgc:
        if 'phV' in var_names and 'phT' in var_names:
            # this is the format of 0949.*.msg files (from Tanya Maurer/MBARI)
            num_len = [4, 4, 4, 2, 6, 6, 2, 6, 6, 6, 2, 6, 4, 2]
            index['hex'].extend([11, 12]) # pH V&T
            index['nbin'].append(13)
            nbin_hdr.append('nbin pH')
            index['conv'].append(9) # 0-offset column index for pH T
            pdb.set_trace()
            hex_conv = np.concatenate((hex_conv,[[61440,1000]]), axis=0) # for pH T
            print(hex_conv)
            has_ph = True      
        elif 'phVrs' in var_names and 'phVk' in var_names:
            # this is the format of 146x.*.msg files (PMEL, July 2022)
            num_len = [4, 4, 4, 2, 6, 6, 2, 6, 6, 6, 2, 6, 2]
            index['hex'].append(11) # pH V
            index['nbin'].append(12)
            nbin_hdr.append('nbin pH')
            #NOT USED index['conv'].append(9) # 0-offset column index for pH T
            #NOT USED hex_conv = np.concatenate((hex_conv,[[61440,1000]]), axis=0) # for pH T
            #DEBUG print(hex_conv)
            has_ph = True              
        else:
            has_ph = False
            #DEBUG pdb.set_trace()
            raise ValueError('not yet coded') # FIXME
    else: # APEX
        num_len = [4, 4, 4, 2]
        has_ph = False
        #pdb.set_trace()

    nvals_line = len(num_len)
    exp_len = sum(num_len)
    hr_values = np.full((nlines, nvals_line), np.nan)

    # determine starting and ending indices for the components
    # of each hex string line
    start_idx = list()
    start_idx.append(0)
    end_idx = list()
    end_idx.append(num_len[0])
    for i in range(1, len(num_len)):
        start_idx.append(end_idx[i-1])
        end_idx.append(end_idx[i-1] + num_len[i])

    #pdb.set_trace()    
    for i in range(nlines):
        last_pos = fp.tell()
        line = fp.readline().strip()
        #print(line)
        # an incomplete line may get commingled with the
        # next line ("# GPS fix...")
        if '#' in line:
            print('unexpected line in high-res section:')
            print(line)
            raise IOError('exiting right now!')
            #pdb.set_trace()
        #if not line or len(line) != exp_len:
        #    print('Premature end to high-res data detected!')
        #    break
        #    #return pts
        # check for the "all zeroes" pattern first;
        # now it will mark the end of the pTS data
        match_obj = regex0.search(line)
        if match_obj:
            if match_obj.group(1):
                nrep = int(match_obj.group(1))
            else:
                nrep = 1
                #print(f'ZEROES FOUND in line {line} with {nrep} reps')
            #FIXME pts['tot_samp'] += nrep # FIXME is this right???
            # "zero values" are not stored in hr_values matrix
            if nrep + i >= nlines:
                #DEBUG print('breaking out of high-res pts')
                # end of high-res data reached
                pts['incomplete'] = False
                #pdb.set_trace()
                break
        # next try the "regular" pattern
        match_obj = regex.search(line)
        if match_obj:
            #values = np.empty(nvals_line) # , dtype=np.int32)
            values = np.full(nvals_line, np.nan)
            val_count = 0
            for c in range(len(num_len)):
                if len(line) >= end_idx[c]:
                    try:
                        # FIXME is this taken care of in conversion with
                        # check for 2*24 - 1?
                        #if (line[start_idx[c]:end_idx[c]] ==
                        #    (end_idx[c] - start_idx[c]) * 'F'):
                        #    values[val_count] = np.nan
                        #else:
                        these_values = int(line[start_idx[c]:end_idx[c]], 16)
                        values[val_count] = these_values
                        val_count += 1
                    except Exception:
                        print('conversion error')
                        pdb.set_trace()
                else:
                    print('line is shorter than expected:')
                    print(line)
                    print(f'Actual length:   {len(line)}')
                    print(f'Expected length: {end_idx[-1]}')
                    # values was initialized to nan, nothing to do
                    break
            #FIXME if val_count == nvals_line:        
            hr_values[pts['nhighres'],:] = values
            pts['nhighres'] += 1
            #FIXME I don't think so! pts['tot_samp'] += 1 # FIXME is this right?
            #FIXME else:
            #FIXME    print('incomplete line - values not used') # FIXME
        else: # FIXME should go into log file
            print('Premature end to high-res data detected!')
            # FIXME MBARI code handles incomplete lines
            print('unexpected line:')
            print(line)
            #pdb.set_trace()
            if 'Resm' not in line:
                fp.seek(last_pos)
            pts['incomplete'] = True
            # FIXME, no - I should just break out of loop
            break
            #return pts
    else:
        pts['incomplete'] = False
    #print(pts['tot_samp'])
    #print(nsamples) # FIXME it should be compared to vars['NBin'] if anything!!
    #DEBUG print('high res pts parsed to the end!!')
    if navis_bgc and not pts['incomplete']:
        # usually contains: "Resm 0, Rstr 0, Rbt 0"
        line = fp.readline() # this line is not used if it contains "Resm..."
        if '#' in line:
            curr_pos = fp.tell() # beginning of next line
            # reset the file pointer so that the 
            fp.seek(curr_pos - len(line) + line.index('#'))

    # delete empty lines
    hr_values = hr_values[:pts['nhighres'],:]
    #pdb.set_trace()
    pts['hr_vals'] = convert_high_res_data(hr_values, index, hex_conv,
                                           navis_bgc, has_ph)
    #pts['tot_samp'] += int(sum(pts['hr_vals'][:,index['nbin'][-1]])) # FIXME correct for Navis?
    #UNUSED pts['tot_samp'] = sum(pts['hr_vals'][:,len(index['hex'])])
    #UNUSED print(f'tot samples: {pts["tot_samp"]}') # FIXME doesn't match NSample
    #pdb.set_trace()
    return pts


def convert_high_res_data(hr_values, index, hex_conv, navis_bgc, has_ph):
    '''Convert the data to regular values.
    Derived from MBARI function parse_NAVISmsg4ARGO.m
% ************************************************************************
%             CONVERT CP TO USEFUL NUMBERS AND/OR COUNTS
%    THIS IS A MODIFICATION AND CONDENSING OF Dan Quittman's CODE FOR:
% hextop.m, hextot.m and hextos.m functions
% for converting 16 bit 2's complement format to decimal
% It appears this is also a way to deal with a 12 bit A/D board in a
% 16 bit world as well as signed integers in a hex world
% ************************************************************************

    FIXME'''
    if hr_values.shape[1] == 0:
        print('no hr values!')
        return None
    # reorder columns - put bin count columns at the end
    hr_values = hr_values[:,index['hex'] + index['nbin']]
    tmp = hr_values[:,index['conv']]
    ono = np.ones((hr_values.shape[0], 1)) # shape: n_prof x 1
    # shape of hex_var1, hex_var2, t_nan, t_hi, t_lo: n_prof x 3
    # @ is the numpy matrix multiplication operator
    hex_var1 = ono @ hex_conv[:,0].reshape(1,hex_conv.shape[0])
    hex_var2 = ono @ hex_conv[:,1].reshape(1,hex_conv.shape[0])
    t_nan = np.full_like(tmp, np.nan, dtype=np.double)
    # only NaN is not equal to itself
    t_nan[tmp - hex_var1 != 0] = 1
    t_hi = (tmp - hex_var1 > 0).astype(int)
    t_lo = (tmp - hex_var1 < 0).astype(int)
    hr_values[:,index['conv']] = (t_hi * (tmp-65536) / hex_var2 +
        t_lo * tmp / hex_var2) * t_nan

    hr_values[hr_values == 2**24 - 1] = np.nan
    if navis_bgc:
        # NOW DO BIO-SENSORS
        hr_values[:,3] = hr_values[:,3] * 1.e-5 - 10. # O2 phase
        hr_values[:,4] = hr_values[:,4] * 1.e-6 - 1.  # O2 temperature volts
        hr_values[:,5:8] -= 500.                      # MCOMS (indices 5,6,7)
        if has_ph:
            hr_values[:,8] = hr_values[:,8] * 1.e-6 - 2.5  # pH volts
    return hr_values


def parse_optode_aircal(fp):
    '''Parse the lines of an BGC float msg file with OptodeAirCal information.
    Create and return a list with the information.'''
    opt = list()
    last_pos = fp.tell()
    line = fp.readline()
    regex = re.compile(r'OptodeAirCal: (.{20})\s+(.*)$')
    while line and line.startswith('OptodeAirCal:'):
        match_obj = regex.search(line)
        if match_obj:
            opt_time = get_seconds_since_1970(match_obj.group(1))
            values = match_obj.group(2).split()
            opt.append(values) # FIXME convert to number?
            if opt_time != int(values[0]):
                print('mismatching optode time')
                pdb.set_trace()
                
        last_pos = fp.tell()
        line = fp.readline()
    # reset to beginning of first line after OptodeAirCal lines
    fp.seek(last_pos)        
    return opt

def parse_msg_middle(fp, vars, coords):
    '''Parse the part of an msg file before the beginning of the high-res
    data. Note that this part doesn't exist for 000.msg files.'''
    # BGC floats have a section (of varying length) with park point data,
    # which are currently not yet used
    if vars['Program'][0] == 'BGC':
        if vars['Float_type'][0] == 'APEX':
            park_str = 'ParkPtFlbb'
        elif vars['Float_type'][0] == 'Navis':
            park_str = 'ParkObs'
        else:
            print('unknown float type')
            pdb.set_trace()
    else:
        park_str = 'ParkPts'
    park = parse_park_points(fp, park_str, vars['Program'][0])
    # if park['incomplete' is True EOF was reached
    if park['incomplete']:
        curr_pos = fp.tell()
        eof_pos = fp.seek(0, 2)
        if curr_pos == eof_pos: # EOF was reached
            return None, None, park, None
        else:
            fp.seek(curr_pos) # reset file pointer
    # next read the "Profile ... terminated ..." line
    # it may be missing, e.g., in 000 profiles
    parse_profile_terminated(fp, coords) # return value not used yet
    #    return vars, coords, None, None, park
    discrete, eof = parse_discrete_samples(fp)
    if discrete:
        copy_park_sample(vars, discrete)
    if eof:
        return discrete, None, park, None
    # read one line with Sbe serial no. and number of samples and bins
    if parse_sbe_line(fp, vars):
        #print('CONTINUE DEBUGGING HERE')
        pts = parse_high_res_pts(fp, vars['NBin'][0], vars['NSample'][0],
                                 discrete['var_names'],
                                 vars['Program'][0] == 'BGC' and
                                 vars['Float_type'][0] == 'Navis')
        #pdb.set_trace()
        vars['NHighResPTS'] = (pts['nhighres'], '')
        vars['ProfileLength'] = (pts['nhighres'], '') # Willa's pages use both variables
    else:
        # FIXME this case needs to be handled properly
        print('Sbe line not found')
        #pdb.set_trace()
        pts = None
    # BGC floats have a section (of varying length) with optode calibration data,
    # which are currently not yet used
    #print('don''t go further until all issues with parse_high_res_pts are fixed!')
    #pdb.set_trace()
    if vars['Program'][0] == 'BGC' and vars['Float_type'][0] == 'APEX':
        opt = parse_optode_aircal(fp)
    else:
        opt = None
    return discrete, pts, park, opt

def parse_msg_footer(fp, vars, coords):
    '''Parse the footer of the msg file with file pointer fp and fill
    dictionary vars with all successfully parsed variable names and their
    values and units. Note that units follow the values immediately.'''
    last_pos = fp.tell()
    # pattern for hex numbers without units:
    regex0 = re.compile(r'([\w]+)=(?:0x)([\da-f]+)')
    #regex1 = re.compile(r'([\w]+)=(?:0x)?([\d\.\-]+)(.*)')
    # standard pattern for scalar numbers (not hex) and optional units:
    regex1 = re.compile(r'([\w]+)=([\d\.\-]+)(.*)')
    regex2 = re.compile(r'([\w]+)\[([\d]+)\]=(.+)') # array variables
    regex3 = re.compile(r'([\w]+)=(.+)') # anything else
    array_vars = dict() # for variables that occur in multiple lines
    # variable names are on the lhs, rhs will always be a tuple
    line = fp.readline()
    while line and fp.tell() > last_pos: # '<EOT>' not in line:
        if 'FwRev' in line:
            #DEBUG pdb.set_trace()
            if line.startswith('Apf'):
                if vars['Float_type'][0] != 'Unknown':
                    if vars['Float_type'][0] != 'APEX':
                        print('ftype conflict')
                        print(line)
                        pdb.set_trace()
                else:
                    vars['Float_type'] = ('APEX', '')
            elif line.startswith('Npf'):
                if vars['Float_type'][0] != 'Unknown':
                    if vars['Float_type'][0] != 'Navis':
                        print('manu conflict')
                        print(line)
                        pdb.set_trace()
                else:
                    vars['Float_type'] = ('Navis', '')
            else:
                print('unexpected case in parse_msg_footer')
                pdb.set_trace()
            fw, rev = get_fwrev(line)
            if fw:
                vars['Firmware'] = (fw, '')
                vars[fw] = (rev, '') # FIXME is this always redundant with match below??
        #DEBUG if 'ParkObs' in line:
        #    print('PARK OBS!')
        match_obj0 = regex0.search(line)
        match_obj = regex1.search(line)
        if 'GPS fix' in line:
            if 'Fix_time' in coords:
                if ARGS.verbose:
                    print('encountered another GPS fix line, aborting read!')
                while line and '<EOT>' not in line:
                    last_pos = fp.tell()
                    line = fp.readline()
                fp.seek(last_pos) # allow reading of <EOT> line below    
            else:        
                fp.seek(last_pos) # current line will be read by parse_msg_gps_fix
                parse_msg_gps_fix(fp, vars, coords)
        elif match_obj0:
            vars[match_obj0.group(1)] = (int(match_obj0.group(2), base=16), '')
        elif match_obj:
            if match_obj.group(1).startswith('TimeSt'):
                #DEBUG print('special treatment for {0:s}'.format(match_obj.group(1)))
                # replace multiple spaces with one
                value = match_obj.group(2) + ' ' + match_obj.group(3)
                vars[match_obj.group(1)] = (' '.join(value.split()), '')
            else:
                vars[match_obj.group(1)] = (match_obj.group(2), match_obj.group(3))
        elif '<EOT>' in line:
            vars['msgEOT'] = ('1','')
            if 'Fix_time' in coords and not np.isnan(coords['Fix_time']):
                break # if GPS fix worked the first time, skip rest of the file
            else:
                last_pos = fp.tell()
                line = fp.readline()
                gps_found = False
                while line:
                    if 'GPS fix obtained' in line:
                        fp.seek(last_pos)
                        gps_found = parse_msg_gps_fix(fp, vars, coords)
                        if gps_found:
                            break # out of the inner line reading loop
                    last_pos = fp.tell()
                    line = fp.readline()
                if gps_found: # if not, keep searching to EOF
                    break # out of the outer line reading loop
        elif line.strip(): # skip empty lines
            # special treatment for several lines like this:
            # ParkDescentP[0]=6
            match_obj = regex2.search(line)
            if match_obj:
                if match_obj.group(1) not in array_vars:
                    array_vars[match_obj.group(1)] = ""
                # assume that they are always listed in ascending order of index
                array_vars[match_obj.group(1)] += match_obj.group(3) + ", "
            else:
                match_obj = regex3.search(line)
                if match_obj:
                    vars[match_obj.group(1)] = (match_obj.group(2), '')
                elif '=' not in line:
                    print('This looks like an incomplete line, it will not be processed:')
                    print(line)
                else: #FIXME this should go into logging output file
                    print(line)
                    print('NO MATCH (footer)') # DEBUG
                    pdb.set_trace()

        last_pos = fp.tell()
        line = fp.readline()
            
    for key in array_vars:
        if key == 'ParkDescentP':
            key_out = 'ParkDescentPistonP'
        else:
            key_out = key
        vars[key_out] = (array_vars[key].rstrip(), 'count')


def parse_isus_file(file, vars):
    '''Parse one isus file FIXME.'''
    pass

def get_seconds_since_1970(datetime_string):
    '''Convert a string in the form "Thu Nov 12 00:08:02 2020" to a time value
    in seconds since Jan 1, 1970 midnight and return it as a float.
    Also try "Nov 12 2020 00:08:02" as an alternate format.'''
    try:
        # e.g., "Thu Nov 12 00:08:02 2020"
        date_time_obj = datetime.datetime.strptime(datetime_string.strip(),'%c')
    except ValueError:
        try:
            # e.g., "Nov 12 2020 00:08:02"
            date_time_obj = datetime.datetime.strptime(datetime_string.strip(),
                                                       '%b %d %Y %H:%M:%S')
        except ValueError:
            # e.g., "07/20/2021 104130"
            date_time_obj = datetime.datetime.strptime(datetime_string.strip(),
                                                       '%m/%d/%Y %H%M%S')

    date_time_1970 = datetime.datetime(1970, 1, 1, 0, 0, 0)
    dt = date_time_obj - date_time_1970
    return dt.total_seconds()


def get_days_since_1950(datetime_string):
    '''Convert a string in the form "Thu Nov 12 00:08:02 2020" to a time value
    in days since Jan 1, 1950 midnight and return it as a float.
    Also try "Nov 12 2020 00:08:02" as an alternate format.'''
    try:
        # e.g., "Thu Nov 12 00:08:02 2020"
        date_time_obj = datetime.datetime.strptime(datetime_string.strip(),'%c')
    except ValueError:
        try:
            # e.g., "Nov 12 2020 00:08:02"
            date_time_obj = datetime.datetime.strptime(datetime_string.strip(),
                                                       '%b %d %Y %H:%M:%S')
        except ValueError:
            # e.g., "07/20/2021 104130"
            date_time_obj = datetime.datetime.strptime(datetime_string.strip(),
                                                       '%m/%d/%Y %H%M%S')

    date_time_1950 = datetime.datetime(1950, 1, 1, 0, 0, 0)
    dt = date_time_obj - date_time_1950
    return dt.total_seconds() / 86400.0


def create_nc_file(filename_out, filename_in, vars, verbose):
    '''Create netcdf file "filename_out" if it does not exist yet 
    and write simple numbers (not time-dependent variables)
    from the "vars" dictionary into it.  
    Note: So far, vars may be None! FIXME! (by reading log file as well)
    FIXME: Lists and strings are skipped for now.'''
    if os.path.exists(filename_out):
        if verbose:
            print(f'"{filename_out}" exists already!')
        return
    ncfile = nc.Dataset(filename_out, 'w', format='NETCDF4')
    # global attributes
    ncfile.history = 'Created by parse_msg_file.py'
    ncfile.source = filename_in
    ncfile.date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # floatid is a single variable, it doesn't need a dimension
    floatid = get_floatid(filename_in)
    floatid_var = ncfile.createVariable('floatid', np.int32, ())
    wmoid_var = ncfile.createVariable('wmoid', np.int32, ())
    string8_dim = ncfile.createDimension('STRING8', 8)
    string16_dim = ncfile.createDimension('STRING16', 16)
    string32_dim = ncfile.createDimension('STRING32', 32)
    string64_dim = ncfile.createDimension('STRING64', 64)
    string128_dim = ncfile.createDimension('STRING128', 128)
    # same for the program type and firmware revision
    prog_var = ncfile.createVariable('Program', 'S1', ('STRING8'))
    ftype_var = ncfile.createVariable('Float_type', 'S1', ('STRING8'))
    fwtype_var = ncfile.createVariable('Firmware', 'S1', ('STRING16'))
    for fw in firmware:
        if fw in vars:
            fw_var = ncfile.createVariable(fw, 'S1', ('STRING32'))
            break
    else:
        print('Warning: no firmware type was found!')
        #pdb.set_trace()
    # time dimension and (xyt) grid variables
    time_dim = ncfile.createDimension('time', None)

    cycle_var = ncfile.createVariable('CYCLE_NUMBER', np.int32, ('time',),
                                      fill_value=99999)
    cycle_var.long_name = 'Float cycle number';
    cycle_var.conventions = '0...N, 0 : launch cycle (if exists), 1 : first complete cycle'
    
    time_var = ncfile.createVariable('time', np.float64, ('time',),
                                     fill_value=np.nan)
    #time_var.units = 'days since 1950-01-01 00:00:00 UTC'
    #time_var.time_origin = '01-JAN-1950 00:00:00'
    #time_var.conventions = 'Relative julian days with decimal part (as parts of day)';
    #time_var.long_name = 'Julian day (UTC) of the station relative to REFERENCE_DATE_TIME';
    time_var.units = 'seconds since 01-JAN-1970 00:00:00'
    time_var.time_origin = '01-JAN-1970 00:00:00'
    time_var.calendar = 'gregorian'
    
    lon_var = ncfile.createVariable('longitude', np.float32, ('time',),
                                     fill_value=np.nan)
    lon_var.units = 'degrees_east'
    lat_var = ncfile.createVariable('latitude', np.float32, ('time',),
                                     fill_value=np.nan)
    lat_var.units = 'degrees_north'

    # output of time-independent variables
    floatid_var[:] = floatid
    wmoid_var[:] = DICT_FLOAT_IDS[floatid]
    if 'Program' in vars:
        # string must be exactly as long as they were dimensioned for
        str_out = vars['Program'][0].ljust(8, '\0')
        prog_var[:] = nc.stringtochar(np.array(str_out, 'S'))
    else:
        prog_var[:] = nc.stringtochar(np.array('UNKNOWN\0', 'S')) # FIXME test this!
    if 'Float_type' in vars:
        str_out = vars['Float_type'][0].ljust(8, '\0')
        ftype_var[:] = nc.stringtochar(np.array(str_out, 'S'))
        
    for fw in firmware:
        if fw in vars:
            str_out = fw.ljust(16, '\0')
            fwtype_var[:] = nc.stringtochar(np.array(str_out, 'S'))
            str_out = vars[fw][0].ljust(32, '\0')
            fw_var[:] = nc.stringtochar(np.array(str_out, 'S'))
            break
    # all time-dependent variables (vars may be None)
    for var in vars or []:
        if 'DialCmd' in var or 'DebugBits' in var or var in non_time_vars:
            continue
        # a few values are lists instead of strings
        if isinstance(vars[var][0], str):
            if var in string_vars or var in long_string_vars or \
               var.startswith('TimeSt'):
                var_type = 'S1'
            else:
                var_type = np.float32
        else:
            if isinstance(vars[var][0], int):
                var_type = np.int32
            elif isinstance(vars[var][0], float):
                var_type = np.float32
            else:
                print('unexpected case!')
                print(var)
                print(vars[var][0])
                pdb.set_trace()
                
        try: # check if this variable is already defined
            nc_var = ncfile[var]
        except IndexError: # it is not, so create it now
            #pdb.set_trace()
            if var in string_vars or var in long_string_vars or \
               var.startswith('TimeSt'):
                if var in long_string_vars:
                    str_type = 'STRING64'
                elif var == 'ParkObs' or var == 'SurfaceObs':
                    str_type = 'STRING128'
                else:
                    str_type = 'STRING32'
                nc_var = ncfile.createVariable(var, var_type,
                                               ('time', str_type),
                                               fill_value='')
            elif var_type == np.int32:
                nc_var = ncfile.createVariable(var, var_type, ('time',),
                                               fill_value=99999)
            else:
                nc_var = ncfile.createVariable(var, var_type, ('time',),
                                               fill_value=np.nan)
            try:
                nc_var.units = vars[var][1]
            except: # ParkObs and SurfaceObs have a different format?FIXME??
                print('except')
                pdb.set_trace()
                pass # FIXME

    ncfile.close()


def write_nc_one_step(filename_out, vars, coords, idt, profile, verbose):
    '''Precondition: filename_out must exist and variables
    must be defined.'''
    ncfile = nc.Dataset(filename_out, 'a')
    print(f'writing step {idt+1} to {filename_out}')
    ncfile['CYCLE_NUMBER'][idt] = profile
    # check if time-independent variables have values assigned
    time_indp_vars = ['Program', 'Float_type', 'Firmware']
    for ivar in time_indp_vars:
        content = ncfile[ivar][:].tobytes().decode().rstrip('\x00')
        if not content or content == 'Unknown':
            if ivar in vars and vars[ivar][0] != 'Unknown':
                dim0 = ncfile[ivar].dimensions[0] # determine string length
                len_str = int(dim0.lstrip('STRING'))
                str_out = vars[ivar][0].ljust(len_str, '\0')
                ncfile[ivar][:] = nc.stringtochar(np.array(str_out, 'S'))
                if ivar == 'Firmware':
                    fw = vars['Firmware'][0]
                    # the variable for the firmware type needs to be defined
                    fw_var = ncfile.createVariable(fw, 'S1', ('STRING32'))
                    str_out = vars[fw][0].ljust(32, '\0')
                    fw_var[:] = nc.stringtochar(np.array(str_out, 'S'))
    if 'time' in coords:
        ncfile['time'][idt] = coords['time']
    if 'lon' in coords:
        ncfile['longitude'][idt] = coords['lon']
    if 'lat' in coords:
        ncfile['latitude'][idt] = coords['lat']
    # the 000.msg file does not contain the CTD serial number,
    # so it must be defined and written later, but only once
    if 'Sbe41cpSerNo' in vars:
        try: # check if this variable is already defined
            nc_var = ncfile['Sbe41cpSerNo']
        except IndexError:
            nc_var = ncfile.createVariable('Sbe41cpSerNo', np.int32, ())
            if vars['Sbe41cpSerNo'][0] != 'Unknown':
                nc_var[:] = int(vars['Sbe41cpSerNo'][0])
    # all time-dependent variables
    for var in vars or []:
        if var in skip_vars or var in non_time_vars:
            continue
        if var in string_vars or var in long_string_vars or \
           var.startswith('TimeSt'):
            var_type = 'S1'
        else:
            var_type = np.float32
        try:
            nc_var = ncfile[var]
        except IndexError:
            if var in string_vars:
                if var == 'ParkObs' or var == 'SurfaceObs':
                    nc_var = ncfile.createVariable(var, var_type, ('time', 'STRING128'))
                else:
                    nc_var = ncfile.createVariable(var, var_type, ('time', 'STRING32'))
            elif var in long_string_vars:
                nc_var = ncfile.createVariable(var, var_type, ('time', 'STRING64'))
            else:
                nc_var = ncfile.createVariable(var, var_type, ('time',),
                                               fill_value=np.nan)
            try:
                nc_var.units = vars[var][1]
            except:
                pass # FIXME
        # a few values are lists instead of strings
        try:
            if (isinstance(vars[var][0], str) or isinstance(vars[var][0], int) or
                isinstance(vars[var][0], float)):
                if var_type == 'S1':
                    if var == 'ParkObs' or var == 'SurfaceObs':
                        str_type = 'STRING128'
                        str_out = vars[var][0].ljust(128, '\0')
                    elif var in long_string_vars:
                        str_type = 'STRING64'
                        str_out = vars[var][0].ljust(64, '\0')
                    else:
                        str_type = 'STRING32'
                        str_out = vars[var][0].ljust(32, '\0')
                    nc_var[idt] = nc.stringtochar(np.array(str_out, 'S'))
                else:
                    if (isinstance(vars[var][0], str) and
                        vars[var][0].startswith('0x')):
                        vars[var] = (vars[var][0][2:], vars[var][1])
                    try:
                        this_num = float(vars[var][0])
                        ncfile[var][idt] = this_num
                    except ValueError:
                        if (not 'nan' in vars[var][0].lower() and
                            not 'disabled' in vars[var][0].lower() and verbose):
                            print(f'cannot convert: {vars[var][0]}')
                            pdb.set_trace()
            else:
                print('unhandled var in write_nc:')
                print(var)
                pdb.set_trace()
        except:
            print('problem in nc write')
            pdb.set_trace()
    ncfile.close()


def add_time_step_nc(filename_out, profile):
    '''Add a time step for a profile between existing time steps
    in a netcdf file.
    Pre: netcdf file must exist and have at least one time step.
    The value of "profile" should be less than the current 
    maximum value of CYCLE_NUMBER - if not, the function will
    return without changing anything in the file.
    Note that new values will not be inserted.
    Return value is the insertion index.'''
    if not os.path.exists(filename_out):
        raise OSError(f'File "{filename_out}" not found')
    try:
        ncfile = nc.Dataset(filename_out, 'a')
    except Error as e:
        pdb.set_trace()
    cycles = ncfile['CYCLE_NUMBER'][:]
    idt = bisect.bisect(cycles, profile)
    # calling function should ensure that this will not happen:
    if idt == len(cycles):
        ncfile.close()
        return
    print(f'Inserting profile {profile} in position {idt} out of {len(cycles)-1}')
    for var in ncfile.variables:
        if 'N_PROF' in ncfile[var].dimensions:
            #print(f'{var} is time-dependent')
            for i in range(len(cycles), idt, -1):
                ncfile[var][i] = ncfile[var][i-1]
                if '_FillValue' in dir(ncfile[var]):    
                    ncfile[var][i-1] = ncfile[var]._FillValue
                elif i == len(cycles):
                    print(f'WARNING: variable {var} does not have a _FillValue attr!')
    ncfile.close()
    return idt
    
        
def write_nc_file(filename_out, vars, coords, profile, verbose):
    if not os.path.exists(filename_out):
        print('"{filename_out} does not exist!')
        return
    ncfile = nc.Dataset(filename_out, 'r')
    cycles = ncfile['CYCLE_NUMBER'][:]
    ncfile.close() # need to switch to append mode
    # check if new time is not yet present in the file
    if len(cycles) == 0 or profile > cycles[-1]:
        # insert at the end (the standard case)
        nt = len(cycles)
        write_nc_one_step(filename_out, vars, coords, nt, profile, verbose)
    else:
        idt = np.nonzero(cycles == profile)[0]
        if idt.size:
            write_nc_one_step(filename_out, vars, coords, idt[0], profile, verbose)
            print(f'Modified data written to {filename_out}, cycle {idt}')
        else:
            print('New profile between existing cycles')
            idt = add_time_step_nc(filename_out, profile)
            write_nc_one_step(filename_out, vars, coords, idt, profile, verbose)
    

def get_floatid(filename_in):
    '''Determine the floatid from the given filename and return it as an int.'''
    regex = re.compile(r'([\d]+)\.[\d]+\.msg')
    match_obj = regex.search(filename_in)
    if match_obj:
        return int(match_obj.group(1))
    else:
        raise ValueError('could not determine floatid of file' +
                         '{0:s}'.format(filename_in))


def get_checksum(filename, hash_function='sha256'):
    '''Generate checksum for file based on hash function (MD5 or SHA256).

    Args:
        filename (str): Path to file that will have the checksum generated.
        hash_function (str):  Hash function name -
                              supports MD5 or SHA256 (default)

    Returns:
        str: Checksum based on Hash function of choice.

    Raises:
        Exception: Invalid hash function is entered.

    Source:
        https://onestopdataanalysis.com/checksum
    '''

    hash_function = hash_function.lower()

    with open(filename, "rb") as f:
        bytes = f.read()  # read file as bytes
        if hash_function == 'sha256':
            readable_hash = hashlib.sha256(bytes).hexdigest()
        elif hash_function == 'md5':
            readable_hash = hashlib.md5(bytes).hexdigest()
        else:
            Raise(f'{hash_function} is an invalid hash function.' +
                  'Please use md5 or sha256')

    return readable_hash


def create_log_file(filename_log):
    '''Create the file that logs which raw Argo files have been
    processed yet. Raise an IOError if the file cannot be created.'''
    try:
        with open(filename_log, 'w') as file:
            file.write('Filename,FloatID,WMOID,Type,Profile,Size,Checksum,')
            file.write('Processing_date\n')
    except:
        raise IOError(f'ERROR: Could not create "{filename_log}"!')


def mark_file_processed(filename, filename_log):
    '''Add the file with the given filename to the list of files that
    have been processed, including information about it, including
    the given file_type, its size, and its checksum.
    Information is written to the log file with the given name. If that file
    doesn't exist yet, it will be created.'''
    # extract internal ID, profile etc. from filename
    _, floatid, profile, ftype = parse_filename(filename)
    wmoid = DICT_FLOAT_IDS[floatid]
    shasum = get_checksum(filename)
    size = os.path.getsize(filename)
    now = datetime.datetime.now()
    with open(filename_log, 'a') as file:
        file.write(f'{filename},{floatid},{wmoid},{ftype},{profile},{size},')
        file.write(f'{shasum},{now.strftime("%Y/%m/%d %H:%M:%S")}\n')



def parse_input_args():
    '''Parse the command line arguments and return them as an object.'''
    parser = argparse.ArgumentParser(description='Parse raw Argo files,' +
                                     'create netcdf files for ERDDAP')

    # required argument:
    parser.add_argument('filename_in', nargs='+', help='name of the input file(s)')
    # options:
    parser.add_argument('-d', '--directory', default='.', type=str,
                       help='working directory (default: cwd)')
    parser.add_argument('-l', '--log', default=None, type=str,
                       help='name of log file (default: no output to log file)')
    parser.add_argument('-o', '--output_directory', default='.', type=str,
                        help='output directory (default: cwd)')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='if set, display more progress updates')

    args = parser.parse_args()
    # note that filename_in is always a list object,
    # even if there is just one file

    # the log file has to reside in the specified directory
    if args.log and args.directory != '.':
        args.log = f'{args.directory}/{args.log}'

    return args


def test_write_high_res_csv(fn_hr, hr_data):
    '''For comparison with Matlab output only!'''
    # %d doesn't work with nan, but %.0f does
    np.savetxt(fn_hr, hr_data, delimiter=',',
               fmt='%.1f,%.3f,%.3f,%.5f,%.6f,%.0f,%.0f,%.0f,%.6f,%.0f,%.0f,%.0f,%.0f',
               header='p,t,s,O2ph,O2tV,Mch1,Mch2,Mch3,phV,nbin ctd,nbin oxygen,nbin MCOMS,nbin pH')
    # older format with phT:
    #np.savetxt(fn_hr, hr_data, delimiter=',',
    #           fmt='%.1f,%.3f,%.3f,%.5f,%.6f,%.0f,%.0f,%.0f,%.6f,%.3f,%.0f,%.0f,%.0f,%.0f',
    #           header='p,t,s,O2ph,O2tV,Mch1,Mch2,Mch3,phV,phT,nbin ctd,nbin oxygen,nbin MCOMS,nbin pH')

    
def test_write_low_res_csv(fn_lr, discrete):
    '''For comparison with Matlab output only! 
    Park sample points are not included.'''
    discr_data = np.column_stack([discrete['p'], discrete['t'], discrete['s'],
                                  discrete['no3'], discrete['O2ph'],
                                  discrete['O2tV'], discrete['Mch1'],
                                  discrete['Mch2'], discrete['Mch3'],
                                  discrete['phVrs'], discrete['phVk'],
                                  discrete['phIb'],discrete['pHIk']]).astype(float)
    # older format                           discrete['phV'], discrete['phT']]).astype(float)
    mask_park = np.array(discrete['park_sample']) # convert list to numpy array
    park_data = discr_data[mask_park]
    lr_data = discr_data[~mask_park]
    # %d doesn't work with nan, but %.0f does
    np.savetxt(fn_lr, lr_data, delimiter=',',
               fmt='%.2f,%.4f,%.4f,%.5f,%.6f,%.6f,%.0f,%.0f,%.0f,%.6f,%.6f,%.6f,%.6f',
               header='p,t,s,no3,O2ph,O2tV,Mch1,Mch2,Mch3,phVrs,phVk,phIb,phIk')
    # older format with pht:
    #np.savetxt(fn_lr, lr_data, delimiter=',',
    #           fmt='%.2f,%.4f,%.4f,%.5f,%.6f,%.6f,%.0f,%.0f,%.0f,%.6f,%.6f',
    #           header='p,t,s,no3,O2ph,O2tV,Mch1,Mch2,Mch3,phV,phT')
    
    
def test_write_park_csv(fn_park, park):
    '''For comparison with Matlab output only! 
    These are "ParkObs" data, not "Park sample" points.'''
    # In Matlab:
    # datenum(1950,1,1): 712224
    park_data = np.column_stack([park['Date'],park['p'], park['t'],
                                 park['s'], park['O2ph'],
                                 park['O2tV'],
                                 park['phVrs'], park['phVk'],
                                 park['phIb'], park['phIk']]).astype(float)
    # older format                           park['phV'], park['phT']]).astype(float)
    # %d doesn't work with nan, but %.0f does
    np.savetxt(fn_park, park_data, delimiter=',',
               fmt='%.9f,%.2f,%.4f,%.4f,%.4f,%.6f,%.6f,%.6f,%.4e,%.4e',
               header='Date,p,t,s,O2ph,O2tV,phVrs,phVk,phIb,phIk')
    
    
if __name__ == '__main__':
    # global variables, used by more than one function
    string_vars = ['MessageTime', 'ParkObs', 'AirSystemBarometer', 'SurfaceObs']
    long_string_vars = ['AirSystemBattery', 'AirSystemCurrent',
                        'ParkDescentPistonP']
    firmware = ['Apf9iFwRev', 'Apf11FwRev', 'NpfFwRev']
    non_time_vars = ['Float_type', 'Program', 'Sbe41cpSerNo', 'Firmware'] + firmware
    skip_vars = ['AltDialCmd', 'AtDialCmd', 'DebugBits', 'Pwd', 'User']
    ARGS = parse_input_args()

    # FIXME hard-coded file name
    # contents of this dictionary: dict_float_ids[internal_id] = wmoid
    DICT_FLOAT_IDS = get_float_ids(f'{ARGS.directory}/floats.csv')

    # not all float types will have all of these file types
    all_file_types = ['msg', 'log'] # FIXME , 'isus', 'dura']

    if ARGS.log and not os.path.exists(ARGS.log):
        create_log_file(ARGS.log)

    for file in ARGS.filename_in:
        if not os.path.exists(file):
            continue
        if ARGS.log and not check_conv_need(file, all_file_types, ARGS.log):
            continue

        filename_out_eng = get_filename_out(file, ARGS.output_directory, 'eng')
        if ARGS.verbose:
            print(f'Processing "{file}", writing to "{filename_out_eng}"')
        vars, coords, pts, discrete, park = parse_msg_file(file)
        full_path = os.path.split(file)
        #DEBUG  pdb.set_trace()
        csv_out = False
        if csv_out:
            fn_hr = f'py_{full_path[1].replace(".msg","").replace(".","_")}_hr.csv'
            fn_lr = fn_hr.replace('_hr.csv', '_lr.csv')
            fn_park = fn_hr.replace('_hr.csv', '_pk.csv')
            if pts:
                test_write_high_res_csv(fn_hr, pts['hr_vals'])
            if discrete: 
                test_write_low_res_csv(fn_lr, discrete)
            if park:    
                test_write_park_csv(fn_park, park)

        fn_log = file.replace('.msg', '.log')
        if parse_log_file(fn_log, vars, coords) >= 0 and ARGS.log:
            # even if <EOT> was not found, mark it as processed
            mark_file_processed(fn_log, ARGS.log)

        #parse_isus_file(file, vars)
        #continue
        #FIXME!!!!! check_airsystem(vars) # FIXME do I always need to have these variables?
        assign_time(vars, coords)
        # FIXME for compatibility with Willa's pages - should it stay? I also have
        # vars['NHighResPTS'] with the same values
        if 'ProfileLength' not in vars:
            vars['ProfileLength'] = ('0', '') # parsing results in strings as well
        if vars or coords:
            create_nc_file(filename_out_eng, file, vars, ARGS.verbose)
            _, _, profile = parse_filename(file)[0:3]    
            write_nc_file(filename_out_eng, vars, coords, profile, ARGS.verbose)
            #pdb.set_trace()
            if ARGS.log:
                mark_file_processed(file, ARGS.log)
        elif ARGS.verbose:
            if not os.path.exists(file):
                print('{0:s} not found, skipping...'.format(file))
            elif not os.path.getsize(file):
                print('{0:s} is an empty file, skipping...'.format(file))
            else:
                print('No relevant data found in {0:s}, skipping...'.format(file))
        #if pts: # FIXME currently only works correctly for Navis
        #    write_nc_nb_sample_ctd(pts['hr_vals'], file)
