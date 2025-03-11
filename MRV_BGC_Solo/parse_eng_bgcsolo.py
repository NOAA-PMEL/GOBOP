#!/home/argotest/anaconda3/bin/python
#
# H. Frenzel, CICOES, UW // NOAA-PMEL
#
# First version: 2025

'''This script
FIXME
'''

import argparse
import datetime
import os
import re
import time

import netCDF4 as nc
import numpy as np
import pandas as pd



import pdb

FILE_TYPES = ['CTD', 'ALK', 'DOX', 'ECO', 'Na3', 'Nb3', 'OCR',
              'en1', 'en2', 'en3', 'en4', 'en5', 'gps', 'pmp']
CTD_COLUMNS = ['Cycle', 'Status', 'numErr', 'volt', 'pres',
               'ma0', 'ma1', 'ma2', 'ma3']
ALK_COLUMNS = ['Cycle', 'Status', 'numErr', 'volt', 'maMax', 'maAvg',
               'Vrs', 'Vk', 'lk', 'lb']
DOX_COLUMNS = ['Cycle', 'Status', 'numErr', 'volt', 'maMax', 'maAvg',
               'doPh', 'thmV', 'DO', 'thmC']
ECO_COLUMNS = ['Cycle', 'Status', 'numErr', 'volt', 'maMax', 'maAvg',
               'Chl', 'bb', 'CDOM']
Na3_COLUMNS = ['Cycle', 'status', 'numErr', 'J_err', 'J_rh', 'J_volt', 'J_amps',
               'J_darkM', 'J_darkS', 'J_no3', 'J_res', 'J_fit1', 'J_fit2',
               'J_Spectra', 'J_SeaDark']
Nb3_COLUMNS = ['Cycle', 'SpecWidth', 'Spec35', 'Spec36', 'Spec37', 'Spec38',
               'Spec39', 'Spec40', 'Spec41', 'Spec42', 'Spec43', 'Spec44',
               'Spec45', 'Spec46', 'Spec47']
Nc3_COLUMNS = ['Cycle', 'Sample NO3 Sensor Output']
OCR_COLUMNS = ['Cycle', 'Status', 'numErr', 'volt', 'maMax', 'maAvg',
               '380', '412', '490', 'PAR'] # FIXME last ones may depend on float
en1_COLUMNS = ['Cycle', '#Eng', '#Queue', '#SentP', '#TrysP', 'RudErr', '#ParsP',
               'SatTme', 'SBscan', 'SBstat', 'Vcpu   (V)','Vpmp   (V)',
               'Vple   (V)', 'VACnow (inHG)', 'VACb4  (inHG)', 'VACaft (inHG)']
en2_COLUMNS = ['Cycle', 'PmtoSf (s)', 'PmSf   (s)', 'FallTm (s)', 'FallRt (mm/s)',
               'SeekTm (s)', 'SeekP  (db)', 'PP>TIP (s/10)', 'SPRX   (db)',
               'SPRXL  (db)', 'PCorr  (db)', 'RH     (%)', 'CPUT   (C)']
en3_COLUMNS = ['Cycle', 'Pavg0  (db)', 'Tavg0  (C)', 'Savg0  (psu)',
               'Pavg1  (db)', 'Tavg1  (C)', 'Savg1  (psu)',
               'diagP0 (db)', 'diagT0 (C)', 'diagS0 (psu)',
               'diagP1 (db)', 'diagT1 (C)', 'diagS1 (psu)']
en4_COLUMNS = ['Cycle', 'HPavgI (ma)', 'HPmaxI (ma)', 'phBat0 (V)', 'phBat1 (V)',
               'Vent   (s/10)', 'VentLD', 'ExcFlg', 'ISRID', 'ATSBD',
               '#BinM', '#SubCy', '#IceDt']
en5_COLUMNS = ['Cycle', 'SciVer', '#SDnow', '#SDtot', 'SDfree (mB)',
               'CTDErr', '#PJump']
gps_COLUMNS = ['Cycle', 'First_Lat', 'First_Long', 'First_CycT', 'First_OK',
               'First_Time', 'First_#S', 'First_MnS', 'First_AvS',
               'First_MxS', 'First_Dil', 'Last_Lat', 'Last_Long',
               'Last_SrfT', 'Last_OK', 'Last_Time', 'Last_#S',
               'Last_MnS', 'Last_AvS', 'Last_MxS', 'Last_Dil',
               'Iridium_Lat', 'Iridium_Long', 'Iridium_Err']
pmp_COLUMNS = ['Number', 'Code', 'Pressure (db)', 'Time (s)', 'Voltage (V)',
               'Current (ma)', 'Energy (J)', 'Cumulative  Energy (KJ)',
               'Vacuum0_load_start', 'Vacuum1_load_stop']
FLOAT_COLUMNS = ['volt', 'pres', 'Vrs', 'Vk', 'lk', 'lb', 'doPh', 'thmV',
                 'DO', 'thmC', 'J_rh', 'J_volt', 'J_amps', 'J_no3',
                 'First_Lat', 'First_Long', 'First_CycT', 'Last_Lat',
                 'Last_Long', 'Iridium_Lat', 'Iridium_Long', 'Iridium_Err',
                 'Pressure (db)', 'Voltage (V)']
STRING_COLUMNS = ['Sample NO3 Sensor Output']

#def change_cwd(this_dir):
#    '''Change to the specified directory, unless it is None.'''
#    if this_dir:
#        os.chdir(this_dir)


def get_column_names(ftype):
    '''Return the names of the expected columns as a list
    of strings based on the given file type.'''
    if ftype == 'CTD':
        columns = CTD_COLUMNS
    elif ftype == 'ALK':
        columns = ALK_COLUMNS
    elif ftype == 'DOX':
        columns = DOX_COLUMNS
    elif ftype == 'ECO':
        columns = ECO_COLUMNS
    elif ftype == 'Na3':
        columns = Na3_COLUMNS
    elif ftype == 'Nb3':
        columns = Nb3_COLUMNS
    elif ftype == 'Nc3':
        columns = Nc3_COLUMNS
    elif ftype == 'OCR':
        columns = OCR_COLUMNS
    elif ftype == 'en1':
        columns = en1_COLUMNS
    elif ftype == 'en2':
        columns = en2_COLUMNS
    elif ftype == 'en3':
        columns = en3_COLUMNS
    elif ftype == 'en4':
        columns = en4_COLUMNS
    elif ftype == 'en5':
        columns = en5_COLUMNS
    elif ftype == 'gps':
        columns = gps_COLUMNS
    elif ftype == 'pmp':
        columns = pmp_COLUMNS
    else:
        raise ValueError('not yet coded')
    return columns


def get_juld(match_obj):
    '''Extract the date and time from a string like
    "22/ 1/2025 20:46: 1", i.e., D/M/YYYY H:MM:SS
    Return fractional days since Jan 1, 1950 (Argo time convention).
    '''
    day = int(match_obj.group(1))
    month = int(match_obj.group(2))
    year = int(match_obj.group(3))
    hour = int(match_obj.group(4))
    minute = int(match_obj.group(5))
    second = int(match_obj.group(6))
    dtime = datetime.datetime(year, month, day, hour, minute, second)
    # use Argo convention: days since 1950/1/1
    juld = (dtime - datetime.datetime(1950,1,1)).total_seconds() / 86400
    return juld


def parse_pmp_file(lines, serial_no):
    '''FIXME
    '''
    file_info = {} # a list with the cycles as keys
    columns = get_column_names('pmp')
    do_parse = False
    get_avg = False
    regex_cycle = re.compile(r'>Cycle\s+(\d+)\s+GMT\s+([\d\s/:]+)')
    regex_td = re.compile(r'<t[dh] align="center">(.+)</t[dh]>')
    regex_ce = re.compile(r'\s*([\d\.]+)\s*/\s*([\d\.]+)')
    regex_avg = re.compile(r'<B>\s*([\d\.]+)\s*\(KJ/cycle\)\s*</B>')
    regex_datetime = re.compile(r'(\d+)/\s*(\d+)/(\d+)\s+(\d+):\s*(\d+):\s*(\d)')
    for line in lines:
        if 'Vacuum' in line:
            line = line.replace('<br>', '_').replace('(load-', 'load_')
            line = line.replace(')', '')
        else:
            line = line.replace('<br>', ' ')
        if 'Cycle' in line and 'GMT' in line:
            match_obj = regex_cycle.search(line)
            if match_obj:
                cycle = int(match_obj.group(1))
                #DEBUG print(f'Cycle: {cycle}')
                col_count = 0
                col_count_header = 0 # each Cycle block has the column names
                do_parse = True
                file_info[cycle] = {}
                for col in columns:
                    file_info[cycle][col] = []
                match_obj_datetime = regex_datetime.search(match_obj.group(2))
                file_info[cycle]['time'] = get_juld(match_obj_datetime)
        elif do_parse:
            if '</table>' in line:
                do_parse = False # end of this cycle
                continue
            match_obj = regex_td.search(line)
            if not match_obj:
                if '<tr>' in line:
                    col_count = 0 # new row started
                continue
            match_str = match_obj.group(1).strip()
            if col_count_header < len(columns):
                if match_str == columns[col_count_header]:
                    col_count_header += 1
            elif 'Average' in line:
                get_avg = True    
            else:
                str_value = match_obj.group(1)
                #print(f'str_value: {str_value}')
                if get_avg:
                    match_obj = regex_avg.search(str_value)
                    file_info[cycle]['Average_Energy'] = float(match_obj.group(1))
                    get_avg = False
                elif columns[col_count] == 'Cumulative  Energy (KJ)':
                    match_obj = regex_ce.search(str_value)
                    file_info[cycle][columns[col_count]].append(
                        float(match_obj.group(1)))
                    file_info[cycle]['Total Cumulative Energy'] = \
                        float(match_obj.group(2)) # only keep last value
                elif columns[col_count] in STRING_COLUMNS:
                    file_info[cycle][columns[col_count]].append(str_value)
                elif columns[col_count] in FLOAT_COLUMNS:
                    file_info[cycle][columns[col_count]].append(float(str_value))
                else:
                    file_info[cycle][columns[col_count]].append(int(str_value))
                col_count += 1    
    return file_info            


def parse_file(file_path, ftype, serial_no):
    '''Parse one file and extract the information from the columns.'''
    file_info = {}
    if not os.path.exists(file_path):
        print(f'File "{file_path}" not found!')
        return file_info
    with open(file_path) as f_in:
        lines = f_in.readlines()
    if ftype == 'pmp':
        return parse_pmp_file(lines, serial_no)

    do_parse = False
    regex_bist = re.compile(r'Serial=\s*(\d+)\s+WMO=\s*(\d+)')
    # note that en* files use 'th', others use 'td'
    regex_td = re.compile(r'<t[dh] align="center">(.+)</t[dh]>')
    is_header = True
    columns = []
    for line in lines:
        if 'Serial' in line and 'WMO' in line:
            #DEBUG print(line)
            match_obj = regex_bist.search(line)
            if match_obj:
                if int(match_obj.group(1)) != serial_no:
                    print(f'Unexpected serial number: {match_obj.group(1)}',
                          end='')
                    print(f' found, {serial_no} expected.')
                    return file_info
                if int(match_obj.group(2)) != DICT_FLOAT_IDS[serial_no][0]:
                    print(f'Unexpected WMO number: {match_obj.group(2)}',
                          end='')
                    print(f' found, {DICT_FLOAT_IDS[serial_no][0]} expected.')
                    return file_info
                do_parse = True
        elif do_parse:
            match_obj = regex_td.search(line)
            if not match_obj:
                if '<tr>' in line:
                    col_count = 0 # new row started
                    if is_header and len(columns):
                        is_header = 0 # next come the data
                        for col in columns:
                            file_info[col] = []
                continue
            match_str = match_obj.group(1).strip()
            if ftype == 'gps':
                if match_str == 'Cyc':
                    match_str = 'Cycle'
                    prefix = 'First_'
                elif match_str == '_':
                    if prefix == 'First_':
                        prefix = 'Last_'
                    else:
                        prefix = 'Iridium_'
                    continue    
                else:
                    match_str = prefix + match_str
            
            if is_header:
                columns.append(match_str)
            else:
                str_value = match_obj.group(1)
                if str_value.startswith('**'):
                    if columns[col_count] in FLOAT_COLUMNS:
                        str_value = '-999.00'
                    else:
                        print('replacing STARS with -999')
                        str_value = '-999'
                        pdb.set_trace()
                if columns[col_count] in STRING_COLUMNS:
                    value = str_value.strip()
                else:
                    value = float(str_value)
                    if (int(value) == value and
                        columns[col_count] not in FLOAT_COLUMNS):
                        value = int(value)
                        #DEBUG print(f'using int for {match_obj.group(1)}')
                file_info[columns[col_count]].append(value)
                col_count += 1

    return file_info


def check_cycle_exists(nc_out):
    '''Check if the 'Cycle' variable exists in the given netCDF file exists
    already. Return True or False.'''
    try:
        var = nc_out.variables['Cycle']
        return True
    except KeyError:
        return False

def get_var_name_units_nc(var):
    '''Create a netCDF-compatible variable name and possibly extract the units
    from the variable name used in the html files.'''
    # for output, replace multiple spaces with single space
    regex_units = re.compile(r'[\w\s]+\(([\w/%]+)\)')
    var = ' '.join(var.split())
    var = var.replace('#', 'Num').replace('>', '_gt_')
    units = None # default
    if ' ' in var:
        match_obj = regex_units.search(var)
        if not match_obj:
            var = var.replace(' ', '_')
        else:
            units = match_obj.group(1)
            unit_str = f'({units})'
            var = var.replace(unit_str, '').strip().replace(' ', '_')
    return var, units


def create_nc_one_variable(nc_out, this_dict, var, ftype, use_number=False):
    '''Create one variable in the netCDF output file.'''
    # There is only one Cycle variable for the file that is
    # used for all sensors
    #print(f'create {var}')
    if var == 'Cycle':
        if not check_cycle_exists(nc_out):
            nc_out.createVariable('Cycle', 'i4', ('N_PROF'))
        return
    if isinstance(this_dict[var], list):
        # FIXME not checking for length
        if isinstance(this_dict[var][0], int):
            var_type = 'i4'
            fill_val = -999
        elif isinstance(this_dict[var][0], float):
            var_type = 'f4'
            if var == 'Iridium_Lat':
                fill_val = -99.99
            elif var == 'Iridium_Long':
                fill_val = -999.99
            elif var == 'Iridium_Err':
                fill_val = 999.0
            else:
                fill_val = -999.0
        elif isinstance(this_dict[var][0], str):
            var_type = str
            #fill_val = ''
        else:
            print('not yet coded')
            pdb.set_trace()
    elif isinstance(this_dict[var], float):
        var_type = 'f4'
        fill_val = -999.0
    else:
        print('also not yet coded')
        pdb.set_trace()

    var, units =  get_var_name_units_nc(var)
    var_name = f'{ftype}_{var}'
    if var_name == 'pmp_time':
        var_name = 'JULD'
        use_number = False
    if var_type == str:
        nc_out.createVariable(var_name, var_type, ('N_PROF','STRING'))
    else:
        if use_number:
            v = nc_out.createVariable(var_name, var_type, ('N_PROF', 'Number'),
                                      fill_value=fill_val)
        else:
            v = nc_out.createVariable(var_name, var_type, ('N_PROF'),
                                      fill_value=fill_val)
            if var_name == 'JULD':
                v.long_name = 'Julian day (UTC) of the station relative to REFERENCE_DATE_TIME'
                v.units = 'days since 1950-01-01 00:00:00 UTC'
                v.standard_name = 'time'
    if units:
        v.units = units


def create_nc_pmp_vars(nc_out, file_info_pmp):
    '''Create the pmp-related variables in the netCDF output file.'''
    if len(file_info_pmp.keys()) == 0:
        return
    # pmp files do not have 'Cycle' as a variable like the other files
    if not check_cycle_exists(nc_out):
        nc_out.createVariable('Cycle', 'i4', ('N_PROF'))
    nc_out.createDimension('Number', 20) # fairly conservative upper bound
    # all cycles should have the same columns, doesn't matter which one we pick
    first_key = next(iter(file_info_pmp))
    this_dict = file_info_pmp[first_key]
    its_vars = file_info_pmp[first_key].keys()
    # most or all of these variables need Number as a second dim, so
    # I can't use the same code as for the other file types!
    for var in its_vars:
        if var == 'Total Cumulative Energy' or var == 'Average_Energy':
            use_number = False
        else:
            use_number = True
        create_nc_one_variable(nc_out, this_dict, var, 'pmp', use_number)


def create_nc_file(filename_out, full_file_info, serial_no):
    '''Create the netCDF file for one float with all dimensions and variables.'''
    print(f'Creating {filename_out}') 
    try:
        nc_out = nc.Dataset(filename_out, 'w', format='NETCDF4')
        nc_out.history = 'Created with parse_eng_bgcsolo.py on ' + \
            time.ctime(time.time())
        nc_out.update = time.ctime(time.time())
        nc_out.createDimension('N_PROF', None) # unlimited dim
        nc_out.createDimension('STRING8', 8)
        nc_out.createDimension('STRING16', 16)
        # time-independent variables
        nc_out.createVariable('floatid', 'i4', (), fill_value=-1)
        nc_out.createVariable('WMOID', 'i4', (), fill_value=-1)
        nc_out.createVariable('AOMLID', 'i4', (), fill_value=-1)
        nc_out.createVariable('Program', 'S1', ('STRING8'), fill_value='')
        nc_out.createVariable('Float_Type', 'S1',
                              ('STRING16'), fill_value='') # FIXME
        lon = nc_out.createVariable('LONGITUDE', 'f4', ('N_PROF'),
                              fill_value=-999.0)
        lon.standard_name = 'longitude'
        lon.units = 'degrees_east'
        lat = nc_out.createVariable('LATITUDE', 'f4', ('N_PROF'),
                              fill_value=-999.0)
        lat.standard_name = 'latitude'
        lat.units = 'degrees_north'

        for ftype in full_file_info.keys():
            print(f'Processing {ftype} file') #DEBUG
            if ftype == 'pmp':
                create_nc_pmp_vars(nc_out, full_file_info[ftype])
            else:
                this_dict = full_file_info[ftype]
                its_vars = this_dict.keys()
                for var in its_vars:
                    create_nc_one_variable(nc_out, this_dict, var, ftype)

        this_row = FLOAT_INFO[FLOAT_INFO.Float_ID == serial_no]
        nc_out['floatid'][:] = serial_no
        row_idx = this_row.index[0]
        nc_out['WMOID'][:] = this_row.at[row_idx,'WMOID']
        nc_out['AOMLID'][:] = this_row.at[row_idx,'AOML_ID']
        str_out = this_row.at[row_idx,'Float_Type'].ljust(16, '\0')
        nc_out['Float_Type'][:] = nc.stringtochar(np.array(str_out, 'S'))
        str_out = this_row.at[row_idx,'Program'].ljust(8, '\0')
        nc_out['Program'][:] = nc.stringtochar(np.array(str_out, 'S'))
        nc_out.close()
        return True # success
    except Exception as exc:
        print(f'An error occurred while writing file {filename_out}')
        print(type(exc))
        print(exc)
        pdb.set_trace()
        return False

def write_pmp_nc(nc_out, file_info_pmp):
    '''Write the pmp-related information to the netCDF file.'''
    its_cycles = list(file_info_pmp.keys())
    n_cycles = len(its_cycles)
    out_vars = {'time': 'JULD',
                'Total Cumulative Energy': 'pmp_Total_Cumulative_Energy',
                'Average_Energy': 'pmp_Average_Energy'}

    while its_cycles:
        min_cycle = min(its_cycles)
        file_cycles = nc_out['Cycle'][:]
        if min_cycle not in file_cycles:
            cyc_idx = len(file_cycles)
            nc_out['Cycle'][cyc_idx] = min_cycle
        else:
            cyc_idx = np.where(file_cycles == min_cycle)[0].item()
        for var in file_info_pmp[min_cycle].keys():
            values = file_info_pmp[min_cycle][var]
            if var in out_vars.keys():
                nc_out[out_vars[var]][cyc_idx] = values
                continue
            n_values = len(values)
            if var == 'Number':
                file_numbers = nc_out['pmp_Number'][cyc_idx][:]
                if not np.ma.is_masked(file_numbers[0]):
                    print('why is Number set already?')
                    pdb.set_trace()
            nc_var = get_var_name_units_nc(var)[0]
            full_var = f'pmp_{nc_var}'
            nc_out[full_var][cyc_idx,0:n_values] = values
        its_cycles.remove(min_cycle)


def write_nc_file(filename_out, full_file_info):
    '''Write all information to the netCDF file for one float.'''
    print(f'Writing to {filename_out}')
    nc_out = nc.Dataset(filename_out, 'a')
    ftypes = full_file_info.keys()
    for ftype in ftypes:
        if ftype == 'pmp':
            write_pmp_nc(nc_out, full_file_info[ftype])
            continue
        if 'Cycle' not in full_file_info[ftype].keys():
            print(f'"Cycle" not found in "{ftype}", cannot process!')
            pdb.set_trace()
            continue
        its_cycles = full_file_info[ftype]['Cycle']
        n_cycles = len(its_cycles)
        vars = full_file_info[ftype].keys()
        #success = True
        for var in vars:
            if len(full_file_info[ftype][var]) != n_cycles:
                #DEBUG print(f'Variable "{var}" has unexpected length in "{ftype}"!')
                if len(full_file_info[ftype][var]):
                    if isinstance(full_file_info[ftype][var][0], int):
                        fill_val = -999
                    elif isinstance(full_file_info[ftype][var][0], float):
                        fill_val = -999.0
                    else:
                        print('is it a string?')
                        pdb.set_trace()
                for i in range(len(full_file_info[ftype][var]), n_cycles):
                    full_file_info[ftype][var].append(fill_val)
        #if not success:
        #    continue # do not process this file's variables
        while its_cycles:
            min_cycle = min(its_cycles)
            min_cycle_index = its_cycles.index(min_cycle)
            file_cycles = nc_out['Cycle'][:]
            if min_cycle not in file_cycles:
                cyc_idx = len(file_cycles)
                nc_out['Cycle'][cyc_idx] = min_cycle
            else:
                cyc_idx = np.where(file_cycles == min_cycle)[0].item()
            for var in vars:
                if var == 'Cycle':
                    continue
                else:
                    nc_var = get_var_name_units_nc(var)[0]
                full_var = f'{ftype}_{nc_var}'
                nc_out[full_var][cyc_idx] = \
                    full_file_info[ftype][var][min_cycle_index]
                # create copies for standardized lon/lat variables
                if full_var == 'gps_First_Lat':
                    nc_out['LATITUDE'][cyc_idx] = \
                        full_file_info[ftype][var][min_cycle_index]
                elif full_var == 'gps_First_Long':
                    nc_out['LONGITUDE'][cyc_idx] = \
                        full_file_info[ftype][var][min_cycle_index]
            its_cycles.remove(min_cycle)
    nc_out.close()


def process_float(serial_no):
    '''Process the files for one float.
    serial_no: serial number (string)
    Pre: serial_no must be listed in floats.csv.'''
    print(f'Processing {serial_no}')
    # there is also <serial_no>_pdump.html! FIXME

    # dictionary with ftype as keys and dictionaries as values
    full_file_info = {}
    for ftype in FILE_TYPES:
        file_name = f'{ftype}_{DICT_FLOAT_IDS[serial_no][0]}.html'
        file_path = f'{ARGS.directory_in}/{file_name}'
        full_file_info[ftype] = parse_file(file_path, ftype, serial_no)

    # create netCDF file with full_file_info
    fn_nc = f'{ARGS.directory_out}/eng_ps{serial_no}.nc'
    if not create_nc_file(fn_nc, full_file_info, serial_no):
        print('failed to create the file')
        return

    # no, not so simple! should have another function that writes
    # one cycle, needs to go through what's available across
    # variables
    write_nc_file(fn_nc, full_file_info)
    

def get_float_ids(filename):
    '''Read float information from the given csv file, extract the
    internal (serial number) and external (WMO) IDs from the appropriate columns
    and return a dictionary with the internal IDs as keys and the WMO IDs
    as the values.'''
    float_info = pd.read_csv(filename)
    float_ids = float_info['Float_ID'].values
    aoml_ids = float_info['AOML_ID'].values
    wmo_ids = float_info['WMOID'].values
    result_dict = {key: (val1, val2) for key, val1, val2 in
            zip(float_ids, wmo_ids, aoml_ids)}
    return result_dict, float_info


def parse_input_args():
    '''Parse the command line arguments and return them as an object.'''
    parser = argparse.ArgumentParser()
    # required arguments:
    parser.add_argument('csv_file', help='name of the input csv file')
    parser.add_argument('directory_in', help='name of the input directory')
    parser.add_argument('directory_out', help='name of the output directory')
    # option:
    #parser.add_argument('--fn_out', default = None,
    #                    help='name of the output file')
    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_input_args()
    # contents of this dictionary: dict_float_ids[serial_number] = (wmoid, aomlid)
    DICT_FLOAT_IDS, FLOAT_INFO = get_float_ids(ARGS.csv_file)
    for sn in DICT_FLOAT_IDS:
        process_float(sn)

