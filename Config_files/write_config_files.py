#!/usr/local/anaconda3/bin/python
#!/home/argoserver/anaconda3/bin/python
#
# Author: H. Frenzel, CICOES, UW // NOAA-PMEL
#
# First version: December 13, 2023

'''
This script reads in a spreadsheet that contains metadata for MRV Argo floats,
typically downloaded from Google Drive.
It then creates configuration (.cfg) files either for all floats listed
in the spreadsheet or only the optionally specified one.
'''

import argparse
import re
import pandas as pd

def check_wmo(table, wmoid):
    '''Check if the float with the specified WMO ID is present in
    the spreadsheet. Raises a ValueError if not.'''
    matching_float = table[table['WMO'] == wmoid]
    if matching_float.empty:
        raise ValueError('Float with specified WMO not present in spreadsheet')


def read_spreadsheet(fn_spread):
    '''Read the information from the spreadsheet file
    with the given name.
    Return the information as a DataFrame.'''
    table = pd.read_excel(fn_spread)
    # convert some columns from float to int
    int_cols = ['serialNumber', 'AOML', 'WMO', 'pressureSensorSerialNumber',
                'CTDSerialNumber', 'IMEI']
    for col in int_cols:
        if any(table.loc[:,col].isna()):
            print(f'\nWARNING: missing values found in column "{col}"!\n')
        else:
            table[col] = table[col].astype('int64')

    return table


def get_inst_types(float_model, rom_version):
    '''Assign DAC and WMO instrument type, data format, and
    float version based on the float model and ROM version.'''
    if float_model.lower().strip() == 'alto':
        dac_inst_type = 'AltoIR_T1'
        wmo_inst_type = 876 # FIXME note that this depends on CTD, SBE assumed
        regex_alto = re.compile(r'([\d\.]+)[\+\w]')
        match_obj = regex_alto.search(rom_version)
        data_format = match_obj.group(1)
        float_version = 3 # FIXME is this documented somewhere?
    elif float_model.lower().strip() == 's2a':
        dac_inst_type = 'SOLO2IR_TS9'
        wmo_inst_type = 854 # FIXME note that this depends on CTD, SBE assumed
        regex_s2a = re.compile(r'SBE\d+\s+\d+\w+\d+\s+ARM\s+[vV]([\d\.]+)')
        match_obj = regex_s2a.search(rom_version)
        data_format = match_obj.group(1)
        float_version = 0 # FIXME is this documented somewhere?
    else:
        raise ValueError(f'Unknown float type: {float_model}')
    return dac_inst_type, wmo_inst_type, data_format, float_version


def create_output_files(table):
    '''Create output files from the given values
    in the table. Output files are created for floats that
    have WMO defined, unless wmo is specified as a 
    command line argument.'''
    if ARGS.wmo < 0:
        floats = table[(table['WMO'] > 1e4)]
    else:
        check_wmo(table, ARGS.wmo)
        floats = table[table['WMO'] == ARGS.wmo]
    for serial_number in floats['serialNumber'].values.tolist():
        this_row = table.loc[table['serialNumber'] == serial_number,:]
        dac_inst_type, wmo_inst_type, data_format, float_version = \
            get_inst_types(this_row['model'].values[0],
                           this_row['ROMVersion'].values[0])
        if pd.isnull(this_row['deployed'].values[0]):
            deploy_status = 0
        else:
            deploy_status = 1
        if 'iridium' in this_row['communication'].values[0].lower().strip():
            trans_type = 'IRIDIUM'
        else:
            raise ValueError('Unknown communication type')

        fn_out = f'{serial_number}.cfg'
        if ARGS.verbose:
            print(f'Creating config file {fn_out} for float with S/N {serial_number}')
        entries = {'SN': serial_number, 'Deploy_Status': deploy_status,
                   'IMEI': this_row["IMEI"].values[0],
                   'Trans_Type': trans_type, 'Trans_ID': serial_number,
                   'DAC_ID': this_row["AOML"].values[0],
                   'DAC_Inst_Type': dac_inst_type,
                   'WMO_ID': this_row["WMO"].values[0],
                   'WMO_Inst_Type': wmo_inst_type,
                   'WMO_Recd_Type': 64, 'Data_Format_Version': data_format,
                   'Float_Version': float_version,
                   'PI': 'Gregory C. Johnson', 'Flt_Prov': 'PMEL',
                   'Phy_Source': 'PMEL', 'Upload_flg': 0}
        with open(fn_out, 'w', encoding='utf-8') as f_out:
            for key, value in entries.items():
                f_out.write(f'{key}={value}\n')


def parse_input_args():
    '''Parse the command line arguments and return them as an object.'''
    parser = argparse.ArgumentParser()
    # required arguments:
    parser.add_argument('spreadsheet', help='name of the spreadsheet file')
    # options:
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='if set, display more progress updates')
    parser.add_argument('-w', '--wmo', type=int, default=-999,
                        help='process selected WMO id only')
    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_input_args()
    TABLE = read_spreadsheet(ARGS.spreadsheet)
    create_output_files(TABLE)
