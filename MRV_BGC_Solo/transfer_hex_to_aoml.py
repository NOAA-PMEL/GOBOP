#!/home/argotest/anaconda3/bin/python

'''
transfer_hex_to_aoml.py: This script checks if new raw data from
MRV BGC-Solo floats have come in. If so, the *.rudics files will be
converted to *hex format, and a gzipped version of the hex file
will be uploaded to specified ftp servers.
'''

import argparse
import datetime
import ftplib
import glob
import hashlib
import json
import os
import re
import subprocess
import time
import shutil
import pandas as pd

SERVER_JSON = 'ftpserver.json'
BASE_PATH = '/var/rudics-store/PlatformDir/'
TIME_GAP = 300 # seconds to wait before processing latest files
CMD_R2H = '/home/argotest/rudics/server/rudics-rs/target/release/rudics2hex'
FTP_NAMES = []
FTP_DIRS = []
FTP_HOSTS = []
FTP_USERS = []
FTP_PW = []


def get_float_ids(filename):
    '''Read float information from the given csv file, extract the
    internal (serial number) and external (WMO) IDs from the appropriate columns
    and return a dictionary with the internal IDs as keys and the WMO IDs
    as the values.'''
    float_info = pd.read_csv(filename)
    internal_ids = float_info['Float ID'].values
    wmo_ids = float_info['Float WMO'].values
    aoml_ids = float_info['AOML'].values
    result_dict = {key: (val1, val2) for key, val1, val2 in
            zip(internal_ids, wmo_ids, aoml_ids)}
    return result_dict


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

    with open(filename, 'rb') as f_ptr:
        bytes_read = f_ptr.read()  # read file as bytes
        if hash_function == 'sha256':
            readable_hash = hashlib.sha256(bytes_read).hexdigest()
        elif hash_function == 'md5':
            readable_hash = hashlib.md5(bytes_read).hexdigest()
        else:
            raise(f'{hash_function} is an invalid hash function.' +
                  'Please use md5 or sha256')

    return readable_hash

def parse_filename(filename):
    '''This function splits the given filename, which may include the
    full path, into path and filename, and further splits the filename into
    floatid, profile, and file type ('S', '0', 'E', 'K').
    Returns file name (without the path), floatid (as int), profile (as int),
    and file type.'''
    fname = os.path.basename(filename) # without the path
    # split by both _ and .
    parts = re.split(r'_|\.', fname)
    if parts[-1] != 'rudics':
        print(f'WARNING: File "{fname}" could not be parsed!')
        return fname, -999, -999, -999, -999, 'X'
    floatid = int(parts[0].replace('sn', ''))
    transmission = int(parts[1].replace('s', ''))
    ftype = parts[-2]
    if ftype == 'K':
        block = -999
        cycle = -999
    else:
        block = int(parts[-3].replace('b', ''))
        cycle = int(parts[-4].replace('d', ''))
    return fname, floatid, transmission, cycle, block, ftype

def check_conv_need_file(filename, log):
    '''This function checks if the file with the given name
    has been processed before.
    If so, it checks if the file is identical to the previously
    processed file. Returns True in that case, False otherwise.'''
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
    return True


def create_log_file(filename_log):
    '''Create the file that logs which raw Argo files have been
    processed yet. Raise an IOError if the file cannot be created.'''
    try:
        with open(filename_log, 'w', encoding='utf-8') as file:
            file.write('Filename,FloatID,WMOID,Type,Transmission,Cycle,Block,')
            file.write('Size,Checksum,Processing_date\n')
    except Exception as exc:
        raise IOError(f'ERROR: Could not create "{filename_log}"!') from exc


def create_ftp_log_file(filename_ftp_log):
    '''Create the file that logs which hex files have been
    uploaded to AOML yet. Raise an IOError if the file cannot be created.'''
    try:
        with open(filename_ftp_log, 'w', encoding='utf-8') as file:
            file.write('Filename,Checksum,Size,Upload_date\n')
    except Exception as exc:
        raise IOError(f'ERROR: Could not create "{filename_ftp_log}"!') from exc


def mark_file_processed(filename, fn_log):
    '''Add the file with the given filename to the list of files that
    have been processed, including information about it, including
    the given file_type, its size, and its checksum.
    Information is written to the log file with the given name. If that file
    doesn't exist yet, it will be created.'''
    # extract internal ID, profile etc. from filename
    _, floatid, trans, cycle, block, ftype = parse_filename(filename)
    wmoid = DICT_FLOAT_IDS[floatid][0]
    shasum = get_checksum(filename)
    size = os.path.getsize(filename)
    now = datetime.datetime.now()
    with open(fn_log, 'a', encoding='utf-8') as f_log:
        f_log.write(f'{filename},{floatid},{wmoid},{ftype},{trans},{cycle},')
        f_log.write(f'{block},{size},{shasum},')
        f_log.write(f'{now.strftime("%Y/%m/%d %H:%M:%S")}\n')

def sort_files_mtime(file_list):
    '''Sort the given list of files in ascending order of their modification
    times. Return the sorted list and the last mtime value.'''
    mtime_files = {} # keys will be the mtimes, values the file names
    last_mtime = -1
    for file in file_list:
        mtime = os.path.getmtime(file)
        mtime_files[mtime] = file
        if mtime > last_mtime:
            last_mtime = mtime
    sorted_mtimes = sorted(mtime_files.keys())
    sorted_files = [mtime_files[mtime] for mtime in sorted_mtimes]
    return sorted_files, last_mtime


def read_server_info():
    '''Read the information about the ftp server (name, account, and password)
    from the file with the globally defined name.
    Return the information as a dictionary.'''
    with open(SERVER_JSON, 'r', encoding='utf-8') as file:
        info = json.load(file)
    return info


def connect_ftp(server, acct, passwd, secure=False):
    '''Connect to the specified ftp server with the given account name
    and password. Use secure protocol if set. Returns an ftp object
    if successful. Throws an ftplib exception if the connection cannot be made.'''
    # note that as of May 2024, the connection to AOML must not be secure
    if secure:
        ftp_server = ftplib.FTP_TLS()
        ftp_server.connect(server, timeout=120)
        ftp_server.login(acct, passwd)
        ftp_server.prot_p()
    else:
        ftp_server = ftplib.FTP(server, acct, passwd)
    return ftp_server


def upload_hex_ftp(fn_hex, fn_ftp_log, destination=None):
    '''Upload the hex file with the specified name to the specified ftp server.'''
    # HF 05/16/2024: Claudia Schmid requested files to be uploaded as
    # e.g., 9674_004005.hex.gz
    ser_no = fn_hex.replace('.hex','').replace('hex/','') # FIXME regex
    aoml_id = DICT_FLOAT_IDS[int(ser_no)][1]
    fn_aoml = f'{aoml_id}_{int(ser_no):06}.hex'
    fn_gzip = f'hex/upload/{fn_aoml}' # FIXME path hard-coded here
    shutil.copy(fn_hex, fn_gzip)
    cmd = ['gzip', '-f', fn_gzip] # force: overwrite existing .gz file
    result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True)
    if result.returncode:
        print('WARNING: gzip command may have failed!')
    fn_gzip += '.gz'
    if not destination:
        dest = FTP_NAMES
    else:
        dest = [destination]
    for dst in dest:
        print(f'Now uploading to {dst}: {fn_gzip}')
        idx = FTP_NAMES.index(dst)
        success = False
        try:
            ftp_server = connect_ftp(FTP_HOSTS[idx], FTP_USERS[idx], FTP_PW[idx])
            ftp_server.cwd(FTP_DIRS[idx])
        except ftplib.all_errors:
            print('Warning: connection to ftp server could not be established')
            return
        try:
            with open(fn_gzip, 'rb') as file:
                print(f'opened {fn_gzip}')
                fname = os.path.basename(fn_gzip) # without the path
                store_cmd = f'STOR {fname}'
                ftp_server.storbinary(store_cmd, file)
            success = True
            ftp_server.retrlines('LIST') # FIXME shows dir listing, remove eventually!
        except OSError:
            print(f'could not read {fn_gzip}')
        except ftplib.all_errors:
            print(f'could not upload {fn_gzip}')
        ftp_server.quit()
        if success:
            shasum = get_checksum(fn_hex)
            size = os.path.getsize(fn_hex)
            now = datetime.datetime.now()
            with open(fn_ftp_log, 'a', encoding='utf-8') as f_log:
                f_log.write(f'{fn_hex},{shasum},{size},{dst},{now}\n')


def wait_transmission_complete(serial_no, log):
    '''If RUDICS files are currently coming in, wait until the full
    set of files has been retrieved. Allow a time gap before
    starting the processing.
    Return a sorted list of new (not previously processed) files.'''
    is_new = True
    while is_new:
        rudics_files = glob.glob(f'{BASE_PATH}{serial_no}/inbox/*.rudics')
        if ARGS.verbose:
            print(f'{len(rudics_files)} *.rudics files found in the inbox')
        new_rudics_files = []
        for file in rudics_files:
            do_process = check_conv_need_file(file, log)
            if do_process:
                new_rudics_files.append(file)
        sorted_new_files, latest_mtime = sort_files_mtime(new_rudics_files)
        now = time.time()
        if now - latest_mtime > TIME_GAP:
            is_new = False
        else:
            print('Waiting a bit, more files may come in...') # VERBOSE
            time.sleep(TIME_GAP - now + latest_mtime)
    return sorted_new_files


def process_float(serial_no):
    '''Process the files for one float.
    serial_no: serial number (string)
    Pre: serial_no must be listed in floats.csv.'''
    fn_log = f'rudics2hex_{serial_no}.log'
    if not os.path.exists(fn_log):
        create_log_file(fn_log)
    fn_ftp_log = f'ftp_{serial_no}.log'
    if not os.path.exists(fn_ftp_log):
        create_ftp_log_file(fn_ftp_log)
    log = pd.read_csv(fn_log)
    ftp_log = pd.read_csv(fn_ftp_log)
    if ARGS.verbose:
        print(f'Processing files for float {serial_no}')
    fn_hex = f'hex/{serial_no}.hex'
    # wait until a set of transmissions is completed - allow a time gap
    # before actually processing them
    sorted_new_files = wait_transmission_complete(serial_no, log)
    #FIXME unneeded? wmoid = DICT_FLOAT_IDS[int(serial_no)][0]
    for file in sorted_new_files:
        full_cmd = [CMD_R2H, '-vvv', '--output', 'hex', 'append', file]
        result = subprocess.run(full_cmd, stdout=subprocess.PIPE, check=True)
        print(result.stdout) # stdout can be redirected to file by user
        if result.returncode:
            print('WARNING: rudics2hex command may have failed!')
        mark_file_processed(file, fn_log)
    if ARGS.no_transfer:
        return
    # upload the hex file only if it was changed or missing on ftp server(s)
    if sorted_new_files:
        upload_hex_ftp(fn_hex, fn_ftp_log) # upload to both servers
    else:
        # even if no new files were created, we should check if a previously
        # generated hex file was successfully uploaded (an ftp server may
        # have been down, for instance)
        shasum = get_checksum(fn_hex)
        rows_log = ftp_log.loc[ftp_log['Filename'] == fn_hex]
        if rows_log.empty: # file not listed in ftp log
            upload_hex_ftp(fn_hex, fn_ftp_log) # upload to both servers
        else:
            for host in FTP_NAMES:
                rows_host = rows_log[rows_log['Destination'] == host]
                if rows_host.empty:
                    upload_hex_ftp(fn_hex, fn_ftp_log, host)
                else:
                    shasum_ftp = rows_host['Checksum'].values[-1] # most recent upload
                    if shasum != shasum_ftp:
                        upload_hex_ftp(fn_hex, fn_ftp_log, host)


def parse_input_args():
    '''Parse the command line arguments and return them as an object.'''
    parser = argparse.ArgumentParser(description='Convert RUDICS to hex files ' +
                                     'for MRV BGC Solo floats and upload them to DAC')
    # required argument:
    parser.add_argument('csv_file',
                        help='name of the csv file with float information')
    # options
    parser.add_argument('-T', '--no_transfer', default=False, action='store_true',
                        help='process only, do not transfer to AOML')
    parser.add_argument('-v', '--verbose', default=False, action='store_true',
                        help='if set, display more progress updates')

    return parser.parse_args()

if __name__ == '__main__':
    ARGS = parse_input_args()
    # contents of this dictionary: dict_float_ids[internal_id] = wmoid
    DICT_FLOAT_IDS = get_float_ids(ARGS.csv_file)
    for sn in DICT_FLOAT_IDS:
        process_float(sn)
