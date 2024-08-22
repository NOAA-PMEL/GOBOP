#!/home/argoserver/anaconda3/bin/python

''' download_raw_navis.py: This script downloads raw files for one BGC Navis Argo
 float from PMEL's ftp server. No further processing is done by this script.
 It is intended to be run as a cron job, typically several times a day.
 This is only necessary for files that did not go directly to the float's account
 on argoserver.

 Requirement:
   A text file "ftpserver.json" must exist in the current working directory.
   It must contain the information about the ftp server in plain text.

 H. Frenzel, CICOES, University of Washington // NOAA-PMEL

 Latest version: August 21, 2024
 First version:  September 1, 2023
'''

import argparse
import calendar
import ftplib
import glob
import hashlib
import json
import os
import re
import shutil
import time
from datetime import datetime


SERVER_JSON = 'ftpserver.json'
FTP_DOWNLOADS = 'ftp_downloads.txt'
MISSING_FILES = 'missing_files.txt'
FTP_DIR = 'FTP'
TYPES = ['msg', 'log', 'isus']
SEC_PER_DAY = 86400


def change_cwd(dir1):
    '''Change to the specified directory, unless it is None.'''
    if dir1:
        os.chdir(dir1)


def check_dir(dir1):
    '''Check if subdirectory with the given name (dir1) exists.
    Create it if not. Return True if it existed before or could
    be created, False if it could not be created.'''
    if not os.path.isdir(dir1):
        try:
            os.mkdir(dir1)
            if ARGS.verbose:
                print(f'Created subdirectory "{dir1}"')
        except (FileExistsError, OSError):
            return False
    return True

def read_text_file(filename):
    '''Read the file with the given filename and return its lines as a list
    of strings. None is returned if the file could not be opened or read.'''
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            lines = file.read().splitlines()
    except OSError:
        print(f'File "{filename}" could not be read!')
        return None
    return lines

def read_server_info():
    '''Read the information about the ftp server (name, account, and password)
    from the file with the globally defined name.
    Return the information as a dictionary.'''
    with open(SERVER_JSON, 'r', encoding='utf-8') as file:
        info = json.load(file)
    return info

def connect_ftp(server_info, secure=True):
    '''Connect to the specified ftp server. Use secure protocol if set.
    Returns an ftp object if successful.
    Throws an ftplib exception if the connection cannot be made.'''
    if secure:
        ftp_server = ftplib.FTP_TLS()
        ftp_server.connect(server_info['server'], timeout=120)
        ftp_server.login(server_info['account'], server_info['password'])
        ftp_server.prot_p()
    else:
        ftp_server = ftplib.FTP(server_info['server'], server_info['account'],
                                server_info['password'])
    return ftp_server


def parse_lines(lines, num_to_skip=0):
    '''Parse the lines that were read from the ftp downloads file.
    Skip the first "num_to_skip" lines.
    Return a dictionary with the file names as keys and their mtimes 
    (month,day,year) and file sizes as values.'''
    entries = {}
    for i in range(num_to_skip, len(lines)):
        parts = lines[i].split(',')
        if len(parts) != 5:
            raise IOError(f'File {FTP_DOWNLOADS} could not be read correctly!')
        entries[parts[0]] = parts[1:]
    return entries


def create_ftp_downloads():
    '''Create the file that tracks the ftp downloads for one float and
    write its headerline.'''
    with open(FTP_DOWNLOADS, 'w', encoding='utf-8') as f_ptr:
        f_ptr.write('File,Size,Month,Day,Year\n')


def get_ftp_listings(ftp_server):
    '''Get a list of all files that are available on the ftp server.
    Return a dictionary with file names as keys and file sizes and 
    dates as values.
    Raises an ftplib exception if the connection to the ftp server
    cannot be established.'''
    all_files = []
    ftp_server.dir(f'{ARGS.float_id}*', all_files.append)
    entries = {}
    for entry in all_files:
        parts = entry.split()
        [size, month, day, year_or_time, name] = parts[4:]
        all_months = {month: index for index, month in
                      enumerate(calendar.month_abbr) if month}
        # the time entry does not need to be kept as it does
        # not help in comparison: once the listing switches from
        # the listing with time to the listing with year, it is
        # impossible to know what the time value is
        if ':' in year_or_time:
            file_month = int(all_months[month])
            today = datetime.today()
            date_month = today.month
            if file_month <= date_month:
                year = str(today.year) # convert for consistency
            else:
                year = str(today.year - 1)
        else:
            year = year_or_time
        entries[name] = [size, month, day, year]
    return entries


def get_prev_downloaded(path='.'):
    '''Retrieve information from file <FTP_DOWNLOADS> if it exists,
    and return file information as a dictionary.'''
    file_lines = read_text_file(path + '/' + FTP_DOWNLOADS)
    if file_lines:
        prev_downloaded = parse_lines(file_lines,1)
    else: # file doesn't exist at all yet
        create_ftp_downloads()
        prev_downloaded = []
    return prev_downloaded


def download_files_ftp():
    '''Download all files for this float that are available on the ftp server
    and have not yet been downloaded before (with the same date and file size).
    Return a list of newly downloaded files.'''
    check_dir(FTP_DIR)
    change_cwd(FTP_DIR)

    server_info = read_server_info()
    ftp_server = connect_ftp(server_info)
    downloaded_files = []
    prev_downloaded = get_prev_downloaded()
    try:
        available_files = get_ftp_listings(ftp_server)
        with open(FTP_DOWNLOADS, 'a', encoding='utf-8') as file_out:
            for filename, stats in available_files.items():
                if ARGS.verbose:
                    print(f'Processing {filename}')
                do_download = False # default
                if not os.path.exists(filename):
                    do_download = True
                else:
                    if ARGS.verbose:
                        print(f'Downloaded {filename} from ftp before')
                    if filename in prev_downloaded:
                        is_ident = [a == b for a, b in
                                    zip(prev_downloaded[filename], stats)]
                        if not all(is_ident):
                            print('Versions differ, downloading again')
                            do_download = True
                    else: # just in case - this should not happen!
                        print(f'WARNING: {filename} was downloaded before,')
                        print(f'but is not listed in {FTP_DOWNLOADS}.')
                        print('It will be downloaded again.')
                        do_download = True
                if do_download:
                    if ARGS.verbose:
                        print(f'Downloading {filename} from ftp')
                    with open(filename, 'wb') as file:
                        ftp_server.retrbinary(f'RETR {filename}', file.write)
                    file_out.write(f'{filename},{stats[0]},{stats[1]},' +
                                   f'{stats[2]},{stats[3]}\n')
                    downloaded_files.append(filename)
        ftp_server.quit()
    except ftplib.all_errors:
        print('Warning: connection to ftp server could not be established')
        return downloaded_files

    if ARGS.verbose:
        print('These files were downloaded:')
        print(downloaded_files)
    return downloaded_files


def get_hash(filename):
    '''Get and return the sha256sum value of the file with the given name.
    Pre: File must exist.'''
    with open(filename, 'rb') as f_ptr:
        digest = hashlib.file_digest(f_ptr, 'sha256')
        hash_value = digest.hexdigest()
    return hash_value


def get_epoch_time(month, day, year):
    '''Get the UNIX/epoch time for midnight of the specified date.
    Input values are strings, with month in literal format, e.g., 'Jan'.
    Return the epoch time (float).'''
    date_str = f'{day} {month} {year}'
    date_obj = datetime.strptime(date_str, '%d %b %Y')
    return time.mktime(date_obj.timetuple())


def check_files_ftp(files_ftp):
    '''Check if the files that were downloaded from the ftp server exist
    in the main directory already. If not, move ftp file to the main directory.
    If both versions are identical, delete the one in the ftp subdirectory.
    If the one in the ftp subdirectory is larger, rename the one in the main
    directory (add ".N", where N is the number of bytes, to the file name)
    and move the larger file from the subdirectory to the main directory.
    If the one in the main directory is larger, keep the one in the
    subdirectory as is.
    If both files are the same size, compare hashes. If they are identical,
    remove the file in the ftp subdirectory, otherwise keep it.
    Pre: cwd must be the main directory.
    Return True if new files exist, False otherwise.'''
    prev_downloaded = get_prev_downloaded(FTP_DIR)
    new_files = False
    for filename in files_ftp:
        if ARGS.verbose:
            print(f'checking {filename}')
        fn_ftp = f'{FTP_DIR}/{filename}'
        if not os.path.exists(filename):
            print(f'COPYING: {fn_ftp} TO {filename}')
            shutil.copy2(fn_ftp, filename)
            new_files = True
        else: # file exists in main and subdirectory
            # mtime values are more important than file sizes
            # there may be files from dock testing with the
            # same name as files from actual profiles
            size_main = os.path.getsize(filename)
            size_ftp = os.path.getsize(fn_ftp)
            mtime_main = os.path.getmtime(filename)
            mtime_ftp = get_epoch_time(prev_downloaded[filename][1],
                                       prev_downloaded[filename][2],
                                       prev_downloaded[filename][3])
            if (size_ftp == size_main and
                get_hash(filename) == get_hash(fn_ftp)):
                print(f'IDENTICAL FILE, NOT USING: {fn_ftp}')
            elif mtime_main > mtime_ftp + SEC_PER_DAY:
                # for older files, only dates are known, not times
                # so keep 1 day as cushion
                print(f'FTP file is older: {fn_ftp}')
            elif size_ftp > size_main:
                # ftp version is larger, keep that one
                fn_new = f'{filename}.{size_main}'
                print(f'RENAMING: {filename} TO {fn_new}')
                os.rename(filename, fn_new)
                print(f'KEEPING FILE FROM FTP: {fn_ftp}')
                shutil.copy2(fn_ftp, filename)
                new_files = True
            else:
                # if ftp version is smaller, keep it as is
                print('FTP VERSION IS SMALLER, KEEP CURRENT FILE')
    return new_files


def backup_missing_files():
    '''If there is a current version of {MISSING_FILES}, create a backup
    file. Start by adding ".BAK" to the name, if that is taken already,
    add successive numbers, starting with 1, until an unused file name
    is found.
    Return the name of the backed up file. Return None if the missing files
    file doesn't exist.'''
    if not os.path.exists(MISSING_FILES):
        return None
    # find a non-existing file name
    if not os.path.exists(f'{MISSING_FILES}.BAK'):
        fn_backup = f'{MISSING_FILES}.BAK'
    else:
        index = 1
        fn_backup = f'{MISSING_FILES}.BAK{index}'
        while os.path.exists(fn_backup):
            index += 1
            fn_backup = f'{MISSING_FILES}.BAK{index}'
    print(f'RENAMING: {MISSING_FILES} -> {fn_backup}')
    os.rename(MISSING_FILES, fn_backup)
    return fn_backup


def find_latest_profile(float_id):
    ''' Determine the highest profile index among all files.
    The highest profile index is taken from any of the standard
    file types, and it will be returned.'''
    highest = -1
    regex = re.compile(r'\w+\.(\d+)\.\w+')
    for ftype in TYPES:
        # do not use "*" as the pattern for the profile index
        # there are log files with very large numbers in that place
        pattern = f'{float_id}.???.{ftype}'
        files_type = glob.glob(pattern)
        for filename in files_type:
            match_obj = regex.search(filename)
            if match_obj:
                index = int(match_obj.group(1))
                if index > highest:
                    highest = index
            else:
                print(f'WEIRD FILE NAME: {filename}')
    return highest

def write_missing_files(filenames_missing):
    ''' Create a file named {MISSING_FILES} and write the missing
    file names into it, one per line).'''
    try:
        with open(MISSING_FILES, 'w', encoding='utf-8') as file:
            for filename in filenames_missing:
                file.write(f'{filename}\n')
    except OSError:
        print(f'Could not create or write to "{MISSING_FILES}"')


def determine_missing_files(float_id, highest):
    '''Given the highest found profile index, determine which
    files are mssing.
    Also create file {MISSING_FILES} if there are missing files.'''
    missing = []
    for index in range(highest+1):
        for ftype in TYPES:
            if index == 0 and ftype == 'isus':
                continue
            filename = f'{float_id}.{index:03d}.{ftype}'
            if not os.path.exists(filename):
                missing.append(filename)
    if missing:
        write_missing_files(missing)


def parse_input_args():
    '''Parse the command line arguments and return them as object.'''
    parser = argparse.ArgumentParser(description='')
    # mandatory argument: float_id (internal, not WMO)
    parser.add_argument('float_id', help='Internal ID of the float')
    # options:
    parser.add_argument('-d', '--directory', default=None, type=str,
                        help='working directory (default: cwd)')
    parser.add_argument('-v', '--verbose', default=False, action="store_true",
                        help='if set, display more progress updates')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    ARGS = parse_input_args()
    change_cwd(ARGS.directory)
    DOWNLOADED_FTP = download_files_ftp()
    change_cwd(ARGS.directory)
    if check_files_ftp(DOWNLOADED_FTP):
        os.system('make -f ./makefile Export')
    FN_BAK_MISSING = backup_missing_files()
    if FN_BAK_MISSING:
        print(f'New name of most recent backup file: {FN_BAK_MISSING}')
    LATEST = find_latest_profile(ARGS.float_id)
    determine_missing_files(ARGS.float_id, LATEST)
