#!/home/argoserver/anaconda3/bin/python

''' download_raw_navis.py: This script downloads raw files for one BGC Navis Argo
 float from PMEL's ftp server. No further processing is done by this script. It is
 intended to be run as a cron job, typically several times a day.
 This is only necessary for files that did not go directly to the float's account
 on argoserver.

 Requirements:
   1) A text file "password.txt" must exist in the current working directory.
      It must contain the password for the ftp server in plain text.
   2) A text file "status_<ID>.txt" must exist, unless
      no files for this float have been downloaded yet.

FIXME
   3) A text file "missing_files.txt" should exist, which keeps track of
      the non-msg files of profiles whose msg files were downloaded successfully.
      If no files were missing before, it will be created. FIXME is this true??

 H. Frenzel, CICOES, University of Washington // NOAA-PMEL

 First version: September 1, 2023
'''

import argparse
import ftplib
import glob
import hashlib
import os
import re


TYPES = ['msg', 'log', 'isus']
SERVER = FTP_SERVER # replace with actual server name
ACCT = ACCOUNT # replace with actual account name
FTP_DIR = 'FTP'
MAX_GAP = 10
FN_MISSING = 'missing_files.txt'


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
        with open(filename) as file:
            lines = file.read().splitlines()
    except OSError:
        print(f'File "{filename}" could not be read!')
        return None
    return lines


def get_latest_index(float_id):
    '''Read the status, i.e., the highest index of the already downloaded files
    for each float. If the status file for a float does not exist yet, -1
    is used as the value. Return the status as an integer.'''
    filename = f'status_{float_id}.txt'
    latest_str = read_text_file(filename)
    if latest_str:
        return int(latest_str[0])
    return -1


def download_files_ftp(passwd, latest):
    '''Download all files for this float that are available on the ftp server.
    Use the maximum gap between indices as a cut-off for trying beyond
    the given latest index and the last file found.
    Return a list of downloaded files.'''
    downloaded_files = []
    try:
        ftp_server = ftplib.FTP(SERVER, ACCT, passwd)
    except ftplib.all_errors:
        print('Warning: connection to ftp server could not be established')
        return downloaded_files
    index = 0
    last_good = -1
    while True:
        for ftype in TYPES:
            if index == 0 and ftype == 'isus':
                continue # there is no '000.isus' file
            filename = f'{ARGS.float_id}.{index:03d}.{ftype}'
            print(filename)
            try:
                with open(filename, 'wb') as file:
                    ftp_server.retrbinary(f'RETR {filename}', file.write)
                last_good = index
                downloaded_files.append(filename)
            except ftplib.all_errors:
                print(f'could not download {filename}')
                # a file of size 0 is created during a failed download
                os.remove(filename)
        index += 1
        # try a few files with higher indices
        if index > latest + MAX_GAP and index > last_good + MAX_GAP:
            break
    return downloaded_files


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
    Pre: cwd must be the main directory.'''
    for filename in files_ftp:
        print(f'checking {filename}')
        fn_ftp = f'{FTP_DIR}/{filename}'
        if not os.path.exists(filename):
            print(f'MOVING: {fn_ftp} TO {filename}')
            os.rename(fn_ftp, filename)
        else: # file exists in main and subdirectory
            # first compare sizes
            size_main = os.path.getsize(filename)
            size_ftp = os.path.getsize(fn_ftp)
            if size_ftp > size_main:
                # ftp version is larger, keep that one
                fn_new = f'{filename}.{size_main}'
                print(f'RENAMING: {filename} TO {fn_new}')
                os.rename(filename, fn_new)
                print(f'KEEPING: {fn_ftp}')
                os.rename(fn_ftp, filename)
            elif size_ftp == size_main:
                # hash both files
                with open(filename, 'rb') as file:
                    digest = hashlib.file_digest(file, 'sha256')
                    hash_main = digest.hexdigest()
                    print(f'hash main: {hash_main}')
                with open(fn_ftp, 'rb') as file:
                    digest = hashlib.file_digest(file, 'sha256')
                    hash_ftp = digest.hexdigest()
                    print(f'hash ftp : {hash_ftp}')
                if hash_main == hash_ftp:
                    print(f'REMOVING: {fn_ftp}')
                    os.remove(fn_ftp)
                else:
                    print('KEEPING BOTH!')
            else:
                # if ftp version is smaller, keep it as is
                print('FTP VERSION IS SMALLER, KEEP IT')


def backup_missing_files():
    '''If there is a current version of "missing_files.txt", create a backup
    file. Start by adding ".BAK" to the name, if that is taken already,
    add successive numbers, starting with 1, until an unused file name
    is found.
    Return the name of the backed up file. Return None if the missing files
    file doesn't exist.'''
    if not os.path.exists(FN_MISSING):
        return None
    # find a non-existing file name
    if not os.path.exists(f'{FN_MISSING}.BAK'):
        fn_backup = f'{FN_MISSING}.BAK'
    else:
        index = 1
        fn_backup = f'{FN_MISSING}.BAK{index}'
        while os.path.exists(fn_backup):
            index += 1
            fn_backup = f'{FN_MISSING}.BAK{index}'
    print(f'RENAMING: {FN_MISSING} -> {fn_backup}')
    os.rename(FN_MISSING, fn_backup)
    return fn_backup


def write_status_file(float_id, index):
    '''Create (overwrite if necessary) a file named
    "status_{float_id}.txt that contains the given index
    as its only line.'''
    try:
        filename_status = f'status_{float_id}.txt'
        with open(filename_status, 'w+') as sfile:
            sfile.write(f'{index}\n')
    except OSError:
        print(f'Could not write status file {filename_status}')


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
                    print(f'highest now: {highest}')
            else:
                print('WEIRD FILE NAME: {filename}')
    write_status_file(float_id, highest)
    return highest


def write_missing_files(filenames_missing):
    ''' Create a file named {FN_MISSING} and write the missing
    file names into it, one per line).'''
    try:
        with open(FN_MISSING, 'w') as file:
            for filename in filenames_missing:
                file.write(f'{filename}\n')
    except OSError:
        print(f'Could not create or write to "{FN_MISSING}"')


def determine_missing_files(float_id, highest):
    '''Given the highest found profile index, determine which
    files are mssing.
    Also create file {FN_MISSING} if there are missing files.'''
    missing = list()
    for index in range(highest+1):
        for ftype in TYPES:
            if index == 0 and ftype == 'isus':
                continue
            filename = f'{float_id}.{index:03d}.{ftype}'
            if not os.path.exists(filename):
                print(f'MISSING FILE: {filename}')
                missing.append(filename)
    if missing:
        write_missing_files(missing)


if __name__ == '__main__':
    ARGS = parse_input_args()
    change_cwd(ARGS.directory)
    PASSWD = read_text_file('password.txt')[0]
    LATEST = get_latest_index(ARGS.float_id)
    check_dir(FTP_DIR)
    change_cwd(FTP_DIR)
    DOWNLOADED_FTP = download_files_ftp(PASSWD, LATEST)
    change_cwd(ARGS.directory)
    check_files_ftp(DOWNLOADED_FTP)
    FN_BAK_MISSING = backup_missing_files()
    print(f'New name of most recent backup file: {FN_BAK_MISSING}')
    LATEST = find_latest_profile(ARGS.float_id)
    determine_missing_files(ARGS.float_id, LATEST)
