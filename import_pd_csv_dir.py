import argparse
from datetime import datetime
import os
import pathlib
import shutil
import subprocess
import sys
import tarfile
import tempfile


parser = argparse.ArgumentParser(description="Import multiple csv files from an archive directory and running the compare_csv_files.py script",
                                 epilog="This script assumes that a day's worth of PD CSV files are stored in a GZIP TAR file where each day's file "
                                        "follows the file name convention: pd-YYYYMMDD.tar.gz where YYYYMMDD is the date of the CSV files in the "
                                        "archive file. The archive file itself contains the PD CSV files published that day on Open Canada")
parser.add_argument("-d", "--data_dir", type=pathlib.Path, required=True,
                    help="The directory where to find the warehouse reports CSV files. There must be at least 2 archive files to run the analysis.")
parser.add_argument("-1", "--latest_only", action='store_true', required=False, default=False,
                    help="Only compare the last two CSV files. This option cannot be selected when either a start or end data is specified.")
parser.add_argument("--start_date", type=lambda s: datetime.strptime(s, '%Y-%m-%d'), required=False,
                    action='store', help="The date to start the warehouse processing from. "
                                         "If no start date is specified, the script will run from the beginning of the directory. Format: YYYY-MM-DD")
parser.add_argument("--end_date", type=lambda s: datetime.strptime(s, '%Y-%m-%d'), action='store', required=False,
                    help="The date to stop on. If not specified, the script will run until the end of the directory. "
                         "Format: YYYY-MM-DD")
args = parser.parse_args()

# Get the list of archive files based on the user provided parameters

file_list = os.listdir(args.data_dir)
sorted_file_list = [x for x in file_list if x.endswith('.tar.gz')]
sorted_file_list.sort()
if len(sorted_file_list) < 2:
    print("Not enough files in data directory.")
    sys.exit(1)
if args.latest_only and (args.start_date or args.end_date):
    print("Cannot use both --latest_only and --start_date or --end_date.")
    sys.exit(1)
if args.latest_only:
    sorted_file_list = sorted_file_list[-2:]
else:
    if args.start_date:
        sorted_file_list = [x for x in sorted_file_list if args.start_date <= datetime.strptime(x[3:11], '%Y%m%d')]
    if args.end_date:
        sorted_file_list = [x for x in sorted_file_list if args.end_date >= datetime.strptime(x[3:11], '%Y%m%d')]

from_file = ""
to_file = ""
to_date = None
from_date = None
temp_from_dir = None

# For each archive file, extract the contents to a temporary directory and call the compare_csv_files.py script

try:
    for tar_file in sorted_file_list:
        # Redundant check, but just in case.
        if not tar_file.endswith('.tar.gz'):
            continue
        to_file = tar_file

        temp_to_dir = tempfile.mkdtemp()
        print('Extracting {0} to {1}'.format(tar_file, temp_to_dir))
        tar = tarfile.open(os.path.join(args.data_dir, tar_file))
        tar.extractall(temp_to_dir)
        tar.close()

        to_date = datetime.strptime(tar_file[3:11], "%Y%m%d")

        if not from_file:
            from_file = to_file
            temp_from_dir = temp_to_dir
            from_date = to_date
            continue

        print('Processing changes to {0}'.format(tar_file))

        sorted_csv_list = os.listdir(temp_to_dir)
        sorted_csv_list.sort()

        # Run the compare script for each file

        for csv_file in sorted_csv_list:
            csv_from = os.path.join(temp_from_dir, csv_file)
            csv_to = os.path.join(temp_to_dir, csv_file)

            if os.path.exists(csv_from) and os.path.exists(csv_to):
                print(f' Running {sys.executable} manage.py compare_csv_files -t {pathlib.Path(csv_file).stem} -fi {csv_from} -f2 {csv_to} -s {from_date.strftime("%Y-%m-%d")} -l {to_date.strftime("%Y-%m-%d")}')
                proc = subprocess.run([sys.executable, 'manage.py', 'compare_csv_files', '-t',
                                       pathlib.Path(csv_file).stem.replace('-', '_'), '-f1', csv_from, '-f2', csv_to,
                                       '-s', from_date.strftime("%Y-%m-%d"), '-l', to_date.strftime("%Y-%m-%d"), '-x'])
                if proc.returncode != 0:
                    print(f'Error running {sys.executable} manage.py compare_csv_files -t {pathlib.Path(csv_file).stem} -fi {csv_from} -f2 {csv_to} -s {from_date.strftime("%Y-%m-%d")} -l {to_date.strftime("%Y-%m-%d")}', file=sys.stderr)
        from_date = to_date

        # Clean up the previous temp dir
        if temp_from_dir and os.path.exists(temp_from_dir):
            shutil.rmtree(temp_from_dir)
        temp_from_dir = temp_to_dir

finally:
    if temp_from_dir and os.path.exists(temp_from_dir):
        shutil.rmtree(temp_from_dir)