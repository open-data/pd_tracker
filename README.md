PD Tracker is a Python Django application that tracks the changes in the PD data 
CSV files collected by Open Canada.

## Installation

### Requirements: 
* Python 3.6 or higher

### Setup:

Clone and download the source code from [GitHub]() and run the following commands:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
Note that Python 3 is required for the application to run.

Copy the `settings.example.py` file to `settings.py` and change the settings to reflect
your environment.

Then run the following commands:

```bash
python manage.py makemigrations
python manage.py sqlmigrate tracker 0001
python manage.py migrate
python manage.py collectstatic --noinput
```

## Usage

Before running PD CSV comparisons, it is necessary to load the field metadata for each PD type into the database.
Run the following command to load the field metadata for each PD type:

```bash
python manage.py import_ckan_schema -t <pd_type>
```

To view the field metadata for a PD type, create a new Django admin user and login to the admin site.

To run a PD CSV comparison, run the following command:

```bash
python manage.py --help

usage: manage.py compare_csv_files [-h] -t TABLE -f1 FIRST_FILE -f2 SECOND_FILE -s SOURCE_DATE -l LOG_DATE -r REPORT_FILE
                                   [--version] [-v {0,1,2,3}] [--settings SETTINGS] [--pythonpath PYTHONPATH] [--traceback]
                                   [--no-color] [--force-color] [--skip-checks]
  -t TABLE, --table TABLE
                        The Recombinant Type that is being loaded
  -f1 FIRST_FILE, --first_file FIRST_FILE
                        The first and oldest file that is being loaded
  -f2 SECOND_FILE, --second_file SECOND_FILE
                        The second and newest file that is being loaded
  -s SOURCE_DATE, --source_date SOURCE_DATE
                        The date of the comparison source. Would normally correspond to the data of the first file.
  -l LOG_DATE, --log_date LOG_DATE
                        The date of the comparison target. Would normally correspond to the data of the second file.
  -r REPORT_FILE, --report_file REPORT_FILE
                        The PD report file. Appends to the file if it already exists
```

##### Example:

```bash
python .\manage.py compare_csv_files --table adminaircraft --first_file data\20220329\adminaircraft.csv --second_file  data\20220330\adminaircraft.csv --source_date 2022-03-29 --log_date 2022-03-30 --report_file data\adminaircraft_activity_log.csv
```

