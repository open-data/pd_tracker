from datetime import datetime, timezone
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import hashlib
import logging
import os.path
from pd_tracker.ColourFormatter import ColourFormatter
import pytz
from sqlalchemy import create_engine, TEXT
import pandas as pd
from tracker.models import PDTableField, PDRunLog

# Inspired by an article by Costas Andreau from https://towardsdatascience.com/how-to-compare-large-files-f58982eccd3a


def md5_hash(file_name):
    '''
    Return a simple file hash
    :param file_name: File name to hash
    :return: MD5 hash of the file
    '''
    block_size = 65536
    md5_hasher = hashlib.md5()
    with open(file_name, 'rb') as handle:
        buf = handle.read(block_size)
        while len(buf) > 0:
            md5_hasher.update(buf)
            buf = handle.read(block_size)
    hash_value = md5_hasher.hexdigest()
    logging.info(f'MD5 hash of {file_name} is {hash_value}')
    return hash_value


def compare_files(file_1, file_2):
    '''
    Compare two text files by their MD5 hash
    :param file_1: First file
    :param file_2: Second File
    :return: True if the files are identical, otherwise return false
    '''

    if md5_hash(file_1) == md5_hash(file_2):
        logging.info(f'Files {file_1} and {file_2} are identical and will not be compared.')
        return True
    else:
        logging.info(f'{file_1} and {file_2} are not identical. Proceeding to detailed checks.')
        return False


def make_field_list(l1, alias='x'):
    '''
    Simple sql statement helper to add a table alias to a list of field names
    :param l1: list of field names
    :param alias: a table alias Ex tablex.field_name
    :return: list of field names with table alias
    '''
    csv_list = ""
    for i, item in enumerate(l1):
        if i == 0:
            csv_list = f'{alias}.{item}'
        else:
            csv_list += f', {alias}.{item}'
    return csv_list


class Command(BaseCommand):
    help = "This command compares two PD CSV files for a specified Open Canada PD type Each of the two versions of the" \
           "PD file must be from different dates specified at runtime. The command will compare the two files and " \
           "write out the saved line along with a log date and change type code (A, D, or C) for each line. This lines" \
           "are written to the specified CSV file and to a SQLite database."

    # Set up logging

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(ColourFormatter())
    logger.addHandler(ch)

    def add_arguments(self, parser):
        parser.add_argument('-t', '--table', type=str, help='The Recombinant Type that is being loaded', required=True)
        parser.add_argument('-f1', '--first_file', type=str, help='The first and oldest file that is being loaded', required=True)
        parser.add_argument('-f2', '--second_file', type=str, help='The second and newest file that is being loaded', required=True)
        parser.add_argument('-s', '--source_date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'), action='store',
                            help='The date of the comparison source. Would normally correspond to the data of the first file.', required=True)
        parser.add_argument('-l', '--log_date', type=lambda s: datetime.strptime(s, '%Y-%m-%d'), action='store',
                            help='The date of the comparison target. Would normally correspond to the data of the second file.', required=True)
        parser.add_argument('-r', '--report_file', type=str, help='The PD report file. Appends to the file if it already exists',
                            required=False, default="")
        parser.add_argument('-x', '--max_reliability', action='store_true', help='Flag to indicate if max SQLite reliability should be used.',
                            required=False, default=False)
        parser.add_argument('-u', '--vacuum', action='store_true', help='Vacuum the SQLite database after the comparison is complete. Don\'t vacuum if you are running this command from a batch job.',
                            required=False, default=False)

    def handle(self, *args, **options):

        table_name = options['table'].replace('-', '_')
        csv_files = [options['first_file'], options['second_file']]

        # Use file hashing to determine if the files are the same before comparing them.
        if compare_files(options['first_file'], options['second_file']):
            exit(0)

        # Look up the primary key for the table from the PD database
        pkeys = PDTableField.objects.filter(table_id=table_name, primary_key=True).order_by('field_order')
        if pkeys.count() == 0:
            raise CommandError(f'No primary key found for table {table_name}')
        primary_key = []
        for pkey in pkeys:
            primary_key.append(pkey.field_name)

        # Look up the key and non-key fields for the table
        pd_fields = PDTableField.objects.filter(table_id=table_name).order_by('field_order')
        field_names = []
        non_key_fields = []
        for pd_field in pd_fields:
            field_names.append(pd_field.field_name)
            if not pd_field.primary_key:
                non_key_fields.append(pd_field.field_name)

        # Connect to the Postgresql database and generate the temporary table names

        temp_tables = ["{0}_{1}".format(table_name, options["source_date"].strftime('%Y_%m_%d')).replace('-', '_'),
                       "{0}_{1}".format(table_name, options["log_date"].strftime('%Y_%m_%d')).replace('-', '_')]
        conn_string = f"postgresql+psycopg2://{str(settings.DATABASES['default']['USER'])}:{str(settings.DATABASES['default']['PASSWORD'])}@{str(settings.DATABASES['default']['HOST'])}/{str(settings.DATABASES['default']['NAME'])}"
        eng = create_engine(conn_string)
        conn = eng.connect()
        try:
            # Clear out the temp files if they exist
            for table in temp_tables:
                conn.execute(f'DROP TABLE IF EXISTS {table}')

            # Read the CSV files into the temporary tables

            chunk_size = 1000
            i = 0
            for file in csv_files:
                self.logger.info(f'Reading {csv_files[i]} into {temp_tables[i]}')
                i = i + 1
                for chunk in pd.read_csv(file, chunksize=chunk_size, delimiter=",", dtype=str):
                    chunk.columns = chunk.columns.str.replace(' ', '_')  # replacing spaces with underscores for column names
                    chunk.to_sql(name=temp_tables[i - 1], con=conn, if_exists='append', dtype=TEXT, index=False)

            # Verify that the columns in both tables match

            temp_columns = []
            column_names = []
            for i, t in enumerate(temp_tables):
                results = conn.execute(f"select column_name from information_schema.columns where table_name = '{t}'")
                column_names = []
                for row in results:
                    column_names.append(row['column_name'])
                temp_columns.append(column_names)

            # Bail if the columns don't match - this condition voids the comparison

            if set(temp_columns[0]) != set(temp_columns[1]):
                raise Exception(f"The columns in the {t} table do not match the columns in the {table_name} definition.")

            # create a list of non-primary key fields that actually in the file. This is based on the fields that were read
            # in from the file. The PD database should hold the latest definition, but older CSV files may not have fewer columns

            std_fields = []
            for f in non_key_fields:
                if f in column_names:
                    std_fields.append(f)

            # create indexes to accelerate queries

            self.logger.info(f'Creating indexes for {temp_tables[0]} and {temp_tables[1]}')
            conn.execute(f'DROP INDEX IF EXISTS pk_index_{temp_tables[0]}')
            conn.execute(f'DROP INDEX IF EXISTS pk_index_{temp_tables[1]}')
            conn.execute(f'CREATE INDEX pk_index_{temp_tables[0]} on {temp_tables[0]} ({", ".join(primary_key)})')
            conn.execute(f'CREATE INDEX pk_index_{temp_tables[1]} on {temp_tables[1]} ({", ".join(primary_key)})')

            # Normally you would not build queries using strings, but the key values are coming from the config database

            for key in primary_key:
                if key == primary_key[0]:
                    joinstatement = f'x.{key} = y.{key}'
                    wherestatement = f' WHERE y.{key} IS NULL'
                    wherenotstatement = f' WHERE y.{key} IS NOT NULL'
                else:
                    joinstatement += f' AND x.{key} = y.{key}'
                    wherestatement += f' AND y.{key} IS NULL'
                    wherenotstatement += f' AND y.{key} IS NOT NULL'

            # Log some information about the two CSV files

            self.logger.info('Total CSV Row Counts')
            statement_counts = f"SELECT 'one', COUNT(*) FROM {temp_tables[0]} UNION SELECT 'two', COUNT(*) FROM {temp_tables[1]}"
            results = conn.execute(statement_counts)
            i = 0

            # Note: loop indexing will not compatible with older versions of Python 3
            for row in results:
                self.logger.info(f'{temp_tables[i]}: {row["count"]}')
                i += 1
            self.logger.info('Checking got new and deleted rows based on data Key.')

            # Build the comparison query

            if "owner_org_title" in column_names:
                column_names.remove("owner_org_title")
            t1 = ",".join(list(map(lambda s: 'x.' + s, column_names)))
            t2 = ",".join(list(map(lambda s: 'x.' + s, column_names)))
            statement1 = f'SELECT {t1} FROM "{temp_tables[0]}" x LEFT JOIN "{temp_tables[1]}" y ON {joinstatement} {wherestatement}'
            statement2 = f'SELECT {t2} FROM "{temp_tables[1]}" x LEFT JOIN "{temp_tables[0]}" y ON  {joinstatement} {wherestatement}'

            # Delete any rows associated with the log date being processed - these will be replaced. First check to see if the table exists

            statement_ruthere = f"SELECT EXISTS(SELECT FROM pg_tables WHERE schemaname = 'public' and tablename = '{table_name}')"
            results = conn.execute(statement_ruthere)
            r = results.fetchone()
            if r['exists']:
                self.logger.info(f'Deleting rows from {table_name} based on {options["log_date"]}')
                log_date_str = options["log_date"].strftime("%Y-%m-%d")
                statement_delete = f"DELETE FROM {table_name} WHERE log_date = '{log_date_str}'"
                conn.execute(statement_delete)

            # Running row matching queries to determine additions and deletions

            primary_key.append('log_date')
            df1 = pd.read_sql(statement1, conn)
            df2 = pd.read_sql(statement2, conn)
            df1['log_date'] = options['log_date'].strftime('%Y-%m-%d')
            df1['log_activity'] = 'D'
            df1.set_index(primary_key)

            df2['log_date'] = options['log_date'].strftime('%Y-%m-%d')
            df2['log_activity'] = 'A'
            df2.set_index(primary_key)

            # if the export file name is not provided, then generate one using the table name abd the default export directory

            report_file = options['report_file'] if options['report_file'] else ""
            if not report_file and settings.EXPORT_TO_CSV_BY_DEFAULT:
                report_file = os.path.join(settings.DEFAULT_CSV_EXPORT_DIR, f'{table_name}_activity.csv')

            # Report on additions and deletions

            first_time = False if os.path.exists(report_file) else True
            if len(df1.index) > 0:
                if report_file:
                    df1.to_csv(report_file, mode='a', index=False, header=first_time)
                df1.to_sql(table_name, con=conn, if_exists='append', dtype=TEXT, index=False)

            first_time = False if os.path.exists(report_file) else True
            if len(df2.index) > 0:
                if report_file:
                    df2.to_csv(report_file, mode='a', index=False, header=first_time)
                df2.to_sql(table_name, con=conn, if_exists='append', dtype=TEXT, index=False)

            # Running row matching query to determine what rows have changed

            change_query = ""
            for field in std_fields:
                change_query += f"(x.{field} <> y.{field}) OR "
            change_fields = make_field_list(column_names, 'y')
            statement3 = f'''SELECT {change_fields} FROM {temp_tables[0]} x
                                       JOIN {temp_tables[1]} y ON {joinstatement} {wherenotstatement}
                                       AND ({change_query[:-4]})'''

            self.logger.info('Checking for changed rows based on data key.')

            df3 = pd.read_sql(statement3, conn,)

            df3['log_date'] = options['log_date'].strftime('%Y-%m-%d')
            df3['log_activity'] = 'C'
            df3.set_index(primary_key)

            # Report on changes

            first_time = False if os.path.exists(report_file) else True
            if len(df3.index) > 0:
                if report_file:
                    df3.to_csv(report_file, mode='a', index=False, header=first_time)
                df3.to_sql(table_name, con=conn, if_exists='append', dtype=TEXT, index=False)

            # Log the PD tracker run to the intenal database

            local_tz = pytz.timezone(settings.TIME_ZONE)
            local_now = local_tz.localize(datetime.now())
            PDRunLog.objects.create(
                table_id=table_name,
                file_from=options['first_file'],
                file_to=options['second_file'],
                activity_date=local_now,
                log_date=local_tz.localize(options['log_date']),
                report_file=report_file,
                rows_added=len(df2.index),
                rows_deleted=len(df1.index),
                rows_updated=len(df3.index),
            )
            self.logger.info(f'{table_name} completed: {len(df2.index)} rows added, {len(df1.index)} rows deleted, {len(df3.index)} rows updated')

        except Exception as e:
            self.logger.critical(f'Error processing table {table_name}')
            self.logger.error(e)

        finally:
            for table in temp_tables:
                conn.execute(f'DROP TABLE {table}')
            if options['vacuum']:
                conn.execute('VACUUM')
            conn.close()



