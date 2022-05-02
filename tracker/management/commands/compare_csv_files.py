from datetime import datetime, timezone
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import hashlib
import logging
import os.path
from pd_tracker.ColourFormatter import ColourFormatter
import pytz
import sqlite3, pandas as pd
from time import sleep
from tracker.models import PDTableField, PDRunLog

# Inspired by code from https://towardsdatascience.com/how-to-compare-large-files-f58982eccd3a


def md5_hash(file_name):
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

    if md5_hash(file_1) == md5_hash(file_2):
        logging.info(f'Files {file_1} and {file_2} are identical and will not be compared.')
        return True
    else:
        logging.info(f'{file_1} and {file_2} are not identical. Proceeding to detailed checks.')
        return False


def make_field_list(l1, alias='x'):
    csv_list = ""
    for i, item in enumerate(l1):
        if i == 0:
            csv_list = f'{alias}.{item}'
        else:
            csv_list += f', {alias}.{item}'
    return csv_list


def execute_sql(sql, conn):
    cursor = conn.cursor()
    for i in range(10):
        try:
            cursor.execute(sql)
            cursor.execute("COMMIT TRANSACTION")
            break
        except sqlite3.OperationalError as e:
            cursor.execute("ROLLBACK TRANSACTION")
            logging.error(f'SQLite error: {e}')
            logging.error(f'Retrying {i} of 10 times')
            sleep(2)
            if i == 9:
                raise e

    cursor.close()


class Command(BaseCommand):
    help = "This command compares two PD CSV files for a specified Open Canada PD type Each of the two versions of the" \
           "PD file must be from different dates specified at runtime. The command will compare the two files and " \
           "write out the saved line along with a log date and change type code (A, D, or C) for each line. This lines" \
           "are written to the specified CSV file and to a SQLite database."

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

        # Look up the primary key for the table from the database
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

        # Connect to the SQLite database and generate the temporary table names
        conn = sqlite3.connect(str(settings.DATABASES['default']['NAME']))
        cur = conn.cursor()
        cur.execute("VACUUM")

        if options['max_reliability']:
            cur.execute("PRAGMA synchronous = NORMAL")
            cur.execute("PRAGMA journal_mode = WAL")
        else:
            cur.execute("PRAGMA synchronous = FULL")
            cur.execute("PRAGMA journal_mode = DELETE")

        temp_tables = ["{0}_{1}".format(table_name, options["source_date"].strftime('%Y_%m_%d')).replace('-', '_'),
                       "{0}_{1}".format(table_name, options["log_date"].strftime('%Y_%m_%d')).replace('-', '_')]

        try:
            # Clear out the temp files if they exist
            for table in temp_tables:
                cur.execute(f'DROP TABLE IF EXISTS {table}')

            # Read the CSV files into the temporary tables
            chunk_size = 500
            i = 0
            for file in csv_files:
                self.logger.info(f'Reading {csv_files[i]} into {temp_tables[i]}')
                i = i + 1
                for chunk in pd.read_csv(file, chunksize=chunk_size, delimiter=","):
                    chunk.columns = chunk.columns.str.replace(' ', '_')  # replacing spaces with underscores for column names
                    chunk.to_sql(name=temp_tables[i - 1], con=conn, if_exists='append')

            # Verify that the columns in both tables match
            temp_columns = []
            for i, t in enumerate(temp_tables):
                cur.execute(f"PRAGMA table_info({t})")
                temp_columns.append([row[1] for row in cur.fetchall()])
            if set(temp_columns[0]) != set(temp_columns[1]):
                raise Exception(f"The columns in the {t} table do not match the columns in the {table_name} definition.")

            # create indexes to accelerate queries
            self.logger.info(f'Creating indexes for {temp_tables[0]} and {temp_tables[1]}')
            cur.execute(f'DROP INDEX IF EXISTS pk_index_{temp_tables[0]}')
            cur.execute(f'DROP INDEX IF EXISTS pk_index_{temp_tables[1]}')
            cur.execute(f'CREATE INDEX pk_index_{temp_tables[0]} on {temp_tables[0]} ({", ".join(primary_key)})')
            cur.execute(f'CREATE INDEX pk_index_{temp_tables[1]} on {temp_tables[1]} ({", ".join(primary_key)})')

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
            statement_counts = f'SELECT "{temp_tables[0]}" AS "TABLE", COUNT(1) FROM "{temp_tables[0]}" UNION SELECT "{temp_tables[1]}" AS "TABLE", COUNT(1) FROM "{temp_tables[1]}"'
            results = cur.execute(statement_counts)
            for row in results:
                self.logger.info(f'{row[0]}: {row[1]}')
            self.logger.info('Checking got new and deleted rows based on data Key.')

            # Build the comparison query
            t1 = ",".join(list(map(lambda s: 'x.' + s, field_names)))
            t2 = ",".join(list(map(lambda s: 'x.' + s, field_names)))
            statement1 = f'SELECT {t1} FROM "{temp_tables[0]}" x LEFT JOIN "{temp_tables[1]}" y ON {joinstatement} {wherestatement}'
            statement2 = f'SELECT {t2} FROM "{temp_tables[1]}" x LEFT JOIN "{temp_tables[0]}" y ON  {joinstatement} {wherestatement}'

            # Clear out any rows associdated with the log date. First check to see if the table exists
            statement_ruthere = f'SELECT name FROM sqlite_master WHERE type="table" AND name="{table_name}"'
            results = cur.execute(statement_ruthere)
            if results.fetchone() is not None:
                self.logger.info(f'Deleting rows from {table_name} based on {options["log_date"]}')
                statement_delete = f'DELETE FROM "{table_name}" WHERE log_date = "{options["log_date"].strftime("%Y-%m-%d")}"'
                cur.execute(statement_delete)

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

            first_time = False if os.path.exists(report_file) else True
            if len(df1.index) > 0:
                if report_file:
                    df1.to_csv(report_file, mode='a', index=False, header=first_time)
                df1.to_sql(table_name, con=conn, if_exists='append')

            first_time = False if os.path.exists(report_file) else True
            if len(df2.index) > 0:
                if report_file:
                    df2.to_csv(report_file, mode='a', index=False, header=first_time)
                df2.to_sql(table_name, con=conn, if_exists='append')

            change_query = ""
            for field in non_key_fields:
                change_query += f"(x.{field} <> y.{field}) OR "
            change_fields = make_field_list(field_names, 'y')
            statement3 = f'''SELECT {change_fields} FROM {temp_tables[0]} x
                                       JOIN {temp_tables[1]} y ON {joinstatement} {wherenotstatement}
                                       AND ({change_query[:-4]})'''

            self.logger.info('Checking for changed rows based on data key.')

            df3 = pd.read_sql(statement3, conn,)

            df3['log_date'] = options['log_date'].strftime('%Y-%m-%d')
            df3['log_activity'] = 'C'
            df3.set_index(primary_key)
            first_time = False if os.path.exists(report_file) else True
            if len(df3.index) > 0:
                if report_file:
                    df3.to_csv(report_file, mode='a', index=False, header=first_time)
                df3.to_sql(table_name, con=conn, if_exists='append')
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
                cur.execute(f'DROP TABLE {table}')
            if options['vacuum']:
                cur.execute('VACUUM')
            conn.close()



