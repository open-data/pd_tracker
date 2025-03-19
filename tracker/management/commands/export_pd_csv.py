import csv
import os.path
import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import logging
from sqlalchemy import create_engine
from tracker.models import PDTableField, PDRunLog


class Command(BaseCommand):
    help = "Export PD data to CSV. Can export all types or a single type."
    logger = logging.getLogger(__name__)


    def export_type(self, table_name, report_dir, cols:list):
        try:
            # Look up the primary key for the table from the database
            pkeys = PDTableField.objects.filter(table_id=table_name, primary_key=True).order_by('field_order')
            if pkeys.count() == 0:
                raise CommandError(f'No primary key found for table {table_name}')
            primary_key = []
            for pkey in pkeys:
                primary_key.append(pkey.field_name)

            # Get the fields to export
            conn_string = f"postgresql+psycopg2://{str(settings.DATABASES['default']['USER'])}:{str(settings.DATABASES['default']['PASSWORD'])}@{str(settings.DATABASES['default']['HOST'])}/{str(settings.DATABASES['default']['NAME'])}"
            eng = create_engine(conn_string)
            conn = eng.connect()

            i = 0
            csv_filename = f'{table_name}_activity.csv'
            if len(cols) > 0:
                csv_filename = f'{table_name}_pd_activity.csv'
            report_file = os.path.join(report_dir, csv_filename)
            sql_query = f'SELECT * FROM "{table_name}"'          
            if len(cols):
                sql_query = f'SELECt {",".join(cols)} FROM {table_name}'

            for chunk in pd.read_sql(sql_query, conn.connection, index_col=primary_key, chunksize=1000):
                if i == 0:
                    chunk.to_csv(report_file, index=True, header=True, mode='w', encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
                    i = chunk.index.size
                else:
                    chunk.to_csv(report_file, mode='a', header=False, index=True, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
                    i += chunk.index.size
            self.logger.info(f'Exported {i} rows to {report_file}')
        finally:
            conn.close()

    def add_arguments(self, parser):
        parser.add_argument('table', type=str, help='The Recombinant Type that to be exported. Use "all" to export all.')
        parser.add_argument('-d', '--report_dir', type=str, help='The directory where to write PD report files.', required=True)
        parser.add_argument('-f', '--filtered', action='store_true', help='Export filtered versions of some PD report files with limited columns')

    def handle(self, *args, **options):
        table_name = options['table'].replace('-', '_')
        try:
            conn_string = f"postgresql+psycopg2://{str(settings.DATABASES['default']['USER'])}:{str(settings.DATABASES['default']['PASSWORD'])}@{str(settings.DATABASES['default']['HOST'])}/{str(settings.DATABASES['default']['NAME'])}"
            eng = create_engine(conn_string)
            conn = eng.connect()

            if table_name == 'all':
                # Obtain a set if the unique table names derived from the fields table
                table_qs = PDTableField.objects.all()
                table_list = []
                for table in table_qs:
                    table_list.append(table.table_id)
                table_list = set(table_list)

                # Export each table
                for table in table_list:
                    statement_ruthere = f"SELECT EXISTS(SELECT FROM pg_tables WHERE schemaname = 'public' and tablename = '{table}')"
                    results = conn.execute(statement_ruthere)
                    r = results.fetchone()
                    if not r['exists']:
                        continue
                    self.export_type(table, options['report_dir'], [])

                    # If filtered files requested, check to see if filter has been specified. Al NIL reports are automatically
                    # excluded from this action.
                    xfields = PDTableField.objects.filter(table_id=table, pd_export=True).order_by('field_order')
                    if not table.endswith("_nil") and  xfields.count() > 0:
                        cols = []
                        for f in xfields:
                            cols.append(f.field_name)
                        self.export_type(table, options['report_dir'], cols=cols)
                    
            else:
                self.export_type(table_name, options['report_dir'])
        finally:
            conn.close()
