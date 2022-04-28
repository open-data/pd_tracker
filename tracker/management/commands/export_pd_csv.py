import os.path
import sqlite3, pandas as pd
from datetime import datetime, timezone
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import logging
from tracker.models import PDTableField, PDRunLog


class Command(BaseCommand):
    help = "Export PD data to CSV. Can export all types or a single type."
    logger = logging.getLogger(__name__)
    # Connect to the SQLite database and generate the temporary table names
    conn = sqlite3.connect(str(settings.DATABASES['default']['NAME']))

    def export_type(self, table_name, report_dir):

        # Look up the primary key for the table from the database
        pkeys = PDTableField.objects.filter(table_id=table_name, primary_key=True).order_by('field_order')
        if pkeys.count() == 0:
            raise CommandError(f'No primary key found for table {table_name}')
        primary_key = []
        for pkey in pkeys:
            primary_key.append(pkey.field_name)

        # Get the fields to export

        i = 0
        report_file = os.path.join(report_dir, f'{table_name}_activity.csv')
        for chunk in pd.read_sql(f'SELECT * FROM "{table_name}"', self.conn, index_col=primary_key, chunksize=1000):
            if i == 0:
                chunk.to_csv(report_file, index=True, header=True, mode='w')
                i = chunk.index.size
            else:
                chunk.to_csv(report_file, mode='a', header=False, index=True)
                i += chunk.index.size
        self.logger.info(f'Exported {i} rows to {report_file}')

    def add_arguments(self, parser):
        parser.add_argument('table', type=str, help='The Recombinant Type that to be exported. Use "all" to export all.')
        parser.add_argument('-d', '--report_dir', type=str, help='The director where to write PD report files.', required=True)

    def handle(self, *args, **options):
        table_name = options['table']
        try:
            if table_name == 'all':
                table_list = PDTableField.objects.all().values_list('table_id', flat=True).distinct()
                for table in table_list:
                    norm_table = table.replace('-', '_')
                    table_exists = pd.read_sql(f"SELECT name FROM sqlite_master WHERE type='table' and name='{norm_table}'", self.conn)
                    if  table_exists.empty:
                        continue
                    self.export_type(table, options['report_dir'])
            else:
                self.export_type(table_name, options['report_dir'])
        finally:
            self.conn.close()
