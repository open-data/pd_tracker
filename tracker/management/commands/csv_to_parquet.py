from django.core.management.base import BaseCommand
import logging
import pyarrow.csv as pv
import pyarrow.parquet as pq

class Command(BaseCommand):
    help = "Convert a CSV activity file to Parquet."
    logger = logging.getLogger(__name__)

    def add_arguments(self, parser):
        parser.add_argument('--csv', type=str, required=True, help='CSV input file name')
        parser.add_argument('--parquet', type=str, required=True, help='Parquet output file name')

    def handle(self, *args, **options):

        parse_opts = pv.ParseOptions(newlines_in_values=True)
        table = pv.read_csv(options['csv'], parse_options=parse_opts)
        pq.write_table(table, options['parquet'])

