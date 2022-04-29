from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import logging
import requests
from tracker.models import PDTableField


class Command(BaseCommand):
    help = "Import CKAN schema"
    logger = logging.getLogger(__name__)

    def add_arguments(self, parser):
        parser.add_argument('-t', '--table', type=str, help='The Recombinant Type that is being loaded', required=True)

    def handle(self, *args, **options):
        table_name = options['table']
        self.logger.info("Importing CKAN schema for table: {}".format(table_name))
        response = requests.get(settings.CKAN_RECOMBINANT_API_URL.format(options['table']), timeout=100, verify=False)
        if response.status_code == 200:
            self.logger.info("Successfully retrieved CKAN schema for table: {}".format(table_name))
            field_info = response.json()
            for r in range(len(field_info['resources'])):
                pk_fields = field_info['resources'][r]['primary_key']
                table_fields = field_info['resources'][r]['fields']

                # Use _ in place of - for table names
                table_id = field_info['resources'][r]['resource_name'].replace('-', '_')
                # Delete all existing fields for this table
                PDTableField.objects.filter(table_id=field_info['resources'][r]['resource_name']).delete()

                for field_order, field in enumerate(table_fields):
                    tbf, created = PDTableField.objects.update_or_create(
                        table_id=table_id,
                        field_name=field['id'],
                        field_order=field_order,
                        field_type=field['datastore_type'],
                        label_en=field['label']['en'],
                        label_fr=field['label']['fr'],
                        primary_key=True if field['id'] in pk_fields else False,
                    )
                    tbf.save()
                tbf, created = PDTableField.objects.update_or_create(
                    table_id=table_id,
                    field_name="owner_org",
                    field_order=len(table_fields),
                    field_type="text",
                    label_en="Organization",
                    label_fr="Organisation",
                    primary_key=True,
                )
                # Note: The Open Data Inventory is an exception and does not audit fields
                if table_name != "inventory":
                    tbf, created = PDTableField.objects.update_or_create(
                        table_id=table_id,
                        field_name="record_created",
                        field_order=len(table_fields) + 1,
                        field_type="text",
                        label_en="Record Creation Time",
                        label_fr="Temps de création de l'enregistrement",
                        primary_key=False,
                    )
                    tbf, created = PDTableField.objects.update_or_create(
                        table_id=table_id,
                        field_name="record_modified",
                        field_order=len(table_fields) + 2,
                        field_type="text",
                        label_en="Last Record Modification Time",
                        label_fr="Temps de modification du dernier enregistrement",
                        primary_key=False,
                    )
                    tbf, created = PDTableField.objects.update_or_create(
                        table_id=table_id,
                        field_name="user_modified",
                        field_order=len(table_fields) + 3,
                        field_type="text",
                        label_en="User Last Modified Record",
                        label_fr="Utilisateur Dernier enregistrement modifié",
                        primary_key=False,
                    )
                tbf.save()
                self.logger.info("Successfully imported CKAN schema for table: {}".format(field_info['resources'][r]['resource_name']))
        else:
            self.logger.error("Failed to retrieve CKAN schema for table: {}".format(table_name))
            self.logger.error("Response: {}".format(response.text))
            self.logger.error("Response status code: {}".format(response.status_code))
            self.logger.error("Response headers: {}".format(response.headers))
            self.logger.error("Response cookies: {}".format(response.cookies))
            self.logger.error("Response history: {}".format(response.history))
            self.logger.error("Response elapsed: {}".format(response.elapsed))
            self.logger.error("Response request: {}".format(response.request))
            self.logger.error("Response url: {}".format(response.url))
            self.logger.error("Response reason: {}".format(response.reason))
            self.logger.error("Response encoding: {}".format(response.encoding))
            self.logger.error("Response text: {}".format(response.text))

            raise CommandError("Failed to retrieve CKAN schema for table: {}".format(table_name))

        return None
