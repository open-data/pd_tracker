from django.db import models


class PDTableField(models.Model):
    """
    This class represents the PDTableFields model.
    It contains the field metadata from the Open Canada PD CKAN recombinant types. This information is
    updated using the import_ckan_schema.py script.
    """
    # Fields
    field_id = models.AutoField(primary_key=True)
    table_id = models.CharField(max_length=100)
    field_name = models.CharField(max_length=200)
    field_order = models.IntegerField()
    field_type = models.CharField(max_length=100)
    label_en = models.CharField(max_length=200)
    label_fr = models.CharField(max_length=200)
    primary_key = models.BooleanField(default=False)
    pd_export = models.BooleanField(default=False)

    # Meta
    class Meta:
        unique_together = ['table_id', 'field_name']
        ordering = ['table_id', 'field_name', 'field_order']
        verbose_name = 'PD Types Field'
        verbose_name_plural = 'PD Types Fields'

    # Relationships
    # Methods
    def __str__(self):
        """
        String for representing the Model object (in Admin site etc.)
        """
        return f'{self.table_id}-{self.field_name}'


class PDRunLog(models.Model):
    """
    This class represents the PDActivityLog model.
    Every time a PD CSV comparison is run, a record is created in this table.
    """
    # Fields
    activity_id = models.AutoField(primary_key=True)
    table_id = models.CharField(max_length=100)
    file_from = models.CharField(max_length=200)
    file_to = models.CharField(max_length=200)
    activity_date = models.DateTimeField()
    log_date = models.DateTimeField()
    report_file = models.CharField(max_length=200)
    rows_added = models.IntegerField(default=0)
    rows_updated = models.IntegerField(default=0)
    rows_deleted = models.IntegerField(default=0)

    # Relationships
    # Methods
    def __str__(self):
        """
        String for representing the Model object (in Admin site etc.)
        """
        return f'{self.table_id}-{self.file_from}-{self.file_to}-{self.log_date}'

    class Meta:
        ordering = ['-log_date', 'table_id']
        verbose_name = 'PD Warehouse Run Log'
        verbose_name_plural = 'PD Warehouse Run Logs'