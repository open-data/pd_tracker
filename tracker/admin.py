from django.contrib import admin
from .models import PDTableField, PDRunLog


# Register your models here.
@admin.register(PDTableField)
class PDTableFieldAdmin(admin.ModelAdmin):
    list_display = ['table_id', 'field_name', 'field_order', 'label_en']
    list_filter = ['table_id']
    ordering = ['table_id', 'field_order']


@admin.register(PDRunLog)
class PDRunLogAdmin(admin.ModelAdmin):

    list_display = ['table_id', 'log_date', 'rows_added', 'rows_updated', 'rows_deleted']
    list_filter =['table_id', 'log_date']
    ordering = ['log_date', 'table_id']