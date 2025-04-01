from django.contrib import admin
from .models import PDTableField, PDRunLog


def set_pdexport_field(modeladmn, request, queryset):
    queryset.update(pd_export=True)

def unset_pdexport_field(modeladmn, request, queryset):
    queryset.update(pd_export=False)

def set_primary_key_field(modeladmn, request, queryset):
    queryset.update(primary_key=True)

def unset_primary_key_field(modeladmn, request, queryset):
    queryset.update(primary_key=False)

# Register your models here.
@admin.register(PDTableField)
class PDTableFieldAdmin(admin.ModelAdmin):
    list_display = ['table_id', 'field_name', 'field_order', 'label_en', 'field_type', 'primary_key', 'pd_export']
    list_filter = ['table_id']
    ordering = ['table_id', 'field_order']
    actions = [set_primary_key_field, unset_primary_key_field, set_pdexport_field, unset_pdexport_field]

@admin.register(PDRunLog)
class PDRunLogAdmin(admin.ModelAdmin):

    list_display = ['table_id', 'log_date', 'rows_added', 'rows_updated', 'rows_deleted']
    list_filter =['table_id', 'log_date']
    ordering = ['log_date', 'table_id']