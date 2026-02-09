import frappe


def execute():
    if frappe.db.has_column("MariaDB Query", "occurence"):
        from toolbox.db_adapter import get_rename_column_sql

        frappe.db.sql_ddl(
            get_rename_column_sql("tabMariaDB Query", "occurence", "occurrence", "INT(11)")
        )
