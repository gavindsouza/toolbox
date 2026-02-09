import frappe


def execute():
    if frappe.db.has_column("MariaDB Query", "occurence"):
        frappe.db.sql_ddl("ALTER TABLE `tabMariaDB Query` CHANGE `occurence` `occurrence` INT(11)")
