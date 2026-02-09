# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

from toolbox.db_adapter import get_unused_indexes_data


def execute(filters=None):
    columns = [
        {"fieldname": "TABLE_NAME", "label": "TABLE_NAME", "fieldtype": "Data", "width": 150},
        {
            "fieldname": "MariaDB Table",
            "label": "MariaDB Table",
            "fieldtype": "Link",
            "options": "MariaDB Table",
            "width": 150,
        },
        {"fieldname": "INDEX_NAME", "label": "INDEX_NAME", "fieldtype": "Data", "width": 150},
        {
            "fieldname": "MariaDB Index",
            "label": "MariaDB Index",
            "fieldtype": "Link",
            "options": "MariaDB Index",
            "width": 300,
        },
    ]

    data = get_unused_indexes_data()
    return columns, data
