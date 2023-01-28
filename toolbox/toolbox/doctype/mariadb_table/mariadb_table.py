# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class MariaDBTable(Document):
    ...
    # TODO:
    # 1. Show table size (MiB)
    # 2. Show table indexes / index size (MiB)
    # 3. Show table health status / fragmentation
    # 5. Show table row count / require partitioning?
