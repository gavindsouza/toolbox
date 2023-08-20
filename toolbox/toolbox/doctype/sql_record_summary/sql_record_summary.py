# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class SQLRecordSummary(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        name: DF.Int | None
        total_sql_count: DF.Int
        unique_sql_count: DF.Int
    # end: auto-generated types
    pass
