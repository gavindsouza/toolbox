# Copyright (c) 2023, Gavin D'souza and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class MariaDBQueryExplain(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        extra: DF.Data | None
        filtered: DF.Int
        id: DF.Int
        key: DF.Data | None
        key_len: DF.Int
        parent: DF.Data
        parentfield: DF.Data
        parenttype: DF.Data
        possible_keys: DF.SmallText | None
        ref: DF.Data | None
        rows: DF.Int
        select_type: DF.Data | None
        table: DF.Link | None
        type: DF.Data | None
    # end: auto-generated types
    ...
