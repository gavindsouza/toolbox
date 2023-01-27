// Copyright (c) 2023, Gavin D'souza and contributors
// For license information, please see license.txt

frappe.ui.form.on("MariaDB Index", {
	refresh(frm) {
        frm.set_read_only();
        frm.fields.filter((field) => field.has_input).forEach((field) => {
            frm.set_df_property(field.df.fieldname, "read_only", "1");
        });
        frm.disable_save();
	},
});
