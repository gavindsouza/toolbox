// Copyright (c) 2023, Gavin D'souza and contributors
// For license information, please see license.txt

frappe.ui.form.on("MariaDB Table", {
	refresh(frm) {
        // TODO: Check need for ANALYZE & add optimize button instead?
        frm.add_custom_button(__("Analyze Table"), () => {
            frappe.show_alert(`Analyzing table ${frm.doc._table_name}`);
            frm.call("analyze").then(({ message }) => {
                let msg = message.pop();
                let indicator = msg.slice(-1).pop() === "OK"? "green" : "red";
                frappe.show_alert({message: msg.join(" "), indicator});
            });
        });
        if (frm.doc.queries.length < frm.doc.num_queries) {
            frm.set_intro(`Only most occurred ${frm.doc.queries.length} queries are loaded in this View`, "yellow");
        }
	},
});
