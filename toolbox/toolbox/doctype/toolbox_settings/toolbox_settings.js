// Copyright (c) 2023, Gavin D'souza and contributors
// For license information, please see license.txt

frappe.ui.form.on("ToolBox Settings", {
    after_save(frm) {
        frappe.boot.toolbox.index_manager.enabled = frm.doc.is_index_manager_enabled;
    },
    refresh(frm) {
        frm.add_custom_button("Index Manager", () => frappe.set_route("dashboard-view", "Index Manager"));
        frm.add_custom_button("Index Manager", () => {
            frappe.db.get_value("Scheduled Job Type", { "method": ["like", "%process_index_manager"] }, "name", ({ name }) => {
                frappe.set_route("Form", "Scheduled Job Type", name);
            });
        }, "Show Scheduled Jobs");
        frm.add_custom_button("Query Processor", () => {
            frappe.db.get_value("Scheduled Job Type", { "method": ["like", "%process_sql_recorder"] }, "name", ({ name }) => {
                frappe.set_route("Form", "Scheduled Job Type", name);
            });
        }, "Show Scheduled Jobs");
    }
});
