// Copyright (c) 2023, Gavin D'souza and contributors
// For license information, please see license.txt

frappe.ui.form.on("ToolBox Settings", {
    after_save(frm) {
        frappe.boot.toolbox.index_manager.enabled = frm.doc.is_index_manager_enabled;
    },
    refresh(frm) {
        frm.add_custom_button("Index Manager", () => frappe.set_route("index-manager"));
        frm.add_custom_button("Query Processor", () => frappe.set_route("Form", "Scheduled Job Type", "toolbox_settings.process_sql_recorder"), "Show Scheduled Jobs");
        frm.add_custom_button("Index Manager", () => frappe.set_route("Form", "Scheduled Job Type", "toolbox_settings.process_index_manager"), "Show Scheduled Jobs");
    }
});
