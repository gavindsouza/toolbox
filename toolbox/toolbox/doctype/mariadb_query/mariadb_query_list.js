// Copyright (c) 2023, Gavin D'souza and contributors
// For license information, please see license.txt

frappe.listview_settings['MariaDB Query'] = {
    onload: function(listview) {
        // Remove the "query" title field from standard search - it's not useful
        document.querySelector("#page-List\\/MariaDB\\ Query\\/List > div.container.page-body > div.page-wrapper > div > div.row.layout-main > div.col.layout-main-section-wrapper > div.layout-main-section.frappe-card > div.page-form.flex > div.standard-filter-section.flex > div:nth-child(2)").remove();
    }
}