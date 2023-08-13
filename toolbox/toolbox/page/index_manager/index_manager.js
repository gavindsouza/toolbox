const ToolboxSettings = "ToolBox Settings";

$(document).on("page-change", function (event) {
    if (event.page?.page_name === "index-manager") {
        frappe.pages['index-manager'].refresh(event.page);
    }
});

frappe.pages['index-manager'].on_page_load = function (wrapper) {
    const page = frappe.ui.make_app_page({
        parent: wrapper,
        title: 'Index Manager - ToolBox',
        single_column: true
    });
    page.add_button("Settings", () => frappe.set_route("Form", ToolboxSettings));
    page.add_menu_item("Refresh", function() {
        frappe.pages['index-manager'].refresh(page.parent);
    }.bind(page));
}

frappe.pages['index-manager'].refresh = async function (wrapper) {
    const page = wrapper.page;

    if (!frappe.boot.toolbox.index_manager.enabled) {
        page.main.html(`<div class="text-center" style="margin-top: 100px;">
            <h1>Index Manager</h1>
            <p>Index Manager is disabled</p>
            <p>Enable it from
                <a href="${frappe.utils.generate_route({ type: "DocType", name: ToolboxSettings })}">
                    ${ToolboxSettings}
                </a>
            </p>
        </div>`);
        return;
    } else {
        $(page.container.find(".page-content")[0]).empty();
    }

    let indexes = await getIndexes(true);
    createIndexesShortcut(indexes);

    let tables  = await getTables();
    createTablesChart(tables);

    let sqlStats = await getSQLStats();
    createSQLStatsChart(sqlStats);

    document.querySelector(
        "#page-index-manager > div.container.page-body > div.page-wrapper > div > div.row.layout-main"
    )?.remove();
}

function createTablesChart(tables) {
    let chart_card = makeCard({width: 100, id: "active-tables-chart" }).appendTo(".page-content").get(0);
    window.fchart = makeActiveTablesChart(chart_card, tables);
}

function createIndexesShortcut(indexes) {
    let shortcutCard = makeCard({
        title: `<h3>${indexes.total}</h3>`,
        subtitle: "Indexes created by ToolBox",
        id: "mariadb-indexes-shortcut",
    }).appendTo(".page-content").get(0);

    shortcutCard.innerHTML += (
        `<a href="${frappe.utils.generate_route({type: "doctype", name: "MariaDB Index"})}" class="btn btn-link sm-2">
            See All ->
        </a>`
    );
}

function createSQLStatsChart(sqlStats) {
    let line_chart = makeCard({width: 100, id: "sql-stats-chart" }).appendTo(".page-content").get(0);
    makeSQLStatsChart(line_chart, sqlStats);
}

function makeSQLStatsChart(wrapper, stats) {
    return new frappe.Chart(wrapper, {
        data: {
            'labels':  stats.map(r => r.creation),
            'datasets': [
                {'name': 'Queries Recorded', 'values': stats.map(r => r.sql_count)},
            ]
        },
        type: 'line',
        colors: ['pink'],
        title: "SQL Queries",
        height: 300,
        lineOptions: {
            regionFill: 1,
        },
    });
}

function makeActiveTablesChart(wrapper, tables) {
    return new frappe.Chart(wrapper, {
        data: {
            'labels': tables.map(r => r.name),
            'datasets': [
                {'name': 'Read', 'values': tables.map(r => r.num_read_queries)},
                {'name': 'Write', 'values': tables.map(r => r.num_write_queries)},
            ]
        },
        type: 'bar',
        colors: ['green', 'blue'],
        barOptions: {'stacked': true},
        title: "Most Active Tables",
        height: 300,
    });
}

function makeActiveTablesCard(tables) {
    return makeCard({
        title: "Most Active Tables",
        subtitle: "Tables which are most frequently queried",
        html: `<table class="table table-sm">
        <table class="table table-sm">
            <thead><tr><th scope="col">Table</th><th scope="col">Count</th></tr></thead>
            <tbody>${
                tables.map(r => `<tr><td>${r.name}</td> <td>${r.num_queries}</td> </tr>`).join("")
            }</tbody>
        </table>
        </div>
    </div>`
    });
}

async function getTables(limit = 10) {
    const { message } = await frappe.call({method: "toolbox.api.index_manager.tables", type: "GET", args: { limit }});
    return message;
}

async function getIndexes(toolbox_only = false) {
    const { message } = await frappe.call({method: "toolbox.api.index_manager.indexes", type: "GET", args: { toolbox_only }});
    return message;
}

async function getSQLStats() {
    const { message } = await frappe.call({method: "toolbox.api.index_manager.summary", type: "GET"});
    return message;
}


function makeCard(opts) {
    return $(`<div class="card m-2" style="width: ${opts.width || '18rem'}; id="${opts.id}"">
        <div class="card-body">
            ${opts.title ? `<h5 class="card-title">${opts.title}</h5>` : ""}
            ${opts.subtitle ? `<h6 class="card-subtitle mb-2 text-muted">${opts.subtitle}</h6>` : ""}
            ${opts.html || ""}
        </div>
        </div>`
    );
}