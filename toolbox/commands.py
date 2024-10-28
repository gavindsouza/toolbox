# Available commands:
# bench --site e.localhost sql-profiler start
# bench --site e.localhost sql-profiler stop
# bench --site e.localhost sql-profiler process
# bench --site e.localhost sql-profiler drop
# bench --site e.localhost sql-profiler optimize

# Note: changed `save 30 100` in redis_cache from `save ""` to persist data over bench restarts

import click
from frappe.commands import get_site, pass_context


@click.group("doctype-manager")
def doctype_manager_cli(): ...


@click.group("sql-recorder")
def sql_recorder_cli(): ...


@click.group("index-manager")
def index_manager_cli(): ...


@click.group("sql-manager")
def sql_manager_cli(): ...


@click.command("start")
@pass_context
def start_recording(context):
    import frappe

    from toolbox.sql_recorder import TOOLBOX_RECORDER_FLAG

    with frappe.init_site(get_site(context)):
        frappe.cache().set_value(TOOLBOX_RECORDER_FLAG, 1)


@click.command("stop")
@pass_context
def stop_recording(context):
    import frappe

    from toolbox.sql_recorder import TOOLBOX_RECORDER_FLAG

    with frappe.init_site(get_site(context)):
        frappe.cache().delete_value(TOOLBOX_RECORDER_FLAG)


@click.command("drop")
@pass_context
def drop_recording(context):
    import frappe

    from toolbox.sql_recorder import TOOLBOX_RECORDER_DATA

    with frappe.init_site(get_site(context)):
        frappe.cache.delete_value(TOOLBOX_RECORDER_DATA)


@click.command("process")
@pass_context
def process_metadata(context):
    import frappe

    from toolbox.toolbox.doctype.toolbox_settings.toolbox_settings import process_sql_recorder
    from toolbox.utils import check_dbms_compatibility, handle_redis_connection_error

    SITE = get_site(context)

    with frappe.init_site(SITE), check_dbms_compatibility(
        frappe.conf
    ), handle_redis_connection_error():
        frappe.connect()
        process_sql_recorder()
        frappe.db.commit()


@click.command("cleanup")
@pass_context
def cleanup_metadata(context):
    from collections import Counter

    import frappe

    with frappe.init_site(get_site(context)):
        frappe.connect()
        mdb_qry = frappe.qb.DocType("MariaDB Query")
        candidates = frappe.get_all("MariaDB Query", pluck="query")
        c = Counter()
        c.update(candidates)

        candidates = [q for q, count in c.most_common(10_000) if count > 1]

        for query in candidates:
            all_occurences = frappe.get_all("MariaDB Query", {"query": query}, pluck="name")
            pick = all_occurences[0]
            frappe.qb.from_(mdb_qry).where(mdb_qry.query == query).where(
                mdb_qry.name != pick
            ).delete().run()
            doc = frappe.get_doc("MariaDB Query", pick)
            doc.occurence = len(all_occurences)
            doc.save()
            frappe.db.commit()


@click.command("show-toolbox-indexes")
@click.option("--extra", is_flag=True, help="Show extra columns")
@pass_context
def show_toolbox_indexes(context, extra: bool = False):
    import frappe
    from frappe.utils.commands import render_table

    from toolbox.doctypes import MariaDBIndex

    with frappe.init_site(get_site(context)):
        frappe.connect()
        ti = MariaDBIndex.get_indexes(toolbox_only=True)
        if not ti:
            print("No indexes found")
            return

        for d in ti:
            for attr in [
                "frappe_table_id",
                "name",
                "owner",
                "modified_by",
                "creation",
                "modified",
            ]:
                del d[attr]

        if not extra:
            for d in ti:
                for attr in [
                    "non_unique",
                    "index_type",
                    "collation",
                ]:
                    del d[attr]

        ti.sort(key=lambda d: (d["table"], d["key_name"], d["seq_id"]))

        headers = ti[0].keys()
        data = [list(row.values()) for row in ti]
        render_table([headers] + data)


@click.command("drop-toolbox-indexes")
@click.option("--dry-run", is_flag=True, help="Show indexes that would be dropped")
@pass_context
def drop_toolbox_indexes(context, dry_run: bool = False):
    import frappe

    from toolbox.doctypes import MariaDBIndex

    with frappe.init_site(get_site(context)):
        frappe.connect()
        tables = {x["table"] for x in MariaDBIndex.get_indexes(toolbox_only=True)}

        for table in tables:
            if not dry_run:
                MariaDBIndex.drop_toolbox_indexes(table)

        if not tables:
            print("No toolbox indexes found")
        else:
            print(f"Dropped indexes for {','.join(tables)}")


@click.command("optimize")
@click.option("--table", "table_name", help="Optimize SQL for a given table")
@click.option("--sql-occurence", help="Minimum occurence as qualifier for optimization", type=int)
@click.option("--skip-backtest", is_flag=True, help="Skip backtesting the query")
@click.option("--verbose", is_flag=True, help="Increase verbosity of output")
@pass_context
def optimize_indexes(
    context,
    sql_occurence: int | None,
    table_name: str = None,
    skip_backtest: bool = False,
    verbose: bool = False,
):
    import frappe

    from toolbox.toolbox.doctype.toolbox_settings.toolbox_settings import process_index_manager

    with frappe.init_site(get_site(context)):
        frappe.connect()
        process_index_manager(
            table_name=table_name,
            sql_occurence=sql_occurence,
            skip_backtest=skip_backtest,
            verbose=verbose,
        )
        frappe.db.commit()


@click.command("trace")
@click.argument("status", type=click.Choice(["on", "off", "status", "purge", "draw"]))
@click.option("--doctypes", "-d", "doctype_names", help="Add DocTypes to trace list")
@pass_context
def trace_doctypes(context, status=str | None, doctype_names: list[str] | None = None):
    import frappe

    import toolbox.doctype_flow as df

    with frappe.init_site(get_site(context)):
        frappe.connect()
        doctypes = [d.strip() for d in (doctype_names or "").split(",") if d.strip()]

        match status:
            case "on":
                df.trace(doctypes)
            case "off":
                df.untrace(doctypes)
            case "purge":
                df.purge(doctypes)
            case "draw":
                df.render()
            case _:
                print(df.status())


doctype_manager_cli.add_command(trace_doctypes)

sql_recorder_cli.add_command(start_recording)
sql_recorder_cli.add_command(stop_recording)
sql_recorder_cli.add_command(drop_recording)

index_manager_cli.add_command(show_toolbox_indexes)
index_manager_cli.add_command(drop_toolbox_indexes)
index_manager_cli.add_command(optimize_indexes)

sql_manager_cli.add_command(process_metadata)
sql_manager_cli.add_command(cleanup_metadata)

commands = [sql_recorder_cli, index_manager_cli, sql_manager_cli, doctype_manager_cli]
