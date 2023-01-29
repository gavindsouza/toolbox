# bench --site business.localhost start-recording
# bench --site business.localhost execute frappe.recorder.export_data
# changed `save 30 100` in redis_cache from `save ""` to persist data

from contextlib import contextmanager
from typing import TYPE_CHECKING

import click
from frappe.commands import get_site, pass_context

if TYPE_CHECKING:
    from toolbox.doctypes import MariaDBQuery


@click.command("delete-recording")
@pass_context
def delete_recording(context):
    import frappe
    from frappe.recorder import delete

    with frappe.init_site(get_site(context)):
        frappe.connect()
        delete()


@click.command("process-sql-metadata")
@pass_context
def process_sql_metadata(context):
    import frappe
    from frappe.recorder import export_data

    with frappe.init_site(get_site(context)), check_dbms_compatibility(
        frappe.conf
    ), handle_redis_connection_error():
        frappe.connect()

        sql_count = 0
        TOOLBOX_TABLES = frappe.get_all("DocType", {"module": "Toolbox"}, pluck="name")

        record_database_state()
        exported_data = export_data()

        for func_call in exported_data:
            for query_info in func_call["calls"]:
                query = query_info["query"]

                if query.lower().startswith(("start", "commit", "rollback")):
                    continue

                if explain := query_info["explain_result"]:
                    # TODO: Handle multiple explain lines of explain output
                    query_record = record_query(query)

                    for _explain in explain:
                        # skip Toolbox internal queries
                        if _explain["table"] in TOOLBOX_TABLES:
                            continue
                        if not _explain["table"]:
                            print(f"Skipping query: {query}")
                            continue

                        table_id = record_table(_explain["table"])
                        query_record.apply_explain(_explain, table_id)
                        query_record.save()

                    sql_count += 1

                else:
                    print(f"Skipping query: {query}")
                    continue

            print(f"Write Transactions: {frappe.db.transaction_writes}", end="\r")

        print(f"Processed {sql_count} queries" + " " * 5)
        frappe.db.commit()
        delete_recording.callback()


def record_database_state():
    import frappe

    for tbl in frappe.db.get_tables(cached=False):
        if not frappe.db.exists("MariaDB Table", {"_table_name": tbl}):
            table_record = frappe.new_doc("MariaDB Table")
            table_record._table_name = tbl
            table_record.insert()


def record_table(table: str) -> str:
    from html import escape

    import frappe

    if table_id := frappe.get_all("MariaDB Table", {"_table_name": table}, limit=1, pluck="name"):
        table_id = table_id[0]
    # handle derived tables & such
    elif table_id := frappe.get_all(
        "MariaDB Table",
        {"_table_name": escape(table)},
        limit=1,
        pluck="name",
    ):
        table_id = table_id[0]
    # generate temporary table names
    else:
        table_record = frappe.new_doc("MariaDB Table")
        table_record._table_name = table
        table_record.insert()
        table_id = table_record.name

    return table_id


def record_query(query: str) -> "MariaDBQuery":
    import frappe

    if query_name := frappe.get_all("MariaDB Query", {"query": query}, limit=1):
        query_record = frappe.get_doc("MariaDB Query", query_name[0])
        query_record.occurence += 1
        return query_record

    query_record = frappe.new_doc("MariaDB Query")
    query_record.query = query
    query_record.occurence = 1

    return query_record


@contextmanager
def check_dbms_compatibility(conf):
    if conf.db_type != "mariadb":
        click.secho(f"WARN: This command might not be compatible with {conf.db_type}", fg="yellow")
    yield


@contextmanager
def handle_redis_connection_error():
    from redis.exceptions import ConnectionError

    try:
        yield
    except ConnectionError as e:
        click.secho(f"ERROR: {e}", fg="red")
        click.secho("NOTE: Make sure Redis services are running", fg="yellow")


@click.command("cleanup-sql-metadata")
@pass_context
def cleanup_sql_metadata(context):
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


commands = [process_sql_metadata, delete_recording, cleanup_sql_metadata]
