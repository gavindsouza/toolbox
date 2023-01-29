# bench --site business.localhost start-recording
# bench --site business.localhost execute frappe.recorder.export_data
# changed `save 30 100` in redis_cache from `save ""` to persist data

from contextlib import contextmanager

import click
from frappe.commands import get_site, pass_context


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
    from html import escape

    import frappe
    from frappe.recorder import export_data

    with frappe.init_site(get_site(context)), check_dbms_compatibility(
        frappe.conf
    ), handle_redis_connection_error():
        sql_count = 0
        frappe.connect()
        exported_data = export_data()

        for tbl in frappe.db.get_tables(cached=False):
            if not frappe.db.exists("MariaDB Table", {"_table_name": tbl}):
                table_record = frappe.new_doc("MariaDB Table")
                table_record._table_name = tbl
                table_record.insert()

        for func_call in exported_data:
            for query_info in func_call["calls"]:

                if explain := query_info["explain_result"]:
                    explain = explain[0]
                    query = query_info["query"]

                    if not explain["table"]:
                        continue

                    if table_name := frappe.get_all(
                        "MariaDB Table", {"_table_name": explain["table"]}, limit=1, pluck="name"
                    ):
                        table_name = table_name[0]
                    # handle derived tables & such
                    elif table_name := frappe.get_all(
                        "MariaDB Table",
                        {"_table_name": escape(explain["table"])},
                        limit=1,
                        pluck="name",
                    ):
                        table_name = table_name[0]
                    # generate temporary table names
                    else:
                        table_record = frappe.new_doc("MariaDB Table")
                        table_record._table_name = explain["table"]
                        table_record.insert()
                        table_name = table_record.name

                    if query_name := frappe.get_all("MariaDB Query", {"query": query}, limit=1):
                        query_record = frappe.get_doc("MariaDB Query", query_name[0])
                        query_record.update(
                            {
                                "type": explain["type"],
                                "possible_keys": explain["possible_keys"],
                                "key": explain["key"],
                                "key_len": explain["key_len"],
                                "ref": explain["ref"],
                                "rows": explain["rows"],
                                "extra": explain["Extra"],
                            }
                        )
                        query_record.occurence += 1
                        query_record.save()
                    else:
                        query_record = frappe.new_doc("MariaDB Query")
                        query_record.update(
                            {
                                "query": query,
                                "table": table_name,
                                "type": explain["type"],
                                "possible_keys": explain["possible_keys"],
                                "key": explain["key"],
                                "key_len": explain["key_len"],
                                "ref": explain["ref"],
                                "rows": explain["rows"],
                                "extra": explain["Extra"],
                                "occurence": 1,
                            }
                        )
                        query_record.insert()
                    sql_count += 1

            print(f"Write Transactions: {frappe.db.transaction_writes}", end="\r")

        print(f"Processed {sql_count} queries" + " " * 5)
        frappe.db.commit()
        delete_recording.callback()


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
