from contextlib import contextmanager
from typing import TYPE_CHECKING

from click import secho

if TYPE_CHECKING:
    from toolbox.doctypes import MariaDBQuery


def record_table(table: str) -> str:
    from html import escape

    import frappe

    table = table or "NULL"

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


def record_query(query: str, p_query: str | None = None) -> "MariaDBQuery":
    import frappe

    if query_name := frappe.get_all("MariaDB Query", {"query": query}, limit=1):
        query_record = frappe.get_doc("MariaDB Query", query_name[0])
        query_record.parameterized_query = p_query
        query_record.occurence += 1
        return query_record

    query_record = frappe.new_doc("MariaDB Query")
    query_record.query = query
    query_record.parameterized_query = p_query
    query_record.occurence = 1

    return query_record


def record_database_state():
    import frappe

    for tbl in frappe.db.get_tables(cached=False):
        if not frappe.db.exists("MariaDB Table", {"_table_name": tbl}):
            table_record = frappe.new_doc("MariaDB Table")
            table_record._table_name = tbl
            table_record.insert()


@contextmanager
def check_dbms_compatibility(conf):
    if conf.db_type != "mariadb":
        secho(f"WARN: This command might not be compatible with {conf.db_type}", fg="yellow")
    yield


@contextmanager
def handle_redis_connection_error():
    from redis.exceptions import ConnectionError

    try:
        yield
    except ConnectionError as e:
        secho(f"ERROR: {e}", fg="red")
        secho("NOTE: Make sure Redis services are running", fg="yellow")


def process_sql_metadata_chunk(
    queries: list[dict],
    site: str,
    setup: bool = True,
    chunk_size: int = 5_000,
    auto_commit: bool = True,
):
    import frappe
    from sqlparse import format as sql_format

    with frappe.init_site(site):
        sql_count = 0
        granularity = chunk_size // 100
        frappe.connect()

        TOOLBOX_TABLES = set(frappe.get_all("DocType", {"module": "Toolbox"}, pluck="name"))

        if setup:
            record_database_state()

        for query_info in queries:
            query: str = query_info["query"]

            if not query.lower().startswith(("select", "insert", "update", "delete")):
                continue

            parameterized_query: str = query_info["args"][0]

            # should check warnings too? unsure at this point
            explain_data = frappe.db.sql(f"EXPLAIN EXTENDED {query}", as_dict=True)

            if not explain_data:
                print(f"Cannot explain query: {query}")
                continue

            # Note: Desk doesn't like Queries with whitespaces in long text for show title in links for forms
            # Better to strip them off and format on demand
            query_record = record_query(
                sql_format(query, strip_whitespace=True, keyword_case="upper"),
                p_query=parameterized_query,
            )
            for explain in explain_data:
                # skip Toolbox internal queries
                if explain["table"] not in TOOLBOX_TABLES:
                    query_record.apply_explain(explain)
            query_record.save()

            sql_count += 1
            # Show approximate progress
            print(
                f"Processed ~{round(sql_count / granularity) * granularity:,} queries per job"
                + " " * 5,
                end="\r",
            )

            if auto_commit and frappe.db.transaction_writes > chunk_size:
                frappe.db.commit()

        if auto_commit:
            frappe.db.commit()
