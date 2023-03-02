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
def doctype_manager_cli():
    ...


@click.group("sql-recorder")
def sql_recorder_cli():
    ...


@click.group("index-manager")
def index_manager_cli():
    ...


@click.group("sql-manager")
def sql_manager_cli():
    ...


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

    from toolbox.sql_recorder import delete_data

    with frappe.init_site(get_site(context)):
        delete_data()


@click.command("process")
@click.option("--chunk-size", default=2500, help="Number of queries to process in a single job")
@pass_context
def process_metadata(context, chunk_size: int = 2500):
    from math import ceil

    import frappe
    from frappe.utils.synchronization import filelock

    from toolbox.sql_recorder import TOOLBOX_RECORDER_FLAG, export_data
    from toolbox.utils import (
        check_dbms_compatibility,
        handle_redis_connection_error,
        process_sql_metadata_chunk,
        record_database_state,
    )

    CHUNK_SIZE = chunk_size or 2500
    SITE = get_site(context)

    with frappe.init_site(SITE), check_dbms_compatibility(
        frappe.conf
    ), handle_redis_connection_error(), filelock("process_sql_metadata", timeout=0.1):
        # stop recording queries while processing
        frappe.cache().delete_value(TOOLBOX_RECORDER_FLAG)
        KEEP_RECORDER_ON = frappe.conf.toolbox and frappe.conf.toolbox.get("recorder")
        queries = [query for request_data in export_data() for query in request_data]

        NUM_JOBS = 1
        if len(queries) > CHUNK_SIZE:
            NUM_JOBS = ceil(len(queries) / CHUNK_SIZE)

        print(f"Processing {len(queries):,} queries in {NUM_JOBS} jobs")

        if NUM_JOBS > 1:
            from multiprocessing import Pool

            frappe.connect()
            record_database_state()

            with Pool(NUM_JOBS) as p:
                for i in range(NUM_JOBS):
                    p.apply_async(
                        process_sql_metadata_chunk,
                        args=(queries[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE],),
                        kwds={"site": SITE, "setup": False, "chunk_size": CHUNK_SIZE},
                    )
                p.close()
                p.join()
        else:
            process_sql_metadata_chunk(queries, SITE, setup=True, chunk_size=CHUNK_SIZE)

        print("Done processing queries across all jobs")

        if KEEP_RECORDER_ON:
            with frappe.init_site(SITE):
                frappe.cache().set_value(TOOLBOX_RECORDER_FLAG, 1)
        else:
            print("*** SQL Recorder switched off ***")

        drop_recording.callback()


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
    from collections import defaultdict
    from itertools import groupby

    import frappe

    from toolbox.doctypes import MariaDBIndex
    from toolbox.utils import Query, QueryBenchmark, Table, get_table_id

    # optimization algorithm v1:
    # 1. Check if the tables involved are scanning entire tables (type: ALL[Worst case] and similar)
    # 2. If so, check if there are any indexes that can be used - create a new query with the indexes
    # 3. compare # of rows scanned and filtered and execution time, before and after
    #
    # TODO: Check if there is any salvageable data in the query explain - like execution time,
    # possible_keys, key_len, filtered (for existing perf & test any improvement, along w query exec
    # time ofcs), etc
    #
    # TODO: Add logging to the process

    with frappe.init_site(get_site(context)):
        frappe.connect()
        # Note: don't push occurence filter in SQL without considering that we're storing captured queries
        # and not candidates. The Query objects here represent query candidates which are reduced considering
        # parameterized queries and occurences
        ok_types = ["ALL", "index", "range", "ref", "eq_ref", "fulltext", "ref_or_null"]
        table_grouper = lambda q: q.table  # noqa: E731
        sql_qualifier = (
            (lambda q: q.occurence > sql_occurence) if sql_occurence else None
        )  # noqa: E731
        filter_map = [
            ["MariaDB Query Explain", "type", "in", ok_types],
            ["MariaDB Query Explain", "parenttype", "=", "MariaDB Query"],
        ]
        if table_name:
            filter_map.append(["MariaDB Query Explain", "table", "=", get_table_id(table_name)])

        recorded_queries = frappe.get_all(
            "MariaDB Query",
            filters=filter_map,
            fields=["query", "parameterized_query", "query_explain.table", "occurence"],
            order_by=None,
            distinct=True,
        )
        recorded_queries = sorted(
            recorded_queries, key=table_grouper
        )  # required for groupby to work

        for table_id, _queries in groupby(recorded_queries, key=table_grouper):
            table = Table(id=table_id)

            if not table.name or not table.exists():
                # First condition is likely due to ghost data in MariaDB Query Explain  - this might be a bug, or require a cleanup
                # Second is for derived and temporary tables                            - this is expected
                if verbose:
                    print(f"Skipping {table_id} - table not found")
                continue

            # combine occurences from parameterized query candidates
            _qrys = list(_queries)
            _query_candidates = defaultdict(lambda: 0)
            for q in _qrys:
                _query_candidates[q.parameterized_query or q.query] += q.occurence

            query_candidates = [Query(*q, table=table) for q in _query_candidates.items()]
            del _query_candidates

            # generate index candidates from the query candidates, qualify them
            index_candidates = table.find_index_candidates(
                query_candidates, qualifier=sql_qualifier
            )
            qualified_index_candidates = table.qualify_index_candidates(index_candidates)

            if not qualified_index_candidates:
                if verbose:
                    print(f"No qualified index candidates for {table.name}")
                continue

            # Generate indexes from qualified index candidates, test gains
            if skip_backtest:
                failed_ics = MariaDBIndex.create(
                    table.name, qualified_index_candidates, verbose=verbose
                )
                continue

            with QueryBenchmark(
                index_candidates=qualified_index_candidates, verbose=verbose
            ) as qbm:
                failed_ics = MariaDBIndex.create(
                    table.name, qualified_index_candidates, verbose=verbose
                )

            # Drop indexes that don't improve query metrics
            redundant_indexes = [
                qualified_index_candidates[q_id]
                for q_id, ctx in qbm.get_unchanged_results()
                if qualified_index_candidates[q_id] not in failed_ics
            ]
            MariaDBIndex.drop(table.name, redundant_indexes, verbose=verbose)

        # TODO: Show summary of changes


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
