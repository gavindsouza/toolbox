# Available commands:
# bench --site e.localhost sql-profiler start
# bench --site e.localhost sql-profiler stop
# bench --site e.localhost sql-profiler process
# bench --site e.localhost sql-profiler drop
# bench --site e.localhost sql-profiler optimize

# Note: changed `save 30 100` in redis_cache from `save ""` to persist data over bench restarts

import click
from frappe.commands import get_site, pass_context


@click.group("sql-profiler")
def sql_profiler():
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

        # the following line will delete the queries that have been logged after the processing started too
        drop_recording.callback()
        print("*** SQL Recorder switched off ***")


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


@click.command("optimize")
@pass_context
def optimize_queries(context):
    ...


sql_profiler.add_command(start_recording)
sql_profiler.add_command(stop_recording)
sql_profiler.add_command(drop_recording)

sql_profiler.add_command(process_metadata)
sql_profiler.add_command(cleanup_metadata)

sql_profiler.add_command(optimize_queries)

commands = [sql_profiler]
