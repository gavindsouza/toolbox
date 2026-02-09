from collections import defaultdict
from itertools import groupby

import frappe

from toolbox.doctypes import MariaDBIndex
from toolbox.utils import Query, QueryBenchmark, Table, get_table_id


def process_index_manager(
    table_name: str = None,
    sql_occurrence: int = 0,
    skip_backtest: bool = False,
    verbose: bool = False,
):
    # optimization algorithm v1:
    # 1. Check if the tables involved are scanning entire tables (type: ALL[Worst case] and similar)
    # 2. If so, check if there are any indexes that can be used - create a new query with the indexes
    # 3. compare # of rows scanned and filtered and execution time, before and after
    #
    # Note: don't push occurrence filter in SQL without considering that we're storing captured queries
    # and not candidates. The Query objects here represent query candidates which are reduced considering
    # parameterized queries and occurrences
    ok_types = ["ALL", "index", "range", "ref", "eq_ref", "fulltext", "ref_or_null"]
    table_grouper = lambda q: q.table  # noqa: E731
    sql_qualifier = (
        (lambda q: q.occurrence > sql_occurrence) if sql_occurrence else None
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
        fields=["query", "parameterized_query", "query_explain.table", "occurrence"],
        order_by=None,
        distinct=True,
    )
    recorded_queries = sorted(recorded_queries, key=table_grouper)  # required for groupby to work

    for table_id, _queries in groupby(recorded_queries, key=table_grouper):
        table = Table(id=table_id)

        if not table.name or not table.exists():
            if verbose:
                frappe.logger("toolbox").debug(f"Skipping {table_id} - table not found")
            continue

        # combine occurrences from parameterized query candidates
        _qrys = list(_queries)
        _query_candidates = defaultdict(lambda: defaultdict(int))

        for q in _qrys:
            reduced_key = q.parameterized_query or q.query
            _query_candidates[reduced_key]["sql"] = q.query
            _query_candidates[reduced_key]["occurrence"] += q.occurrence

        query_candidates = [Query(**q, table=table) for q in _query_candidates.values()]
        del _query_candidates

        # generate index candidates from the query candidates, qualify them
        index_candidates = table.find_index_candidates(query_candidates, qualifier=sql_qualifier)
        qualified_index_candidates = table.qualify_index_candidates(index_candidates)

        if not qualified_index_candidates:
            if verbose:
                frappe.logger("toolbox").debug(f"No qualified index candidates for {table.name}")
            continue

        # Generate indexes from qualified index candidates, test gains
        if skip_backtest:
            failed_ics = MariaDBIndex.create(
                table.name, qualified_index_candidates, verbose=verbose
            )
            continue

        with QueryBenchmark(index_candidates=qualified_index_candidates, verbose=verbose) as qbm:
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

        total_indexes_created = len(qualified_index_candidates) - len(failed_ics)
        total_indexes_dropped = len(redundant_indexes)

        if verbose and (total_indexes_created != total_indexes_dropped):
            logger = frappe.logger("toolbox")
            logger.info(f"Optimized {table.name}")
            logger.info(f"Indexes created: {total_indexes_created}")
            logger.info(f"Indexes dropped: {total_indexes_dropped}")
