import os
import glob
import ray
import asyncio
import uuid
# from fastapi import FastAPI, HTTPException, status
from fastapi import FastAPI, HTTPException, Response, Query, status
from pydantic import BaseModel
import sqlglot
from sqlglot import exp, parse_one
import duckdb
import pyarrow as pa
from sqlglot.errors import ParseError
from contextlib import asynccontextmanager
from collections import defaultdict
from .worker import DuckDBWorker
from .settings import settings
import logging
NUM_SHUFFLE_PARTITIONS = 4
BROADCAST_THRESHOLD_MB = 0
import pyarrow.ipc as ipc
# from fastapi import Response # Import Response
from typing import Optional

logger = logging.getLogger(__name__)
# --- Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initializes Ray connection on startup."""
    ray.init(address="auto", namespace="quack-cluster", ignore_reinit_error=True)
    print("✅ Ray connected.")
    yield
    print("Ray disconnecting...")

app = FastAPI(title="Quack-Cluster", lifespan=lifespan)

class QueryRequest(BaseModel):
    sql: str

# --- Core Recursive Execution Logic ---

# --- Core Execution Logic ---

async def resolve_and_execute(query_ast: exp.Expression, con: duckdb.DuckDBPyConnection) -> pa.Table:
    """
    Recursively resolves and executes a query AST by correctly handling CTEs
    and subqueries before final execution.
    """
    if not query_ast:
        raise ValueError("Received an empty AST node for execution.")

    # --- START OF FIX ---

    # KEY FIX 1: Check for the presence of a 'with' argument directly,
    # instead of checking the object type. This is more robust.
    if with_clause := query_ast.args.get('with'):
        # To handle recursion correctly, we operate on a copy of the main query
        # from which we'll remove the WITH clause.
        query_body = query_ast.copy()
        query_body.set('with', None)

        # Process the CTE definitions from the original AST's 'with' clause.
        for cte in with_clause.expressions:
            print(f"➡️  Resolving CTE: '{cte.alias}'")
            # Recursively execute the query that defines the CTE.
            cte_result_table = await resolve_and_execute(cte.this, con)
            # Register the result as an in-memory table.
            con.register(cte.alias, cte_result_table)
            print(f"✅ CTE '{cte.alias}' registered as in-memory table.")

        # KEY FIX 2: After resolving CTEs, recursively process the rest of the query
        # (the SELECT part without the WITH clause). Do NOT use '.this'.
        return await resolve_and_execute(query_body, con)

    # --- END OF FIX ---

    # --- NEW: HANDLE UNION / UNION ALL ---
    # Case 2: The query is a UNION. Resolve both sides and combine.
    if isinstance(query_ast, exp.Union):
        print(f"➡️  Resolving UNION ({'DISTINCT' if query_ast.args.get('distinct') else 'ALL'})")

        # Execute the left and right sides of the union in parallel.
        left_result, right_result = await asyncio.gather(
            resolve_and_execute(query_ast.this, con),
            resolve_and_execute(query_ast.expression, con)
        )

        # Combine the results on the coordinator.
        combined_table = pa.concat_tables([left_result, right_result])

        # If it's a standard UNION, we need to apply DISTINCT.
        if query_ast.args.get('distinct'):
            print("✨ Applying DISTINCT for UNION operation on coordinator.")
            con.register("union_combined_view", combined_table)
            final_result_table = con.execute("SELECT DISTINCT * FROM union_combined_view").fetch_arrow_table()
            con.unregister("union_combined_view")
            return final_result_table
        else:
            # For UNION ALL, just return the concatenated table.
            return combined_table
    # --- END OF NEW LOGIC ---


    # Case 2: No WITH clause at this level. Resolve subqueries.
    # This logic remains correct.
    for subquery in query_ast.find_all(exp.Subquery):
        subquery_alias = subquery.alias_or_name or f"_subquery_{uuid.uuid4().hex[:8]}"
        print(f"➡️  Resolving subquery: '{subquery_alias}'")

        subquery_result_table = await resolve_and_execute(subquery.this, con)

        if isinstance(subquery.parent, (exp.From, exp.Join)):
            con.register(subquery_alias, subquery_result_table)
            subquery.replace(exp.to_table(subquery_alias))
        elif isinstance(subquery.parent, exp.In):
            if subquery_result_table.num_columns > 0 and subquery_result_table.num_rows > 0:
                values = subquery_result_table.columns[0].to_pylist()
                literal_expressions = [exp.Literal.string(v) if isinstance(v, str) else exp.Literal.number(v) for v in values]
                subquery.replace(exp.Tuple(expressions=literal_expressions))
            else:
                subquery.replace(exp.Tuple(expressions=[exp.Null()]))

    # Case 3: The AST is now simple. Execute it.
    print(f"✅ Submitting simplified query for execution: {query_ast.sql(dialect='duckdb')}")
    return await execute_simplified_query(query_ast, con)

async def execute_simplified_query(parsed_query: exp.Expression, final_con: duckdb.DuckDBPyConnection) -> pa.Table:
    """
    Executes a query that has been simplified (no CTEs or subqueries).
    It decides whether to run on the coordinator or distribute to workers.
    """
    all_tables_in_query = [t.this.name for t in parsed_query.find_all(exp.Table)]

    # --- START OF FIX ---
    # Handle queries with no tables (e.g., "SELECT 1")
    if not all_tables_in_query:
        return final_con.execute(parsed_query.sql(dialect="duckdb")).fetch_arrow_table()

    # Check if all tables required by this query are already in-memory (e.g., from a CTE).
    # If so, execute directly on the coordinator and skip all distributed logic.
    registered_tables = {row[0] for row in final_con.execute("PRAGMA show_tables;").fetchall()}
    if all(table_name in registered_tables for table_name in all_tables_in_query):
        print(f"✅ All tables ({all_tables_in_query}) are in-memory. Executing directly on coordinator.")
        return final_con.execute(parsed_query.sql(dialect="duckdb")).fetch_arrow_table()
    # --- END OF FIX ---

    # If not all tables are in memory, proceed with distributed execution logic...
    join_expression = parsed_query.find(exp.Join)

    if join_expression:
        # --- DISTRIBUTED JOIN LOGIC ---
        if isinstance(join_expression, exp.Join) and join_expression.args.get('kind') == 'CROSS':
            raise NotImplementedError("Distributed CROSS JOIN is not yet fully implemented.")

        left_table_expr = parsed_query.find(exp.Table)
        left_table_name = left_table_expr.this.name
        left_table_alias = left_table_expr.alias_or_name

        right_table_expr = join_expression.this
        right_table_name = right_table_expr.this.name
        right_table_alias = right_table_expr.alias_or_name

        join_on_clause = join_expression.args.get('on')
        join_using_clause = join_expression.args.get('using')

        if not join_on_clause and not join_using_clause:
            raise ValueError("Unsupported JOIN type: No ON or USING clause found.")

        if join_on_clause:
            eq_expr = join_on_clause.find(exp.EQ)
            if not eq_expr: raise ValueError("Could not find a valid equality (=) condition in the ON clause.")
            left_key = eq_expr.left.name
            right_key = eq_expr.right.name
        else: # join_using_clause
            key_name = join_using_clause.expressions[0].this.name
            left_key = right_key = key_name

        print(f"✅ Strategy chosen: SHUFFLE JOIN (type: {join_expression.kind or 'INNER'})")
        left_files = glob.glob(os.path.join(settings.DATA_DIR, f"{left_table_name}.*"))
        right_files = glob.glob(os.path.join(settings.DATA_DIR, f"{right_table_name}.*"))
        if not left_files or not right_files:
            raise FileNotFoundError(f"Data for join tables '{left_table_name}' or '{right_table_name}' not found on disk.")

        map_workers = [DuckDBWorker.remote() for _ in range(len(left_files) + len(right_files))]
        left_tasks = [map_workers[i].partition_by_key.remote(f, left_key, NUM_SHUFFLE_PARTITIONS) for i, f in enumerate(left_files)]
        right_tasks = [map_workers[len(left_files) + i].partition_by_key.remote(f, right_key, NUM_SHUFFLE_PARTITIONS) for i, f in enumerate(right_files)]

        partitions_for_joiners = defaultdict(lambda: defaultdict(list))
        for partitioned_dict in await asyncio.gather(*left_tasks):
            for p_id, table in partitioned_dict.items():
                partitions_for_joiners[p_id]['left'].append(table)
        for partitioned_dict in await asyncio.gather(*right_tasks):
            for p_id, table in partitioned_dict.items():
                partitions_for_joiners[p_id]['right'].append(table)
        print("✅ Shuffle phase complete.")

        query_for_workers = parsed_query.copy()
        if query_for_workers.find(exp.Order): query_for_workers.find(exp.Order).pop()
        if query_for_workers.find(exp.Limit): query_for_workers.find(exp.Limit).pop()
        query_for_workers.find(exp.Table).replace(parse_one(f"{left_table_alias}_local AS {left_table_alias}"))
        query_for_workers.find(exp.Join).this.replace(parse_one(f"{right_table_alias}_local AS {right_table_alias}"))
        worker_sql_str = query_for_workers.sql(dialect="duckdb")

        join_workers = [DuckDBWorker.remote() for _ in range(NUM_SHUFFLE_PARTITIONS)]
        join_tasks = [
            join_workers[i].join_partitions.remote(partitions_for_joiners[i]['left'], partitions_for_joiners[i]['right'], worker_sql_str, left_table_alias, right_table_alias)
            for i in range(NUM_SHUFFLE_PARTITIONS) if i in partitions_for_joiners
        ]
        partial_results_list = await asyncio.gather(*join_tasks)
        valid_partials = [tbl for tbl in partial_results_list if tbl.num_rows > 0]
        if not valid_partials: return pa.Table.from_pydict({})
        partial_results_table = pa.concat_tables(valid_partials)
        final_con.register("partial_results", partial_results_table)

        # Final aggregation on coordinator
        if parsed_query.find(exp.AggFunc):
            final_selects, final_group_bys = [], []
            if group_by_node := parsed_query.find(exp.Group):
                for expr in group_by_node.expressions:
                    final_group_bys.append(expr.name)
                    final_selects.append(expr.name)
            for expr in parsed_query.find(exp.Select).expressions:
                if expr.name in final_group_bys: continue
                agg_func = expr.find(exp.AggFunc)
                if isinstance(agg_func, (exp.Count, exp.Sum)):
                    final_selects.append(f"SUM({expr.alias_or_name}) AS {expr.alias_or_name}")
            final_agg_query = f"SELECT {', '.join(final_selects)} FROM partial_results"
            if final_group_bys: final_agg_query += f" GROUP BY {', '.join(final_group_bys)}"
        else:
            final_agg_query = "SELECT * FROM partial_results"

        if order_by := parsed_query.find(exp.Order):
            rewritten_order = order_by.copy()
            for col in rewritten_order.find_all(exp.Column): col.set('table', None)
            final_agg_query += f" {rewritten_order.sql()}"
        if limit := parsed_query.find(exp.Limit): final_agg_query += f" {limit.sql()}"

        print(f"✅ Coordinator executing final query: {final_agg_query}")
        return final_con.execute(final_agg_query).fetch_arrow_table()

    else:
        # --- DISTRIBUTED NON-JOIN LOGIC (SCAN) ---
        table_expression = parsed_query.find(exp.Table)
        table_name = table_expression.this.name
        file_pattern = os.path.join(settings.DATA_DIR, f"{table_name}.*")
        all_files = glob.glob(file_pattern)
        if not all_files:
            raise FileNotFoundError(f"Table '{table_name}' not found on disk matching pattern '{file_pattern}'.")

        # Worker query: select all columns but apply WHERE filters
        where_str = ""
        if where_clause := parsed_query.find(exp.Where):
            where_str = f" {where_clause.sql(dialect='duckdb')}"
        distributed_sql_template = f"SELECT * FROM read_parquet({{files}}){where_str}"

        # Coordinator query: run the original query, but remove WHERE (already applied)
        final_query_ast = parsed_query.copy()
        if final_query_ast.find(exp.Where): final_query_ast.find(exp.Where).pop()
        final_query_ast.find(exp.Table).replace(parse_one("combined_arrow_table"))
        final_query = final_query_ast.sql(dialect="duckdb")

        print(f"   - Worker template: {distributed_sql_template}")
        print(f"   - Coordinator query: {final_query}")

        actors = [DuckDBWorker.remote() for _ in all_files]
        tasks = [actors[i].query.remote(distributed_sql_template, [all_files[i]]) for i in range(len(all_files))]
        partial_results = await asyncio.gather(*tasks)
        valid_results = [r for r in partial_results if r is not None and r.num_rows > 0]
        if not valid_results: return pa.Table.from_pydict({})

        combined_arrow_table = pa.concat_tables(valid_results)
        final_con.register("combined_arrow_table", combined_arrow_table)
        return final_con.execute(final_query).fetch_arrow_table()

# --- FastAPI Endpoint ---
@app.post("/query")
async def execute_query(
    request: QueryRequest,
    format: Optional[str] = Query("json", enum=["json", "arrow"])
):
    """The main entry point for executing SQL queries."""
    try:
        try:
            parsed_query = parse_one(request.sql, read="duckdb")
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid SQL syntax: {e}")

        main_con = duckdb.connect(database=':memory:')
        # Start the recursive execution process
        print(f"✅ {parsed_query}")
        final_arrow_table = await resolve_and_execute(parsed_query, main_con)

        # --- Format Selection Logic ---
        if format == "arrow":
            # Serialize the Arrow Table to bytes
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, final_arrow_table.schema) as writer:
                writer.write_table(final_arrow_table)
            buffer = sink.getvalue()
            content_bytes = buffer.to_pybytes()

            # Return as a streaming binary response
            return Response(
                content=content_bytes, 
                media_type="application/vnd.apache.arrow.stream"
            )
        else: # Default to JSON
            # Convert to Pandas and then to a dictionary for JSON response
            final_result_df = final_arrow_table.to_pandas()
            return {"result": final_result_df.to_dict(orient="records")}

        # final_result_df = final_arrow_table.to_pandas()
        # return {"result": final_result_df.to_dict(orient="records")}

    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except (ValueError, NotImplementedError, duckdb.BinderException, duckdb.ParserException) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException as http_exc:
        raise http_exc # Re-raise exceptions we've already handled
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"An unhandled error occurred: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An internal error occurred: {e}")