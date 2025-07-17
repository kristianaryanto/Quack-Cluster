import os
import glob
import ray
import asyncio
import uuid
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import sqlglot
from sqlglot import exp, parse_one
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from sqlglot.errors import ParseError
from contextlib import asynccontextmanager
from collections import defaultdict
from .worker import DuckDBWorker
from .settings import settings

NUM_SHUFFLE_PARTITIONS = 4
BROADCAST_THRESHOLD_MB = 0

# --- Lifespan Manager ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    ray.init(address="auto", namespace="quack-cluster", ignore_reinit_error=True)
    print("✅ Ray connected.")
    yield
    print("Ray disconnecting...")

app = FastAPI(title="Quack-Cluster", lifespan=lifespan)

class QueryRequest(BaseModel):
    sql: str

# --- Core Recursive Execution Logic ---
async def execute_recursive(query_ast: exp.Expression, con: duckdb.DuckDBPyConnection):
    """
    Mengeksekusi kueri secara rekursif, menyelesaikan CTE dan semua jenis subquery terlebih dahulu.
    """
    if hasattr(query_ast, 'with_expressions') and query_ast.with_expressions:
        rewritten_ast = query_ast.copy()
        for cte in rewritten_ast.with_expressions:
            print(f"➡️  Resolving CTE: '{cte.alias}'")
            cte_result_table = await execute_recursive(cte.this, con)
            con.register(cte.alias, cte_result_table)
        rewritten_ast.set('with', None)
        query_ast = rewritten_ast

    for subquery in query_ast.find_all(exp.Subquery):
        subquery_alias = subquery.alias_or_name or f"_subquery_{uuid.uuid4().hex[:8]}"
        print(f"➡️  Resolving subquery: '{subquery_alias}'")
        
        subquery_result_table = await execute_recursive(subquery.this, con)

        if isinstance(subquery.parent, (exp.From, exp.Join, exp.Table)):
            con.register(subquery_alias, subquery_result_table)
            subquery.replace(exp.to_table(subquery_alias))
        
        # [FIX] Logika materialisasi klausa IN yang diperbaiki dan lebih kuat
        elif isinstance(subquery.parent, exp.In):
            if subquery_result_table.num_columns > 0 and subquery_result_table.num_rows > 0:
                # Ambil daftar nilai dari kolom pertama tabel hasil subquery
                values = subquery_result_table.columns[0].to_pylist()
                
                # Buat daftar ekspresi literal sqlglot dari nilai-nilai tersebut
                literal_expressions = []
                for v in values:
                    if isinstance(v, str):
                        literal_expressions.append(exp.Literal.string(v))
                    elif isinstance(v, (int, float)):
                        literal_expressions.append(exp.Literal.number(v))
                    # Tambahkan tipe lain jika perlu
                
                # Ganti node subquery dengan Tuple yang berisi literal
                print(f"   - Materializing IN clause with values: {values}")
                subquery.replace(exp.Tuple(expressions=literal_expressions))
            else:
                # Ganti dengan (NULL) agar klausa IN menjadi '... IN (NULL)' yang selalu salah
                subquery.replace(exp.Tuple(expressions=[exp.Null()]))

    print(f"✅ Executing simplified query: {query_ast.sql(dialect='duckdb')}")
    return await execute_simplified_query(query_ast, con)

# --- Main Query Execution Logic ---
async def execute_simplified_query(parsed_query: exp.Expression, final_con: duckdb.DuckDBPyConnection) -> pa.Table:
    """
    Menjalankan kueri SQL yang telah disederhanakan dan diparsing secara terdistribusi.
    """
    all_tables_in_query = [t.this.name for t in parsed_query.find_all(exp.Table)]
    registered_tables = {row[0] for row in final_con.execute("PRAGMA show_tables;").fetchall()}
    
    if all_tables_in_query and all(table_name in registered_tables for table_name in all_tables_in_query):
        print("   - All tables are in-memory, executing directly.")
        return final_con.execute(parsed_query.sql(dialect="duckdb")).fetch_arrow_table()

    join_expression = parsed_query.find(exp.Join)
    
    if join_expression:
        # [FIX] Typo diperbaiki: exp.CrossJoin -> exp.CrossJoin
        if isinstance(join_expression, exp.CrossJoin):
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
        else: # USING clause
            key_name = join_using_clause.expressions[0].this.name
            left_key = right_key = key_name

        print(f"✅ Strategy chosen: SHUFFLE JOIN (type: {join_expression.kind or 'INNER'})")
        left_files = glob.glob(os.path.join(settings.DATA_DIR, f"{left_table_name}.*"))
        right_files = glob.glob(os.path.join(settings.DATA_DIR, f"{right_table_name}.*"))
        if not left_files or not right_files:
            raise FileNotFoundError(f"Data for join tables '{left_table_name}' or '{right_table_name}' not found.")
        
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
            join_workers[i].join_partitions.remote(
                partitions_for_joiners[i]['left'], partitions_for_joiners[i]['right'],
                worker_sql_str, left_table_alias, right_table_alias
            ) for i in range(NUM_SHUFFLE_PARTITIONS) if i in partitions_for_joiners
        ]
        partial_results_list = await asyncio.gather(*join_tasks)
        valid_partials = [tbl for tbl in partial_results_list if tbl.num_rows > 0]
        if not valid_partials: return pa.Table.from_pydict({})
        
        partial_results_table = pa.concat_tables(valid_partials)
        final_con.register("partial_results", partial_results_table)
        
        if parsed_query.find(exp.AggFunc):
            final_selects, final_group_bys = [], []
            if group_by_node := parsed_query.find(exp.Group):
                for expr in group_by_node.expressions:
                    col_name = expr.name
                    final_group_bys.append(col_name)
                    final_selects.append(col_name)
            for expr in parsed_query.find(exp.Select).expressions:
                if expr.name in final_group_bys: continue
                alias = expr.alias_or_name
                agg_func = expr.find(exp.AggFunc)
                if isinstance(agg_func, (exp.Count, exp.Sum)):
                    final_selects.append(f"SUM({alias}) AS {alias}")
            final_select_str = ", ".join(final_selects)
            final_group_by_str = f" GROUP BY {', '.join(final_group_bys)}" if final_group_bys else ""
            final_agg_query = f"SELECT {final_select_str} FROM partial_results{final_group_by_str}"
        else:
            final_agg_query = "SELECT * FROM partial_results"

        if order_by := parsed_query.find(exp.Order):
            rewritten_order = order_by.copy()
            for col in rewritten_order.find_all(exp.Column):
                col.set('table', None)
            final_agg_query += f" {rewritten_order.sql()}"
        if limit := parsed_query.find(exp.Limit):
            final_agg_query += f" {limit.sql()}"
            
        print(f"✅ Coordinator executing final query: {final_agg_query}")
        return final_con.execute(final_agg_query).fetch_arrow_table()

    # BLOK NON-JOIN
    table_expression = parsed_query.find(exp.Table)
    if not table_expression:
        return final_con.execute(parsed_query.sql(dialect="duckdb")).fetch_arrow_table()

    table_name = table_expression.this.name
    file_pattern = os.path.join(settings.DATA_DIR, f"{table_name}.*")
    all_files = glob.glob(file_pattern)

    if not all_files:
        raise FileNotFoundError(f"Table '{table_name}' not found on disk matching pattern '{file_pattern}'.")

    where_str = ""
    if where_clause := parsed_query.find(exp.Where):
        where_str = f" {where_clause.sql(dialect='duckdb')}"
    
    distributed_sql_template = f"SELECT * FROM read_parquet({{files}}){where_str}"

    final_query_ast = parsed_query.copy()
    # Ganti hanya tabel utama dalam kueri, bukan semua tabel.
    final_query_ast.find(exp.Table).replace(parse_one("combined_arrow_table"))
    final_query = final_query_ast.sql(dialect="duckdb")

    print(f"   - Worker template: {distributed_sql_template}")
    print(f"   - Coordinator query: {final_query}")

    actors = [DuckDBWorker.remote() for _ in all_files]
    tasks = [actors[i].query.remote(distributed_sql_template, [all_files[i]]) for i in range(len(all_files))]
    partial_results = await asyncio.gather(*tasks)
    valid_results = [r for r in partial_results if r is not None and r.num_rows > 0]
    if not valid_results:
        return pa.Table.from_pydict({})

    combined_arrow_table = pa.concat_tables(valid_results)
    final_con.register("combined_arrow_table", combined_arrow_table)
    return final_con.execute(final_query).fetch_arrow_table()


# --- FastAPI Endpoint ---
@app.post("/query")
async def execute_query(request: QueryRequest):
    try:
        try:
            parsed_query = parse_one(request.sql, read="duckdb")
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid SQL syntax: {e}")

        main_con = duckdb.connect(database=':memory:')
        final_arrow_table = await execute_recursive(parsed_query, main_con)
        main_con.close()
        
        final_result_df = final_arrow_table.to_pandas()
        return {"result": final_result_df.to_dict(orient="records")}

    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except (ValueError, NotImplementedError, duckdb.BinderException) as e:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"An unhandled error occurred: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))