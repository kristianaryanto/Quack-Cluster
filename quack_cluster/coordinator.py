# quack_cluster/coordinator.py
import ray
import asyncio
import uuid
import logging
import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import time
import hashlib
import graphviz  # Import graphviz
from fastapi import FastAPI, HTTPException, Response, Query, status
from pydantic import BaseModel
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError
from contextlib import asynccontextmanager
from typing import Optional, Any, Dict, Tuple, List

from .planner import Planner
from .executor import Executor
from .settings import settings
from .execution_plan import BasePlan, DistributedScanPlan, DistributedShuffleJoinPlan, LocalExecutionPlan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- KELAS UNTUK MENGELOLA CACHE (Tidak berubah) ---
class QueryCache:
    """Cache in-memory sederhana dengan dukungan TTL dan thread-safe."""
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._ttl: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key in self._cache:
                if time.time() < self._ttl[key]:
                    logger.info(f"✅ Cache hit for key: {key[:10]}...")
                    return self._cache[key]
                else:
                    logger.info(f"Cache expired for key: {key[:10]}...")
                    del self._cache[key]
                    del self._ttl[key]
            return None

    async def set(self, key: str, value: Any, ttl_seconds: int):
        async with self._lock:
            self._cache[key] = value
            self._ttl[key] = time.time() + ttl_seconds
            logger.info(f"Cache set for key: {key[:10]}... with TTL: {ttl_seconds}s")

query_cache = QueryCache()


@asynccontextmanager
async def lifespan(app: FastAPI):
    ray.init(address="auto", namespace="quack-cluster", ignore_reinit_error=True)
    logger.info("✅ Ray connected.")
    yield
    logger.info("Ray disconnecting...")

app = FastAPI(title="Quack-Cluster", lifespan=lifespan)

class QueryRequest(BaseModel):
    sql: str

# --- FUNGSI BARU UNTUK VISUALISASI PLAN ---

def visualize_plan(plan: BasePlan) -> bytes:
    """
    Membuat visualisasi dari execution plan menggunakan Graphviz.
    """
    dot = graphviz.Digraph(comment='Query Execution Plan')
    dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue')
    dot.attr(rankdir='TB', label=f'Execution Plan: {plan.plan_type.upper()}', labelloc='t', fontsize='20')

    coordinator_color = 'skyblue'
    worker_color = 'lightgreen'
    data_color = 'khaki'

    # Gaya node umum
    coordinator_node_attrs = {'fillcolor': coordinator_color}
    worker_node_attrs = {'fillcolor': worker_color}
    data_node_attrs = {'shape': 'parallelogram', 'fillcolor': data_color}

    if isinstance(plan, LocalExecutionPlan):
        dot.node('Coordinator', 'Coordinator\n(Local Execution)', **coordinator_node_attrs)
        dot.node('SQL', f"Execute SQL:\n{plan.query_ast.sql(pretty=True)}", shape='note', fillcolor='white')
        dot.node('Result', 'Final Result', shape='ellipse', fillcolor='lightgray')
        dot.edge('Coordinator', 'SQL')
        dot.edge('SQL', 'Result')

    elif isinstance(plan, DistributedScanPlan):
        with dot.subgraph(name='cluster_workers') as c:
            c.attr(label='Phase 1: Parallel Scan (Workers)')
            c.attr(style='filled', color='lightgrey')
            c.node('Worker_Scan', f"Scan & Filter Workers (1..N)\nTemplate: {plan.worker_query_template}", **worker_node_attrs)
            c.node('Data_Files', f"Files for\n'{plan.table_name}'", **data_node_attrs)
            c.edge('Data_Files', 'Worker_Scan')

        with dot.subgraph(name='cluster_coordinator') as c:
            c.attr(label='Phase 2: Final Aggregation (Coordinator)')
            c.attr(style='filled', color='lightgrey')
            final_query = plan.query_ast.copy()
            if final_query.find(exp.Where): final_query.find(exp.Where).pop()
            final_query.find(exp.Table).replace(parse_one("combined_arrow_table"))
            c.node('Coordinator_Agg', f"Coordinator\nFinal Aggregation\n{final_query.sql(pretty=True)}", **coordinator_node_attrs)

        dot.node('Result', 'Final Result', shape='ellipse', fillcolor='lightgray')
        dot.edge('Worker_Scan', 'Coordinator_Agg', label='Partial Results')
        dot.edge('Coordinator_Agg', 'Result')

    elif isinstance(plan, DistributedShuffleJoinPlan):
        # Fase 1: Map/Partition
        with dot.subgraph(name='cluster_partition') as c:
            c.attr(label='Phase 1: Partition by Key (Map)')
            c.attr(style='filled', color='lightgrey')
            c.node('Left_Data', f"Files for '{plan.left_table_name}'", **data_node_attrs)
            c.node('Right_Data', f"Files for '{plan.right_table_name}'", **data_node_attrs)
            c.node('Partition_L', f"Partition Workers (L)\nKey: {plan.left_join_key}", **worker_node_attrs)
            c.node('Partition_R', f"Partition Workers (R)\nKey: {plan.right_join_key}", **worker_node_attrs)
            c.edge('Left_Data', 'Partition_L')
            c.edge('Right_Data', 'Partition_R')

        # Fase 2: Reduce/Join
        with dot.subgraph(name='cluster_join') as c:
            c.attr(label='Phase 2: Join Partitions (Reduce)')
            c.attr(style='filled', color='lightgrey')
            c.node('Joiner', f"Joiner Workers (1..{settings.NUM_SHUFFLE_PARTITIONS})\nSQL: {plan.worker_join_sql}", **worker_node_attrs)

        # Fase 3: Final Aggregation
        with dot.subgraph(name='cluster_final_agg') as c:
            c.attr(label='Phase 3: Final Aggregation (Coordinator)')
            c.attr(style='filled', color='lightgrey')
            c.node('Final_Agg', 'Coordinator\nFinal Aggregation, Order, Limit', **coordinator_node_attrs)

        dot.node('Result', 'Final Result', shape='ellipse', fillcolor='lightgray')

        dot.edge('Partition_L', 'Joiner', label=f'Shuffled Partitions (L)')
        dot.edge('Partition_R', 'Joiner', label=f'Shuffled Partitions (R)')
        dot.edge('Joiner', 'Final_Agg', label='Partial Join Results')
        dot.edge('Final_Agg', 'Result')

    # Render image ke bytes
    return dot.pipe(format='png')


# ... fungsi resolve_and_execute tidak berubah ...
async def resolve_and_execute(query_ast: exp.Expression, con: duckdb.DuckDBPyConnection) -> pa.Table:
    """
    Secara rekursif menyelesaikan CTE, UNION, dan subquery, lalu mendelegasikannya
    ke Planner dan Executor untuk eksekusi query yang sudah sederhana.
    """
    if not query_ast:
        raise ValueError("Received an empty AST node for execution.")

    # Kasus 1: WITH clause (CTE). Selesaikan CTE terlebih dahulu.
    if with_clause := query_ast.args.get('with'):
        query_body = query_ast.copy()
        query_body.set('with', None)
        for cte in with_clause.expressions:
            logger.info(f"➡️ Resolving CTE: '{cte.alias}'")
            cte_result_table = await resolve_and_execute(cte.this, con)
            con.register(cte.alias, cte_result_table)
            logger.info(f"✅ CTE '{cte.alias}' registered as in-memory table.")
        return await resolve_and_execute(query_body, con)

    # Kasus 2: UNION.
    if isinstance(query_ast, exp.Union):
        logger.info(f"➡️ Resolving UNION ({'DISTINCT' if query_ast.args.get('distinct') else 'ALL'})")
        left_result, right_result = await asyncio.gather(
            resolve_and_execute(query_ast.this, con),
            resolve_and_execute(query_ast.expression, con)
        )
        combined_table = pa.concat_tables([left_result, right_result])
        if query_ast.args.get('distinct'):
            logger.info("✨ Applying DISTINCT for UNION operation on coordinator.")
            con.register("union_combined_view", combined_table)
            final_result_table = con.execute("SELECT DISTINCT * FROM union_combined_view").fetch_arrow_table()
            con.unregister("union_combined_view")
            return final_result_table
        return combined_table

    # Kasus 3: Subquery.
    for subquery in query_ast.find_all(exp.Subquery):
        subquery_alias = subquery.alias_or_name or f"_subquery_{uuid.uuid4().hex[:8]}"
        logger.info(f"➡️ Resolving subquery: '{subquery_alias}'")
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

    # Kasus 4: AST sederhana.
    logger.info(f"✅ Simplified AST ready for planning: {query_ast.sql(dialect='duckdb')}")
    registered_tables = {row[0] for row in con.execute("PRAGMA show_tables;").fetchall()}
    plan = Planner.create_plan(query_ast, registered_tables)
    logger.info(f"✅ Plan created: {plan.plan_type}")
    return await Executor.execute(plan, con)


# --- ENDPOINT BARU ---
@app.post("/explain")
async def explain_query(request: QueryRequest):
    """
    Menganalisis SQL, membuat rencana eksekusi, dan mengembalikan
    visualisasi rencana tersebut sebagai gambar.
    """
    logger.info(f"Generating EXPLAIN plan for: {request.sql}")
    try:
        # 1. Parse SQL
        try:
            parsed_query = parse_one(request.sql, read="duckdb")
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid SQL syntax: {e}")

        # 2. Buat Plan
        # Untuk membuat plan, kita perlu tahu tabel apa yang mungkin sudah
        # ada di-memory dari CTE sebelumnya. Dalam konteks /explain, kita asumsikan tidak ada.
        plan = Planner.create_plan(parsed_query, registered_tables=set())
        logger.info(f"✅ Plan created for EXPLAIN: {plan.plan_type}")

        # 3. Visualisasikan Plan
        image_bytes = visualize_plan(plan)

        # 4. Kembalikan sebagai Response gambar
        return Response(content=image_bytes, media_type="image/png")

    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except (ValueError, NotImplementedError) as e:
        logger.exception("An error occurred during query planning for EXPLAIN")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.exception("An unhandled error occurred during EXPLAIN")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An internal error occurred: {e}")


@app.post("/query")
async def execute_query(
    request: QueryRequest,
    format: Optional[str] = Query("json", enum=["json", "arrow"])
):
    """Endpoint utama untuk eksekusi SQL, sekarang dengan caching."""
    cache_key = hashlib.sha256(f"{request.sql}:{format}".encode()).hexdigest()
    cached_data = await query_cache.get(cache_key)

    if cached_data:
        content, media_type = cached_data
        if media_type == "application/json":
            return content
        else:
            return Response(content=content, media_type=media_type)

    logger.info(f"Cache miss for key: {cache_key[:10]}... Executing query.")
    
    try:
        try:
            parsed_query = parse_one(request.sql, read="duckdb")
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid SQL syntax: {e}")

        main_con = duckdb.connect(database=':memory:')
        final_arrow_table = await resolve_and_execute(parsed_query, main_con)

        if final_arrow_table is None:
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Query execution returned no result.")

        if format == "arrow":
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, final_arrow_table.schema) as writer:
                writer.write_table(final_arrow_table)
            content = sink.getvalue().to_pybytes()
            media_type = "application/vnd.apache.arrow.stream"
            await query_cache.set(cache_key, (content, media_type), settings.CACHE_TTL_SECONDS)
            return Response(content=content, media_type=media_type)
        else:
            result_dict = {"result": final_arrow_table.to_pandas().to_dict(orient="records")}
            await query_cache.set(cache_key, (result_dict, "application/json"), settings.CACHE_TTL_SECONDS)
            return result_dict

    except HTTPException as http_exc:
        raise http_exc
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except (ValueError, NotImplementedError, duckdb.BinderException, duckdb.ParserException, TypeError) as e:
        logger.exception("An error occurred during query planning or execution")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.exception("An unhandled error occurred")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An internal error occurred: {e}")