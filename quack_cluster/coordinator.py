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
from fastapi import FastAPI, HTTPException, Response, Query, status
from pydantic import BaseModel
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError
from contextlib import asynccontextmanager
from typing import Optional, Any, Dict, Tuple

from .planner import Planner
from .executor import Executor
from .settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- KELAS UNTUK MENGELOLA CACHE ---
class QueryCache:
    """Cache in-memory sederhana dengan dukungan TTL dan thread-safe."""
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._ttl: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key in self._cache:
                # Periksa apakah cache sudah kedaluwarsa
                if time.time() < self._ttl[key]:
                    logger.info(f"✅ Cache hit for key: {key[:10]}...")
                    return self._cache[key]
                else:
                    # Hapus cache yang sudah kedaluwarsa
                    logger.info(f"Cache expired for key: {key[:10]}...")
                    del self._cache[key]
                    del self._ttl[key]
            return None

    async def set(self, key: str, value: Any, ttl_seconds: int):
        async with self._lock:
            self._cache[key] = value
            self._ttl[key] = time.time() + ttl_seconds
            logger.info(f"Cache set for key: {key[:10]}... with TTL: {ttl_seconds}s")

# Buat instance cache global
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

# ... fungsi resolve_and_execute tidak berubah ...
async def resolve_and_execute(query_ast: exp.Expression, con: duckdb.DuckDBPyConnection) -> pa.Table:
    """
    Secara rekursif menyelesaikan CTE, UNION, dan subquery, lalu mendelegasikannya
    ke Planner dan Executor untuk eksekusi query yang sudah sederhana.
    """
    if not query_ast:
        raise ValueError("Received an empty AST node for execution.")

    # [cite_start]Kasus 1: WITH clause (CTE). Selesaikan CTE terlebih dahulu. [cite: 3]
    if with_clause := query_ast.args.get('with'):
        query_body = query_ast.copy()
        query_body.set('with', None) # Hapus klausa WITH untuk diproses nanti
        for cte in with_clause.expressions:
            logger.info(f"➡️ Resolving CTE: '{cte.alias}'")
            cte_result_table = await resolve_and_execute(cte.this, con)
            con.register(cte.alias, cte_result_table) # Daftarkan hasil CTE sebagai tabel in-memory
            logger.info(f"✅ CTE '{cte.alias}' registered as in-memory table.")
        return await resolve_and_execute(query_body, con) # Lanjutkan rekursi dengan sisa query

    # Kasus 2: UNION. [cite_start]Selesaikan kedua sisi secara paralel lalu gabungkan. [cite: 7, 8]
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
        # [cite_start]For UNION ALL, just return the concatenated table. [cite: 10, 11]
        return combined_table


    # Kasus 3: Subquery. [cite_start]Selesaikan subquery dan ganti dengan hasilnya. [cite: 12]
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

    # --- BAGIAN UTAMA YANG BERUBAH ---
    # Kasus 4: AST sekarang sederhana (tidak ada CTE/UNION/subquery di level ini).
    # Saatnya membuat rencana dan mengeksekusinya.
    logger.info(f"✅ Simplified AST ready for planning: {query_ast.sql(dialect='duckdb')}")
    registered_tables = {row[0] for row in con.execute("PRAGMA show_tables;").fetchall()}

    # 1. Panggil Planner untuk membuat rencana
    plan = Planner.create_plan(query_ast, registered_tables)
    logger.info(f"✅ Plan created: {plan.plan_type}")

    # 2. Panggil Executor untuk menjalankan rencana
    return await Executor.execute(plan, con)

@app.post("/query")
async def execute_query(
    request: QueryRequest,
    format: Optional[str] = Query("json", enum=["json", "arrow"])
):
    """Endpoint utama untuk eksekusi SQL, sekarang dengan caching."""
    cache_key = hashlib.sha256(f"{request.sql}:{format}".encode()).hexdigest()
    cached_data = await query_cache.get(cache_key)

    # --- BLOK LOGIKA CACHE HIT YANG DIPERBAIKI ---
    if cached_data:
        content, media_type = cached_data
        
        # Jika tipe media adalah JSON, 'content' adalah sebuah dict.
        # Kembalikan secara langsung, FastAPI akan menanganinya dengan benar.
        if media_type == "application/json":
            return content
        
        # Jika tipe media adalah Arrow, 'content' adalah bytes.
        # Gunakan kelas Response generik.
        else: # (media_type == "application/vnd.apache.arrow.stream")
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
        else: # Default ke JSON
            result_dict = {"result": final_arrow_table.to_pandas().to_dict(orient="records")}
            # Simpan dict ke cache, bukan objek Response
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
