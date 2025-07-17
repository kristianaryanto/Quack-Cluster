import os
import glob
import ray
import asyncio
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import sqlglot
from sqlglot import exp, parse_one
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd # <-- Diperlukan untuk membaca parquet dengan mudah
from sqlglot.errors import ParseError
from contextlib import asynccontextmanager

from .worker import DuckDBWorker
from .settings import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    ray.init(address="auto", namespace="quack-cluster", ignore_reinit_error=True)
    print("✅ Ray connected.")
    yield
    print("Ray disconnecting...")

app = FastAPI(title="Quack-Cluster", lifespan=lifespan)

class QueryRequest(BaseModel):
    sql: str

@app.post("/query")
async def execute_query(request: QueryRequest):
    try:
        try:
            parsed_query = parse_one(request.sql)
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid SQL syntax: {e}")

        join_expression = parsed_query.find(exp.Join)
        if join_expression:
            print("✅ JOIN query detected. Initiating Broadcast Join flow...")
            
            # 1. Identifikasi tabel besar dan kecil
            # TEMUKAN EKSPRESI TABEL BESAR UNTUK MENDAPATKAN ALIASNYA
            large_table_expression = parsed_query.find(exp.Table)
            large_table_name = large_table_expression.this.name
            large_table_alias = large_table_expression.alias_or_name # <-- KUNCI: DAPATKAN ALIAS 'o'

            broadcast_table_name = join_expression.this.this.name
            print(f"   - Large table: '{large_table_name}' (alias: '{large_table_alias}')")
            print(f"   - Broadcast table: '{broadcast_table_name}'")

            # 2. Proses "Broadcast" (tidak ada perubahan di sini)
            broadcast_file_path = os.path.join(settings.DATA_DIR, f"{broadcast_table_name}.parquet")
            if not os.path.exists(broadcast_file_path):
                 raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Broadcast table '{broadcast_table_name}' not found.")


            broadcast_table_data = pq.read_table(broadcast_file_path)
            broadcast_table_ref = ray.put(broadcast_table_data)
            print(f"   - Placed '{broadcast_table_name}' into Ray object store.")

            # 3. Persiapan tugas untuk Worker (dengan alias yang benar)
            large_file_pattern = os.path.join(settings.DATA_DIR, f"{large_table_name}.parquet")
            all_large_files = glob.glob(large_file_pattern)
            if not all_large_files:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Large table '{large_table_name}' not found.")
            
            # BUAT STRING PENGGANTI DENGAN menyertakan ALIAS
            replacement_sql_str = f"read_parquet('{all_large_files[0]}') AS {large_table_alias}"
            
            # GANTI EKSPRESI TABEL LAMA DENGAN FUNGSI BARU + ALIAS
            # parsed_query akan dimodifikasi secara langsung (in-place)
            large_table_expression.replace(parse_one(replacement_sql_str))
            
            # # SIMPAN DAN COPOT ORDER BY / LIMIT
            # # Buat salinan dari AST untuk dimodifikasi
            # query_for_workers = parsed_query.copy()

            # # Simpan klausa ORDER BY jika ada, lalu hapus dari kueri worker
            # order_by_clause = query_for_workers.find(exp.Order)
            # if order_by_clause:
            #     order_by_clause.pop()

            # # Simpan klausa LIMIT jika ada, lalu hapus dari kueri worker
            # limit_clause = query_for_workers.find(exp.Limit)
            # if limit_clause:
            #     limit_clause.pop()

            # # Sekarang, `query_for_workers` adalah versi bersih tanpa ORDER BY/LIMIT
            # # yang akan digunakan untuk membuat `final_sql_str` untuk para joiner worker.

            # # --- FASE 2: GABUNG LOKAL ---
            # # Gunakan `query_for_workers` untuk membuat SQL
            # final_join_sql = query_for_workers
            
            final_join_sql = parsed_query.copy()
            # 4. Kirim tugas ke worker (tidak ada perubahan di sini)
            worker = DuckDBWorker.remote()
            result_table_ref = worker.run_join_task.remote(
                final_join_sql,
                broadcast_table_ref,
                broadcast_table_name
            )
            final_result_table = await result_table_ref
            
            final_result_df = final_result_table.to_pandas()
            return {"result": final_result_df.to_dict(orient="records")}
        # =================================================================
        # === SELESAI BLOK LOGIKA BROADCAST JOIN ===========================
        # =================================================================

        # Jika bukan kueri JOIN, lanjutkan ke logika yang sudah ada
        table_expression = parsed_query.find(exp.Table)
        if not table_expression:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Could not find a table in the SQL query.")

        table_name = table_expression.this.name
        file_pattern = os.path.join(settings.DATA_DIR, f"{table_name}.parquet")
        all_files = glob.glob(file_pattern)

        if not all_files:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Table '{table_name}' not found.")

        is_agg_query = bool(parsed_query.find(exp.AggFunc))
        if not is_agg_query:
            # --- Alur Sederhana (SELECT *, WHERE, dll.) ---
            worker_query = parsed_query.copy()
            worker_query.find(exp.Table).replace(parse_one("__TABLE_PLACEHOLDER__"))
            sql_with_placeholder = worker_query.sql(dialect="duckdb")
            distributed_sql_template = sql_with_placeholder.replace("__TABLE_PLACEHOLDER__", "read_parquet({files})")
            final_query = sql_with_placeholder.replace("__TABLE_PLACEHOLDER__", "combined_arrow_table")
        else:
            # --- Alur Agregasi (SUM, COUNT, AVG) ---
            # (Kode agregasi yang sudah ada tidak diubah)
            group_by_clause = parsed_query.find(exp.Group)
            group_by_cols = [col.sql() for col in group_by_clause.expressions] if group_by_clause else []
            
            worker_selects = group_by_cols.copy()
            final_selects = group_by_cols.copy()
            # ... (sisa logika agregasi tidak ditampilkan untuk keringkasan)
            for i, select_expr in enumerate(parsed_query.find(exp.Select).expressions):
                if select_expr.sql() in group_by_cols: continue
                agg_func = select_expr.find(exp.AggFunc)
                alias = select_expr.alias_or_name
                if isinstance(agg_func, exp.Count) and isinstance(agg_func.this, exp.Star):
                    p_alias = f"partial_count_{i}"
                    worker_selects.append(f"COUNT(*) AS {p_alias}")
                    final_selects.append(f"SUM({p_alias}) AS {alias}")
                elif isinstance(agg_func, exp.Sum):
                    inner_col = agg_func.this.sql()
                    p_alias = f"partial_sum_{i}"
                    worker_selects.append(f"SUM({inner_col}) AS {p_alias}")
                    final_selects.append(f"SUM({p_alias}) AS {alias}")
                elif isinstance(agg_func, exp.Avg):
                    inner_col = agg_func.this.sql()
                    p_sum_alias = f"partial_avg_sum_{i}"
                    p_count_alias = f"partial_avg_count_{i}"
                    worker_selects.append(f"SUM({inner_col}) AS {p_sum_alias}")
                    worker_selects.append(f"COUNT({inner_col}) AS {p_count_alias}")
                    final_selects.append(f"SUM({p_sum_alias}) / SUM({p_count_alias}) AS {alias}")

            where_clause_str = f" {parsed_query.find(exp.Where).sql()}" if parsed_query.find(exp.Where) else ""
            group_by_str = f" GROUP BY {', '.join(group_by_cols)}" if group_by_cols else ""
            order_by_str = f" {parsed_query.find(exp.Order).sql()}" if parsed_query.find(exp.Order) else ""
            distributed_sql_template = f"SELECT {', '.join(worker_selects)} FROM read_parquet({{files}}){where_clause_str}{group_by_str}"
            final_query = f"SELECT {', '.join(final_selects)} FROM combined_arrow_table{group_by_str}{order_by_str}"

        # --- Eksekusi ---
        actors = [DuckDBWorker.remote() for _ in all_files]
        tasks = [actors[i].query.remote(distributed_sql_template, [all_files[i]]) for i in range(len(all_files))]
        partial_results = await asyncio.gather(*tasks)
        
        valid_results = [r for r in partial_results if r is not None and r.num_rows > 0]
        if not valid_results:
             return {"result": []}

        combined_arrow_table = pa.concat_tables(valid_results)
        agg_con = duckdb.connect(database=':memory:')
        
        final_result_df = agg_con.execute(final_query).fetch_df()
        return {"result": final_result_df.to_dict(orient="records")}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"An unhandled error occurred: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))