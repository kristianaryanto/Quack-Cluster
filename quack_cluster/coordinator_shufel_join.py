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
from collections import defaultdict
NUM_SHUFFLE_PARTITIONS = 4
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
            join_expression = parsed_query.find(exp.Join)
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid SQL syntax: {e}")

        if join_expression:
            print("🚀 Shuffle Join detected. Orchestrating shuffle...")

            # Ekstrak informasi penting dari kueri
            left_table_expr = parsed_query.find(exp.Table)
            left_table_name = left_table_expr.this.name
            left_table_alias = left_table_expr.alias_or_name

            right_table_expr = join_expression.this
            right_table_name = right_table_expr.this.name
            right_table_alias = right_table_expr.alias_or_name

            # =================================================================
            # === PENDEKATAN BARU YANG LEBIH KUAT =============================
            # =================================================================
            # Langsung cari ekspresi perbandingan (EQ untuk '=') di dalam node Join
            comparison_expr = join_expression.find(exp.EQ)

            # Jika tidak ada kondisi '=', berikan error
            if not comparison_expr:
                raise ValueError("Could not find a valid equality (=) condition in the JOIN clause.")

            # `comparison_expr.left` adalah node Column. Kita bisa langsung ambil namanya.
            # Ini adalah cara yang lebih bersih daripada `.this.name`
            left_key = comparison_expr.left.name
            right_key = comparison_expr.right.name
            # =================================================================

            print(f"   - Left: {left_table_name} (alias {left_table_alias}) on key '{left_key}'")
            print(f"   - Right: {right_table_name} (alias {right_table_alias}) on key '{right_key}'")

            # --- FASE 1: PARTISI & ACAK ---
            left_files = glob.glob(os.path.join(settings.DATA_DIR, f"{left_table_name}.parquet"))
            right_files = glob.glob(os.path.join(settings.DATA_DIR, f"{right_table_name}.parquet"))
            
            map_workers = [DuckDBWorker.remote() for _ in range(len(left_files) + len(right_files))]
            
            # Buat tugas partisi untuk setiap file dari kedua tabel
            left_map_tasks = [
                map_workers[i].partition_by_key.remote(file, left_key, NUM_SHUFFLE_PARTITIONS)
                for i, file in enumerate(left_files)
            ]
            right_map_tasks = [
                map_workers[len(left_files) + i].partition_by_key.remote(file, right_key, NUM_SHUFFLE_PARTITIONS)
                for i, file in enumerate(right_files)
            ]
            
            # --- TAHAP PENGUMPULAN HASIL ACAK ---
            # Ini adalah inti dari shuffle. Kita mengumpulkan semua potongan data
            # yang ditujukan untuk setiap partisi tujuan.
            
            # `partitions_for_joiners` akan menjadi:
            # {
            #   0: {'left': [data_A_part0, data_B_part0], 'right': [data_C_part0]},
            #   1: {'left': [data_A_part1, data_B_part1], 'right': [data_C_part1]}, ...
            # }
            partitions_for_joiners = defaultdict(lambda: defaultdict(list))
            
            # Kumpulkan hasil dari tabel kiri
            for task in left_map_tasks:
                partitioned_dict = await task
                for partition_id, table in partitioned_dict.items():
                    partitions_for_joiners[partition_id]['left'].append(table)
            
            # Kumpulkan hasil dari tabel kanan
            for task in right_map_tasks:
                partitioned_dict = await task
                for partition_id, table in partitioned_dict.items():
                    partitions_for_joiners[partition_id]['right'].append(table)
            
            print("✅ Shuffle phase complete. Data regrouped for joiners.")

            # --- FASE 2: GABUNG LOKAL ---
            # Siapkan kueri untuk worker joiner

            # SIMPAN DAN COPOT ORDER BY / LIMIT
            # Buat salinan dari AST untuk dimodifikasi
            query_for_workers = parsed_query.copy()

            # Simpan klausa ORDER BY jika ada, lalu hapus dari kueri worker
            order_by_clause = query_for_workers.find(exp.Order)
            if order_by_clause:
                order_by_clause.pop()

            # Simpan klausa LIMIT jika ada, lalu hapus dari kueri worker
            limit_clause = query_for_workers.find(exp.Limit)
            if limit_clause:
                limit_clause.pop()

            # Sekarang, `query_for_workers` adalah versi bersih tanpa ORDER BY/LIMIT
            # yang akan digunakan untuk membuat `final_sql_str` untuk para joiner worker.

            # --- FASE 2: GABUNG LOKAL ---
            # Gunakan `query_for_workers` untuk membuat SQL
            final_join_sql = query_for_workers

            # final_join_sql = parsed_query.copy()

            # =================================================================
            # === BAGIAN YANG DIPERBAIKI ======================================
            # =================================================================
            # Temukan node untuk kedua tabel
            left_table_node = final_join_sql.find(exp.Table)
            right_table_node = final_join_sql.find(exp.Join).this

            # Buat string pengganti YANG MENYERTAKAN ALIAS ASLI
            left_replacement_str = f"{left_table_alias}_local AS {left_table_alias}"   # contoh: "o_local AS o"
            right_replacement_str = f"{right_table_alias}_local AS {right_table_alias}" # contoh: "u_local AS u"

            # Ganti node dengan ekspresi baru yang sudah memiliki alias
            left_table_node.replace(parse_one(left_replacement_str))
            right_table_node.replace(parse_one(right_replacement_str))
            # =================================================================

            final_sql_str = final_join_sql.sql(dialect="duckdb")
            
            join_workers = [DuckDBWorker.remote() for _ in range(NUM_SHUFFLE_PARTITIONS)]
            join_tasks = []
            for i in range(NUM_SHUFFLE_PARTITIONS):
                if i in partitions_for_joiners:
                    task = join_workers[i].join_partitions.remote(
                        partitions_for_joiners[i]['left'],
                        partitions_for_joiners[i]['right'],
                        final_sql_str,
                        left_table_alias,
                        right_table_alias
                    )
                    join_tasks.append(task)
            
            # Kumpulkan hasil akhir

            final_results = await asyncio.gather(*join_tasks)
            # `partial_results_table` berisi hasil GROUP BY parsial dari semua worker
            partial_results_table = pa.concat_tables(final_results)

            # --- TAHAP FINALISASI ---
            # Kita perlu melakukan GROUP BY sekali lagi di koordinator untuk menggabungkan hasil parsial
            # Misalnya, SUM(partial_sum) atau SUM(partial_count)
            agg_con = duckdb.connect(database=':memory:')
            agg_con.register("partial_results", partial_results_table)
            # =============================================================
            # === BAGIAN YANG DIPERBAIKI: MEMBUAT KUERI FINAL YANG BENAR ========
            # =================================================================

            # Siapkan list untuk bagian-bagian kueri final
            final_selects = []
            final_group_bys = []

            # 1. Tentukan kolom GROUP BY untuk tahap final
            group_by_node = parsed_query.find(exp.Group)
            if group_by_node:
                for expr in group_by_node.expressions:
                    # Ambil nama kolom sederhana (misal: 'name' dari 'u.name')
                    col_name = expr.name 
                    final_group_bys.append(col_name)
                    final_selects.append(col_name)

            # 2. Terjemahkan fungsi agregasi untuk tahap final
            select_node = parsed_query.find(exp.Select)
            for expr in select_node.expressions:
                # Lewati kolom group by, sudah ditambahkan
                if expr.name in final_group_bys:
                    continue

                alias = expr.alias_or_name
                agg_func = expr.find(exp.AggFunc)

                # COUNT() dan SUM() di tahap parsial menjadi SUM() di tahap final
                if isinstance(agg_func, (exp.Count, exp.Sum)):
                    final_selects.append(f"SUM({alias}) AS {alias}")
                # TODO: Tambahkan logika untuk AVG (SUM(sum)/SUM(count)) jika diperlukan

            # 3. Bangun string kueri final yang bersih
            final_select_str = ", ".join(final_selects)
            final_group_by_str = ", ".join(final_group_bys)

            final_agg_query = f"SELECT {final_select_str} FROM partial_results"
            if final_group_by_str:
                final_agg_query += f" GROUP BY {final_group_by_str}"

            # 4. Tambahkan ORDER BY dan LIMIT (dengan alias yang sudah dihapus)
            if order_by_clause:
                # Hapus alias tabel dari klausa ORDER BY (misal: 'u.name' -> 'name')
                rewritten_order_by = order_by_clause.copy()
                for col in rewritten_order_by.find_all(exp.Column):
                    col.set('table', None) 
                final_agg_query += f" {rewritten_order_by.sql()}"

            if limit_clause:
                final_agg_query += f" {limit_clause.sql()}"
            # =================================================================

            print(f"✅ Coordinator executing final query: {final_agg_query}")

            # Jalankan kueri final
            final_df = agg_con.execute(final_agg_query).fetch_df()
            return {"result": final_df.to_dict(orient="records")}
                        
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