# worker.py (Perbaikan)

import ray
import duckdb
import pyarrow as pa
from typing import List, Dict
import traceback
import os
import logging as logger

import os  # Impor os
import time # Impor time

@ray.remote
class DuckDBWorker:
    """A Ray actor that executes a SQL query on a subset of data files."""
    def __init__(self):
        self.con = duckdb.connect(database=':memory:', read_only=False)
        print("✅ DuckDBWorker actor initialized.")

    def run_join_task(self, join_sql: str, broadcast_table: pa.Table, broadcast_table_name: str) -> pa.Table:
        """
        Mengeksekusi kueri JOIN di mana satu tabel (yang kecil)
        diterima langsung sebagai objek PyArrow Table.
        """
        try:
            print(f"✅ Worker received broadcast table '{broadcast_table_name}' with {broadcast_table.num_rows} rows.")
            self.con.register(broadcast_table_name, broadcast_table)
            print(f"✅ Worker registered table '{broadcast_table_name}'.")
            
            print(f"✅ Worker executing JOIN: {join_sql}")
            result = self.con.execute(join_sql).fetch_arrow_table()
            print(f"✅ Worker JOIN produced result with {result.num_rows} rows.")
            return result
        except Exception as e:
            print("🔥🔥🔥 JOIN WORKER FAILED! 🔥🔥🔥")
            print(f"   - Join SQL: {join_sql}")
            print(f"   - Broadcast Table Name: {broadcast_table_name}")
            print(f"   - Exception: {traceback.format_exc()}")
            return pa.Table.from_pydict({})

    def query(self, sql_query: str, file_paths: List[str]) -> pa.Table:
        query_to_execute = "" 
        try:
            # Menggunakan list comprehension untuk format yang aman
            formatted_paths = ", ".join([f"'{path}'" for path in file_paths])
            query_to_execute = sql_query.format(files=f"[{formatted_paths}]")
            result = self.con.execute(query_to_execute).fetch_arrow_table()
            return result
        except Exception as e:
            print("🔥🔥🔥 QUERY WORKER FAILED! 🔥🔥🔥")
            print(f"   - Query Template: {sql_query}")
            print(f"   - Formatted Query: {query_to_execute}")
            print(f"   - Exception: {e}")
            return pa.Table.from_pydict({})


    def partition_by_key(
        self,
        file_path: str,
        key_column: str,
        num_partitions: int,
        reader_function: str,
        where_sql: str = ""
    ) -> Dict[int, pa.Table]:
        """
        Membaca file, menerapkan filter, lalu mempartisi.
        Dengan simulasi kegagalan untuk pengujian.
        """
        # --- KODE UNTUK SIMULASI KEGAGALAN ---
        if os.environ.get("TEST_FAULT_TOLERANCE") == "1" and not self._has_failed_once:
            self._has_failed_once = True
            logger.warning(f"💣 FAULT TOLERANCE TEST: Worker for '{file_path}' is intentionally failing in 2 seconds...")
            print(f"💣 FAULT TOLERANCE TEST: Worker for '{file_path}' is intentionally failing in 2 seconds...")
            time.sleep(2) # Beri jeda agar pesan log sempat terkirim
            ray.actor.exit_actor() # Hentikan aktor ini untuk memicu RayActorError di koordinator
        # --- AKHIR KODE SIMULASI ---


        logger.info(f"👷 Worker partitioning '{file_path}' using '{reader_function}'...")
        if where_sql:
            logger.info(f"   - Applying filter: {where_sql}")

        base_query = f"""
        CREATE OR REPLACE TEMP VIEW partitioned_view AS
        SELECT *, HASH({key_column}) % {num_partitions} AS __partition_id
        FROM {reader_function}('{file_path}')
        {where_sql}
        """
        self.con.execute(base_query)

        partitions = {}
        for i in range(num_partitions):
            result = self.con.execute(
                f"SELECT * EXCLUDE (__partition_id) FROM partitioned_view WHERE __partition_id = {i}"
            ).fetch_arrow_table()
            
            if result.num_rows > 0:
                partitions[i] = result
                
        return partitions

    def join_partitions(
        self,
        left_partitions: List[pa.Table],
        right_partitions: List[pa.Table],
        join_sql: str,
        left_alias: str,
        right_alias: str
    ) -> pa.Table:
        """
        Menerima beberapa partisi data dari worker lain, menggabungkannya,
        dan melakukan JOIN secara lokal.
        """
        print(f"🤝 Worker joining partitions...")
        
        left_table = pa.concat_tables(left_partitions) if left_partitions else pa.Table.from_pydict({})
        right_table = pa.concat_tables(right_partitions) if right_partitions else pa.Table.from_pydict({})
        
        print(f"   - Left side has {left_table.num_rows} rows.")
        print(f"   - Right side has {right_table.num_rows} rows.")

        if left_table.num_rows == 0 or right_table.num_rows == 0:
            return pa.Table.from_pydict({}) # Hindari error jika salah satu sisi kosong

        self.con.register(f"{left_alias}_local", left_table)
        self.con.register(f"{right_alias}_local", right_table)
        
        return self.con.execute(join_sql).fetch_arrow_table()