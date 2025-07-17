# worker.py (Perbaikan)

import ray
import duckdb
import pyarrow as pa
from typing import List, Dict

@ray.remote
class DuckDBWorker:
    """A Ray actor that executes a SQL query on a subset of data files."""
    def __init__(self):
        # Setiap worker memiliki koneksi DuckDB sendiri
        self.con = duckdb.connect(database=':memory:', read_only=False)
        print("✅ DuckDBWorker actor initialized.")

    # =================================================================
    # === METODE JOIN YANG SUDAH DIPERBAIKI ===========================
    # =================================================================
    # Perhatikan perubahan nama parameter dan penghapusan ray.get()
    def run_join_task(self, join_sql: str, broadcast_table: pa.Table, broadcast_table_name: str) -> pa.Table:
        """
        Mengeksekusi kueri JOIN di mana satu tabel (yang kecil)
        diterima langsung sebagai objek PyArrow Table.
        """
        try:
            # 1. TIDAK PERLU ray.get(). Data sudah diterima secara langsung.
            #    Nama parameter diubah menjadi `broadcast_table` agar lebih jelas.
            print(f"✅ Worker received broadcast table '{broadcast_table_name}' with {broadcast_table.num_rows} rows.")

            # 2. Daftarkan tabel dari memori ini ke koneksi DuckDB lokal worker
            self.con.register(broadcast_table_name, broadcast_table)
            print(f"✅ Worker registered table '{broadcast_table_name}'.")
            
            # 3. Eksekusi kueri JOIN
            print(f"✅ Worker executing JOIN: {join_sql}")
            result = self.con.execute(join_sql).fetch_arrow_table()
            print(f"✅ Worker JOIN produced result with {result.num_rows} rows.")
            return result
        except Exception as e:
            print("🔥🔥🔥 JOIN WORKER FAILED! 🔥🔥🔥")
            print(f"   - Join SQL: {join_sql}")
            print(f"   - Broadcast Table Name: {broadcast_table_name}")
            # Cetak traceback untuk debug yang lebih baik
            import traceback
            print(f"   - Exception: {traceback.format_exc()}")
            return pa.Table.from_pydict({}) # Kembalikan tabel kosong jika gagal

    # Metode query yang sudah ada tidak perlu diubah
    def query(self, sql_query: str, file_paths: List[str]) -> pa.Table:
        # ... (tidak ada perubahan di sini) ...
        query_to_execute = "" 
        try:
            query_to_execute = sql_query.format(files=file_paths)
            result = self.con.execute(query_to_execute).fetch_arrow_table()
            return result
        except Exception as e:
            print("🔥🔥🔥 QUERY WORKER FAILED! 🔥🔥🔥")
            print(f"   - Query Template: {sql_query}")
            print(f"   - Formatted Query: {query_to_execute}")
            print(f"   - Exception: {e}")
            return pa.Table.from_pydict({})


# worker.py -> Ganti metode partition_by_key dengan ini

    def partition_by_key(self, file_path: str, key_column: str, num_partitions: int, where_sql: str = "") -> Dict[int, pa.Table]:
        """
        Membaca file, menerapkan filter WHERE (jika ada), lalu mempartisi hasilnya
        TANPA konversi ke Pandas.
        """
        print(f"👷 Worker partitioning '{file_path}' by key '{key_column}' (Arrow-native)...")
        if where_sql:
            print(f"   - Applying filter: {where_sql}")

        # 1. Buat tabel sementara di memori dengan ID partisi
        # DuckDB akan menangani ini dengan sangat efisien.
        base_query = f"""
        CREATE OR REPLACE TEMP VIEW partitioned_view AS
        SELECT *, HASH({key_column}) % {num_partitions} AS __partition_id
        FROM read_parquet('{file_path}')
        {where_sql}
        """
        self.con.execute(base_query)

        # 2. Iterasi untuk setiap ID partisi dan ekstrak datanya sebagai Arrow table
        partitions = {}
        for i in range(num_partitions):
            # Ambil data untuk partisi `i` dan hapus kolom __partition_id
            result = self.con.execute(
                f"SELECT * EXCLUDE (__partition_id) FROM partitioned_view WHERE __partition_id = {i}"
            ).fetch_arrow_table()
            
            # Hanya tambahkan jika partisi tersebut memiliki data
            if result.num_rows > 0:
                partitions[i] = result
                
        return partitions

    # =================================================================
    # === KEMAMPUAN BARU 2: GABUNG PARTISI LOKAL (FASE 2) =============
    # =================================================================
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
        
        # Gabungkan semua potongan partisi menjadi satu tabel besar lokal
        left_table = pa.concat_tables(left_partitions)
        right_table = pa.concat_tables(right_partitions)
        
        print(f"   - Left side has {left_table.num_rows} rows.")
        print(f"   - Right side has {right_table.num_rows} rows.")

        # Daftarkan tabel-tabel ini ke DuckDB dengan nama sementara
        self.con.register(f"{left_alias}_local", left_table)
        self.con.register(f"{right_alias}_local", right_table)
        
        # Eksekusi kueri JOIN yang sudah dimodifikasi
        return self.con.execute(join_sql).fetch_arrow_table()