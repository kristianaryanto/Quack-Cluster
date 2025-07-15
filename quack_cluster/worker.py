import ray
import duckdb
import pyarrow as pa
from typing import List

@ray.remote
class DuckDBWorker:
    """A Ray actor that executes a SQL query on a subset of data files."""
    def __init__(self):
        # Setiap worker memiliki koneksi DuckDB sendiri
        self.con = duckdb.connect(database=':memory:', read_only=False)
        print("✅ DuckDBWorker actor initialized.")

    def query(self, sql_query: str, file_paths: List[str]) -> pa.Table:
        """Executes a DuckDB query against a list of Parquet files."""
        query_to_execute = "" # Definisikan di luar try untuk logging error
        try:
            # Menggunakan f-string aman di sini karena input dikontrol secara internal
            query_to_execute = sql_query.format(files=file_paths)
            print(f"✅ Worker executing: {query_to_execute}")
            result = self.con.execute(query_to_execute).fetch_arrow_table()
            print(f"✅ Worker produced result with {result.num_rows} rows.")
            return result
        except Exception as e:
            # SOLUSI: Cetak error yang sebenarnya dari worker
            print("🔥🔥🔥 WORKER FAILED! 🔥🔥🔥")
            print(f"    Query Template: {sql_query}")
            print(f"    File Paths: {file_paths}")
            print(f"    Formatted Query: {query_to_execute}")
            print(f"    Exception Type: {type(e).__name__}")
            print(f"    Exception Details: {e}")
            # Kembalikan tabel kosong agar tidak crash
            return pa.Table.from_pydict({})