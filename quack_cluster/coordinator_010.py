import os
import glob
import ray
import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlglot
import duckdb
import pyarrow as pa

from .worker import DuckDBWorker
from .settings import settings

app = FastAPI(title="Quack-Cluster")

@app.on_event("startup")
def startup_event():
    ray.init(address="auto", namespace="quack-cluster", ignore_reinit_error=True)
    print("✅ Ray connected.")

class QueryRequest(BaseModel):
    sql: str

@app.post("/query")
async def execute_query(request: QueryRequest):
    try:
        parsed_query = sqlglot.parse_one(request.sql)
        table_expression = parsed_query.find(sqlglot.exp.Table)

        if not table_expression:
            raise HTTPException(status_code=400, detail="Could not find a table in the SQL query.")

        table_name = table_expression.this.name
        
        file_pattern = os.path.join(settings.DATA_DIR, f"{table_name}.parquet")
        all_files = glob.glob(file_pattern)

        if not all_files:
            raise HTTPException(
                status_code=404, 
                detail=f"Table '{table_name}' not found. No files matching pattern: {file_pattern}"
            )

        # FIX: Logika di sini tidak perlu diubah, karena 'parsed_query' sudah mengandung
        # klausa WHERE jika ada. Saat kita mengganti nama tabel, klausa WHERE tetap ada.
        worker_query = parsed_query.copy()
        worker_query.find(sqlglot.exp.Table).replace(sqlglot.parse_one("__TABLE_PLACEHOLDER__"))
        sql_with_placeholder = worker_query.sql(dialect="duckdb")

        distributed_sql_template = sql_with_placeholder.replace(
            "__TABLE_PLACEHOLDER__", "read_parquet({files})"
        )
        
        actors = [DuckDBWorker.remote() for _ in all_files]
        tasks = [actors[i].query.remote(distributed_sql_template, [all_files[i]]) for i in range(len(all_files))]
        
        partial_results = await asyncio.gather(*tasks)
        
        valid_results = [r for r in partial_results if r is not None and r.num_rows > 0]
        if not valid_results:
             return {"result": []}

        combined_arrow_table = pa.concat_tables(valid_results)
        agg_con = duckdb.connect(database=':memory:')

        final_query_tree = parsed_query.copy()
        final_query_tree.find(sqlglot.exp.Table).replace(sqlglot.parse_one("combined_arrow_table"))
        final_query = final_query_tree.sql(dialect="duckdb")

        final_result_df = agg_con.execute(final_query).fetch_df()
        
        return {"result": final_result_df.to_dict(orient="records")}

    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))