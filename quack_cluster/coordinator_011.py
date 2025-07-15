import os
import glob
import ray
import asyncio
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import sqlglot
import duckdb
import pyarrow as pa
from sqlglot.errors import ParseError
from contextlib import asynccontextmanager

from .worker import DuckDBWorker
from .settings import settings # <-- SOLUSI: Tambahkan baris ini

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
            parsed_query = sqlglot.parse_one(request.sql)
        except ParseError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid SQL syntax: {e}")

        table_expression = parsed_query.find(sqlglot.exp.Table)
        if not table_expression:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Could not find a table in the SQL query.")

        table_name = table_expression.this.name
        
        file_pattern = os.path.join(settings.DATA_DIR, f"{table_name}.parquet")
        all_files = glob.glob(file_pattern)

        if not all_files:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, 
                detail=f"Table '{table_name}' not found. No files matching pattern: {file_pattern}"
            )
        
        worker_query = parsed_query.copy()
        
        for count_expr in worker_query.find_all(sqlglot.exp.Count):
            if isinstance(count_expr.this, sqlglot.exp.Star):
                count_expr.replace(sqlglot.parse_one("COUNT(*) AS partial_count"))

        worker_query.find(sqlglot.exp.Table).replace(sqlglot.parse_one("__TABLE_PLACEHOLDER__"))
        sql_with_placeholder = worker_query.sql(dialect="duckdb")
        distributed_sql_template = sql_with_placeholder.replace("__TABLE_PLACEHOLDER__", "read_parquet({files})")
        
        actors = [DuckDBWorker.remote() for _ in all_files]
        tasks = [actors[i].query.remote(distributed_sql_template, [all_files[i]]) for i in range(len(all_files))]
        partial_results = await asyncio.gather(*tasks)
        
        valid_results = [r for r in partial_results if r is not None and r.num_rows > 0]
        if not valid_results:
             return {"result": []}

        combined_arrow_table = pa.concat_tables(valid_results)
        agg_con = duckdb.connect(database=':memory:')

        final_query_tree = parsed_query.copy()
        
        for count_expr in final_query_tree.find_all(sqlglot.exp.Count):
            if isinstance(count_expr.this, sqlglot.exp.Star):
                if isinstance(count_expr.parent, sqlglot.exp.Alias):
                    count_expr.parent.set("this", sqlglot.parse_one("SUM(partial_count)"))
                else:
                    count_expr.replace(sqlglot.parse_one("SUM(partial_count)"))

        final_query_tree.find(sqlglot.exp.Table).replace(sqlglot.parse_one("combined_arrow_table"))
        final_query = final_query_tree.sql(dialect="duckdb")
        
        final_result_df = agg_con.execute(final_query).fetch_df()
        return {"result": final_result_df.to_dict(orient="records")}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"An unhandled error occurred: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))