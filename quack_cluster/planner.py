# quack_cluster/planner.py
import os
import glob
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError
from .execution_plan import (
    BasePlan, LocalExecutionPlan, DistributedScanPlan, DistributedShuffleJoinPlan
)
from .settings import settings
from typing import Tuple, List

class Planner:
    """
    Menganalisis AST query dan membuat rencana eksekusi yang sesuai.
    """

    @staticmethod
    def _discover_table_files_and_reader(table_name: str) -> Tuple[str, List[str]]:
        """
        Menemukan file data untuk sebuah tabel dan menentukan fungsi baca DuckDB yang sesuai.
        Prioritas: .parquet > .csv > .json
        """
        for ext, reader_func in [
            ("parquet", "read_parquet"),
            ("csv", "read_csv_auto"),
            ("json", "read_json_auto")
        ]:
            file_pattern = os.path.join(settings.DATA_DIR, f"{table_name}.*.{ext}")
            files = glob.glob(file_pattern)
            if not files:
                 # Coba tanpa pola shard, cth: users.csv
                file_pattern_single = os.path.join(settings.DATA_DIR, f"{table_name}.{ext}")
                files = glob.glob(file_pattern_single)

            if files:
                return reader_func, files

        raise FileNotFoundError(f"No data files found for table '{table_name}' with supported extensions (.parquet, .csv, .json)")


    @staticmethod
    def create_plan(query_ast: exp.Expression, registered_tables: set) -> BasePlan:
        """
        Menganalisis AST dan mengembalikan objek rencana eksekusi yang sesuai.
        """
        all_tables_in_query = [t.this.name for t in query_ast.find_all(exp.Table)]

        if not all_tables_in_query or all(t in registered_tables for t in all_tables_in_query):
            return LocalExecutionPlan(
                query_ast=query_ast
            )

        if join_expression := query_ast.find(exp.Join):
            if isinstance(join_expression, exp.Join) and join_expression.args.get('kind') == 'CROSS':
                raise NotImplementedError("Distributed CROSS JOIN is not yet fully implemented.")

            left_table_expr = query_ast.find(exp.Table)
            left_table_name = left_table_expr.this.name
            left_table_alias = left_table_expr.alias_or_name
            left_reader_func, _ = Planner._discover_table_files_and_reader(left_table_name)

            right_table_expr = join_expression.this
            right_table_name = right_table_expr.this.name
            right_table_alias = right_table_expr.alias_or_name
            right_reader_func, _ = Planner._discover_table_files_and_reader(right_table_name)


            join_on_clause = join_expression.args.get('on')
            join_using_clause = join_expression.args.get('using')

            if not join_on_clause and not join_using_clause:
                raise ValueError("Unsupported JOIN type: No ON or USING clause found.")

            if join_on_clause:
                eq_expr = join_on_clause.find(exp.EQ)
                if not eq_expr: raise ValueError("Could not find a valid equality (=) condition in the ON clause.")
                left_key = eq_expr.left.name
                right_key = eq_expr.right.name
            else: # join_using_clause
                key_name = join_using_clause.expressions[0].this.name
                left_key = right_key = key_name
            
            query_for_workers = query_ast.copy()
            if query_for_workers.find(exp.Order): query_for_workers.find(exp.Order).pop()
            if query_for_workers.find(exp.Limit): query_for_workers.find(exp.Limit).pop()
            
            query_for_workers.find(exp.Table).replace(parse_one(f"{left_table_alias}_local AS {left_table_alias}"))
            query_for_workers.find(exp.Join).this.replace(parse_one(f"{right_table_alias}_local AS {right_table_alias}"))
            worker_sql_str = query_for_workers.sql(dialect="duckdb")

            return DistributedShuffleJoinPlan(
                query_ast=query_ast,
                left_table_name=left_table_name,
                left_table_alias=left_table_alias,
                left_join_key=left_key,
                right_table_name=right_table_name,
                right_table_alias=right_table_alias,
                right_join_key=right_key,
                worker_join_sql=worker_sql_str,
                left_reader_function=left_reader_func, # Baru
                right_reader_function=right_reader_func # Baru
            )
            
        else:
            table_name = all_tables_in_query[0]
            reader_func, _ = Planner._discover_table_files_and_reader(table_name)
            
            where_str = ""
            if where_clause := query_ast.find(exp.Where):
                where_str = f" {where_clause.sql(dialect='duckdb')}" 
            
            # Gunakan fungsi baca yang sudah ditemukan
            worker_template = f"SELECT * FROM {reader_func}({{files}}){where_str}"

            return DistributedScanPlan(
                query_ast=query_ast,
                table_name=table_name,
                worker_query_template=worker_template,
            )