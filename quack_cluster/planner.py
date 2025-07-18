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
            
            # --- LOGIKA PEMISAHAN KLAUSA WHERE ---
            left_where_parts = []
            right_where_parts = []
            left_where_sql = ""
            right_where_sql = ""

            if where_clause := query_ast.find(exp.Where):
                def process_condition(condition):
                    cleaned_condition = condition.copy()
                    for col in cleaned_condition.find_all(exp.Column):
                        col.set('table', None)

                    first_col = condition.find(exp.Column)
                    if first_col:
                        table_of_col = first_col.table
                        if table_of_col == left_table_alias:
                            left_where_parts.append(cleaned_condition.sql(dialect="duckdb"))
                        elif table_of_col == right_table_alias:
                            right_where_parts.append(cleaned_condition.sql(dialect="duckdb"))

                if isinstance(where_clause.this, exp.Connector):
                    for condition in where_clause.this.expressions:
                        process_condition(condition)
                else:
                    process_condition(where_clause.this)

                if left_where_parts:
                    left_where_sql = f"WHERE {' AND '.join(left_where_parts)}"
                if right_where_parts:
                    right_where_sql = f"WHERE {' AND '.join(right_where_parts)}"
                    

            # --- LOGIKA FUNGSI WINDOW (FINAL) ---
            final_select_sql = None
            original_select_clause = query_ast.find(exp.Select)
            
            if any(expr.find(exp.Window) for expr in original_select_clause.expressions):
                # 1. Buat AST baru untuk tahap final, mulai dari AST asli.
                final_query_ast = query_ast.copy()

                # 2. Hapus klausa yang sudah dieksekusi di tahap sebelumnya.
                if final_query_ast.find(exp.Where): final_query_ast.find(exp.Where).pop()
                if final_query_ast.find(exp.Group): final_query_ast.find(exp.Group).pop()
                if final_query_ast.find(exp.Join): final_query_ast.find(exp.Join).pop()

                # 3. Ganti tabel sumber menjadi hasil agregasi dari tahap sebelumnya.
                final_query_ast.find(exp.Table).replace(parse_one("aggregated_results"))

                # 4. Buat peta dari ekspresi agregat ke aliasnya.
                #    Contoh: { "SUM(o.amount)": "total_spent" }
                agg_to_alias_map = {
                    expr.this.sql(dialect="duckdb"): expr.alias
                    for expr in original_select_clause.expressions
                    if isinstance(expr, exp.Alias) and expr.find(exp.AggFunc)
                }

                # 5. Ganti SEMUA fungsi agregat (di SELECT list dan di OVER clause)
                #    dengan kolom aliasnya karena agregasi sudah terjadi.
                for agg_func in final_query_ast.find_all(exp.AggFunc):
                    agg_sql = agg_func.sql(dialect="duckdb")
                    if agg_sql in agg_to_alias_map:
                        # Ganti SUM(o.amount) dengan kolom `total_spent`
                        agg_func.replace(exp.column(agg_to_alias_map[agg_sql]))

                # 6. Hapus alias tabel dari kolom (cth: u.city -> city) karena
                #    'aggregated_results' adalah tabel datar tanpa alias internal.
                for col in final_query_ast.find_all(exp.Column):
                    col.set('table', None)
                
                final_select_sql = final_query_ast.sql(dialect="duckdb")
            # --- PERSIAPAN KUERI UNTUK WORKER ---
            query_for_workers = query_ast.copy()

            if final_select_sql:
                select_clause_for_worker = query_for_workers.find(exp.Select)
                new_expressions = []
                for expr in select_clause_for_worker.expressions:
                    if not expr.find(exp.Window):
                        new_expressions.append(expr)
                select_clause_for_worker.set('expressions', new_expressions)
            
            if query_for_workers.find(exp.Where): query_for_workers.find(exp.Where).pop()
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
                left_reader_function=left_reader_func,
                right_reader_function=right_reader_func,
                left_where_sql=left_where_sql,
                right_where_sql=right_where_sql,
                final_select_sql=final_select_sql
            )
            
        else: # Bukan Join
            table_name = all_tables_in_query[0]
            reader_func, _ = Planner._discover_table_files_and_reader(table_name)
            
            where_str = ""
            if where_clause := query_ast.find(exp.Where):
                where_str = f" {where_clause.sql(dialect='duckdb')}" 
            
            worker_template = f"SELECT * FROM {reader_func}({{files}}){where_str}"

            return DistributedScanPlan(
                query_ast=query_ast,
                table_name=table_name,
                worker_query_template=worker_template,
            )
