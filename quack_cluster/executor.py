# quack_cluster/executor.py
import asyncio
import glob
import os
from collections import defaultdict
import duckdb
import pyarrow as pa
from sqlglot import exp, parse_one
# from sqlglot import exp, parse_one
from sqlglot.errors import ParseError
from .execution_plan import BasePlan, LocalExecutionPlan, DistributedScanPlan, DistributedShuffleJoinPlan,DistributedBroadcastJoinPlan 
from .worker import DuckDBWorker
from .settings import settings
import logging
from typing import List, Callable, Any, Coroutine # <-- THIS LINE IS ADDED

logger = logging.getLogger(__name__)

class Executor:
    """
    Mengeksekusi sebuah ExecutionPlan yang diberikan dengan berinteraksi dengan Ray workers.
    Sekarang dengan toleransi kesalahan.
    """
    @staticmethod
    async def _execute_with_retries(
        initial_inputs: List[Any],
        task_function: Callable[[Any], Coroutine]
    ) -> List[Any]:
        """
        Menjalankan serangkaian task dengan mekanisme coba lagi (retry).

        Args:
            initial_inputs: Daftar input, setiap elemen akan diproses oleh satu task.
            task_function: Fungsi yang menerima satu input dan mengembalikan Ray task (ObjectRef).

        Returns:
            Daftar hasil dari task yang berhasil.

        Raises:
            Exception: Jika task terus gagal setelah semua percobaan ulang.
        """
        inputs_to_process = list(initial_inputs)
        all_results = []
        retries = 0

        while inputs_to_process and retries <= settings.MAX_RETRIES:
            if retries > 0:
                logger.warning(f"🔁 Retrying {len(inputs_to_process)} failed tasks... (Attempt {retries}/{settings.MAX_RETRIES})")
                await asyncio.sleep(1) # Beri jeda singkat sebelum mencoba lagi

            # Buat task baru hanya untuk input yang gagal
            tasks = [task_function(inp) for inp in inputs_to_process]
            # Buat pemetaan dari task kembali ke inputnya untuk melacak kegagalan
            task_to_input_map = {tasks[i]: inputs_to_process[i] for i in range(len(tasks))}

            # Jalankan task dan kumpulkan hasilnya, termasuk pengecualian (exceptions)
            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            failed_inputs = []
            for i, res in enumerate(task_results):
                if isinstance(res, Exception):
                    # Jika hasilnya adalah Exception (misalnya RayActorError), catat dan antrekan untuk dicoba lagi
                    failed_input = task_to_input_map[tasks[i]]
                    logger.error(f"‼️ Task for input '{failed_input}' failed. Error: {res}")
                    failed_inputs.append(failed_input)
                else:
                    # Jika berhasil, kumpulkan hasilnya
                    all_results.append(res)
            
            inputs_to_process = failed_inputs
            if inputs_to_process:
                retries += 1

        if inputs_to_process:
            # Jika masih ada input yang gagal setelah semua percobaan ulang, lemparkan exception
            error_message = f"🛑 Query failed after {settings.MAX_RETRIES} retries. Could not process inputs: {inputs_to_process}"
            logger.critical(error_message)
            raise Exception(error_message)

        return all_results

    @staticmethod
    async def execute(plan: BasePlan, con: duckdb.DuckDBPyConnection) -> pa.Table:
        """Metode utama untuk menjalankan rencana."""
        if isinstance(plan, LocalExecutionPlan):
            return await Executor._execute_local(plan, con)
        elif isinstance(plan, DistributedScanPlan):
            return await Executor._execute_scan(plan, con)
        elif isinstance(plan, DistributedShuffleJoinPlan):
            return await Executor._execute_join(plan, con)
        elif isinstance(plan, DistributedBroadcastJoinPlan):
            return await Executor._execute_broadcast_join(plan, con)
        else:
            raise NotImplementedError(f"Execution for plan type '{plan.plan_type}' is not implemented.")

    @staticmethod
    async def _execute_local(plan: LocalExecutionPlan, con: duckdb.DuckDBPyConnection) -> pa.Table:
        """Menjalankan query sederhana langsung di koordinator."""
        logger.info(f"Executing local plan: {plan.query_ast.sql()}")
        return con.execute(plan.query_ast.sql(dialect="duckdb")).fetch_arrow_table()


    @staticmethod
    async def _execute_scan(plan: DistributedScanPlan, final_con: duckdb.DuckDBPyConnection) -> pa.Table:
        logger.info(f"Executing distributed scan on table '{plan.table_name}'")
        file_pattern = os.path.join(settings.DATA_DIR, f"{plan.table_name}.*")
        all_files = glob.glob(file_pattern)
        if not all_files:
            raise FileNotFoundError(f"Table '{plan.table_name}' not found. Pattern: '{file_pattern}'.")

        # Definisikan fungsi untuk membuat task scan
        def create_scan_task(file_path: str):
            actor = DuckDBWorker.remote()
            return actor.query.remote(plan.worker_query_template, [file_path])

        # Gunakan helper fault-tolerant untuk menjalankan semua task
        partial_results = await Executor._execute_with_retries(all_files, create_scan_task)
        
        valid_results = [r for r in partial_results if r is not None and r.num_rows > 0]
        if not valid_results: return pa.Table.from_pydict({})


        combined_arrow_table = pa.concat_tables(valid_results)
        final_con.register("combined_arrow_table", combined_arrow_table)

        final_query_ast = plan.query_ast.copy()
        if final_query_ast.find(exp.Where): final_query_ast.find(exp.Where).pop()
        final_query_ast.find(exp.Table).replace(parse_one("combined_arrow_table"))
        final_query = final_query_ast.sql(dialect="duckdb")

        logger.info(f"Coordinator executing final scan aggregation: {final_query}")
        return final_con.execute(final_query).fetch_arrow_table()


    @staticmethod
    async def _execute_join(plan: DistributedShuffleJoinPlan, final_con: duckdb.DuckDBPyConnection) -> pa.Table:
        logger.info(f"Executing distributed join between '{plan.left_table_name}' and '{plan.right_table_name}'")
        
        # Fase 1: Map & Shuffle (dengan toleransi kesalahan)
        left_files = glob.glob(os.path.join(settings.DATA_DIR, f"{plan.left_table_name}.*"))
        right_files = glob.glob(os.path.join(settings.DATA_DIR, f"{plan.right_table_name}.*"))
        if not left_files or not right_files:
            raise FileNotFoundError(f"Data for join tables missing.")

        # Definisikan fungsi untuk membuat task partisi        
        
        def create_partition_task(file_info: tuple):
            file_path, join_key, reader_func, where_sql = file_info
            actor = DuckDBWorker.remote()
            return actor.partition_by_key.remote(file_path, join_key, settings.NUM_SHUFFLE_PARTITIONS, reader_func, where_sql)

        # Siapkan input untuk helper dengan menyertakan klausa WHERE
        left_inputs = [(f, plan.left_join_key, plan.left_reader_function, plan.left_where_sql) for f in left_files]
        right_inputs = [(f, plan.right_join_key, plan.right_reader_function, plan.right_where_sql) for f in right_files]
        
        if plan.left_where_sql: logger.info(f"Pushing down filter to left side: {plan.left_where_sql}")
        if plan.right_where_sql: logger.info(f"Pushing down filter to right side: {plan.right_where_sql}")
        
        # Jalankan task partisi untuk sisi kiri dan kanan secara paralel
        left_partitions_list, right_partitions_list = await asyncio.gather(
            Executor._execute_with_retries(left_inputs, create_partition_task),
            Executor._execute_with_retries(right_inputs, create_partition_task)
        )

        partitions_for_joiners = defaultdict(lambda: defaultdict(list))
        for partitioned_dict in left_partitions_list:
            for p_id, table in partitioned_dict.items():
                partitions_for_joiners[p_id]['left'].append(table)
        for partitioned_dict in right_partitions_list:
            for p_id, table in partitioned_dict.items():
                partitions_for_joiners[p_id]['right'].append(table)
        logger.info("✅ Shuffle phase complete.")

        # Fase 2: Reduce (Join) (dengan toleransi kesalahan)
        join_inputs = [
            (i, partitions_for_joiners[i]['left'], partitions_for_joiners[i]['right'])
            for i in range(settings.NUM_SHUFFLE_PARTITIONS) if i in partitions_for_joiners
        ]
        
        def create_join_task(join_input: tuple):
            partition_id, left_parts, right_parts = join_input
            actor = DuckDBWorker.remote()
            return actor.join_partitions.remote(left_parts, right_parts, plan.worker_join_sql, plan.left_table_alias, plan.right_table_alias)

        partial_results_list = await Executor._execute_with_retries(join_inputs, create_join_task)
        
        valid_partials = [tbl for tbl in partial_results_list if tbl.num_rows > 0]
        if not valid_partials: return pa.Table.from_pydict({})
        
        partial_results_table = pa.concat_tables(valid_partials)
        final_con.register("partial_results", partial_results_table)

        # --- TAHAP 3: AGREGASI FINAL (YANG DIPERBAIKI) ---
        final_agg_query = "SELECT * FROM partial_results"
        parsed_query = plan.query_ast

        if parsed_query.find(exp.AggFunc):
            final_selects = []
            final_group_bys = []

            if group_by_node := parsed_query.find(exp.Group):
                for expr in group_by_node.expressions:
                    final_group_bys.append(expr.alias_or_name)
            
            window_function_aliases = set()
            if plan.final_select_sql:
                final_select_ast = parse_one(plan.final_select_sql, read="duckdb")
                for expr in final_select_ast.expressions:
                    if expr.find(exp.Window):
                        window_function_aliases.add(expr.alias_or_name)

            final_selects.extend(final_group_bys)
            for expr in parsed_query.find(exp.Select).expressions:
                alias = expr.alias_or_name
                if alias in final_group_bys or alias in window_function_aliases:
                    continue
                if expr.find(exp.AggFunc):
                     final_selects.append(f"SUM({alias}) AS {alias}")

            final_agg_query = f"SELECT {', '.join(final_selects)} FROM partial_results"
            if final_group_bys:
                final_agg_query += f" GROUP BY {', '.join(final_group_bys)}"
        
        # KODE YANG SALAH TELAH DIHAPUS DARI SINI
        # JANGAN TAMBAHKAN ORDER BY ATAU LIMIT DI SINI

        logger.info(f"Coordinator executing final aggregation: {final_agg_query}")
        aggregated_results_table = final_con.execute(final_agg_query).fetch_arrow_table()

        # --- TAHAP 4: EKSEKUSI FUNGSI WINDOW (YANG DIPERBAIKI) ---
        final_table = aggregated_results_table
        if plan.final_select_sql:
            logger.info(f"Coordinator applying final projection with window functions...")
            final_con.register("aggregated_results", aggregated_results_table)
            
            # CUKUP GUNAKAN SQL YANG SUDAH DIBUAT DENGAN BENAR OLEH PLANNER
            window_query = plan.final_select_sql
            
            logger.info(f"Executing window function query: {window_query}")
            final_table = final_con.execute(window_query).fetch_arrow_table()

        # --- TAHAP 5: PENGURUTAN DAN PEMBATASAN FINAL (SATU-SATUNYA YANG BENAR) ---
        final_con.register("final_results_before_sort", final_table)
        final_ordered_query = "SELECT * FROM final_results_before_sort"

        if order_by := parsed_query.find(exp.Order):
            rewritten_order = order_by.copy()
            for col in rewritten_order.find_all(exp.Column):
                col.set('table', None)
            final_ordered_query += f" {rewritten_order.sql()}"
        
        if limit := parsed_query.find(exp.Limit):
            final_ordered_query += f" {limit.sql()}"

        logger.info(f"Coordinator applying final sort and limit: {final_ordered_query}")
        return final_con.execute(final_ordered_query).fetch_arrow_table()


    @staticmethod
    async def _execute_broadcast_join(plan: DistributedBroadcastJoinPlan, final_con: duckdb.DuckDBPyConnection) -> pa.Table:
        """
        Executes a broadcast join.
        1. Coordinator reads the small table into memory.
        2. It broadcasts this table to all workers.
        3. Each worker joins its partition of the large table with the broadcasted table.
        4. Coordinator gathers and aggregates the results.
        """
        logger.info(f"Executing broadcast join: small_table='{plan.small_table_name}', large_table='{plan.large_table_name}'")

        # 1. Coordinator reads the entire small table into a PyArrow Table.
        small_table_files = glob.glob(os.path.join(settings.DATA_DIR, f"{plan.small_table_name}.*"))
        if not small_table_files:
            raise FileNotFoundError(f"Data files for small table '{plan.small_table_name}' not found.")

        # Use a temporary DuckDB connection to read the files
        temp_con = duckdb.connect(database=':memory:')
        file_list_sql = ", ".join([f"'{f}'" for f in small_table_files])
        broadcast_table_arrow = temp_con.execute(
            f"SELECT * FROM {plan.small_table_reader_func}([{file_list_sql}])"
        ).fetch_arrow_table()
        temp_con.close()
        logger.info(f"✅ Prepared small table '{plan.small_table_name}' for broadcast ({broadcast_table_arrow.num_rows} rows).")


        # 2. Find all files for the large table and create worker tasks.
        large_table_files = glob.glob(os.path.join(settings.DATA_DIR, f"{plan.large_table_name}.*"))
        if not large_table_files:
            raise FileNotFoundError(f"Data files for large table '{plan.large_table_name}' not found.")

        # This helper function will be called by the fault-tolerant executor
        def create_broadcast_task(file_path: str):
            actor = DuckDBWorker.remote()
            # Format the worker SQL with the specific file path for the large table partition
            task_sql = plan.worker_join_sql.format(file_path=file_path)
            return actor.run_join_task.remote(
                task_sql,
                broadcast_table_arrow, # This is the broadcast!
                plan.small_table_alias
            )

        # 3. Execute tasks with retries
        # The _execute_with_retries helper can be used here without modification.
        partial_results = await Executor._execute_with_retries(large_table_files, create_broadcast_task)

        # 4. Gather results and perform final aggregation on the coordinator
        valid_results = [r for r in partial_results if r is not None and r.num_rows > 0]
        if not valid_results:
            return pa.Table.from_pydict({})

        combined_arrow_table = pa.concat_tables(valid_results)
        final_con.register("combined_arrow_table", combined_arrow_table)

        # The workers have performed the JOIN and WHERE.
        # Now, the coordinator runs the original query's logic on the combined data.
        final_query_ast = plan.query_ast.copy()

        # Remove the JOIN clause, as it's already been executed.
        if join_node := final_query_ast.find(exp.Join):
            join_node.pop()

        # Point the FROM clause to the in-memory results table.
        # This will find the single remaining table expression from the original FROM clause.
        if from_table := final_query_ast.find(exp.Table):
             from_table.replace(exp.to_table("combined_arrow_table"))
        else:
             raise ValueError("Could not find table to replace in final query AST.")

        # The WHERE clause was also handled by the workers. Remove it for the final aggregation step.
        if where_node := final_query_ast.find(exp.Where):
            where_node.pop()


        for col in final_query_ast.find_all(exp.Column):
            col.set('table', None)

        final_query = final_query_ast.sql(dialect="duckdb")
        # --- FIX ENDS HERE ---

        logger.info(f"Coordinator executing final aggregation/sort: {final_query}")

        return final_con.execute(final_query).fetch_arrow_table()