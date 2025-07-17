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
from .execution_plan import BasePlan, LocalExecutionPlan, DistributedScanPlan, DistributedShuffleJoinPlan
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
            file_path, join_key, reader_func = file_info
            actor = DuckDBWorker.remote()
            return actor.partition_by_key.remote(file_path, join_key, settings.NUM_SHUFFLE_PARTITIONS, reader_func)

        # Siapkan input untuk helper
        left_inputs = [(f, plan.left_join_key, plan.left_reader_function) for f in left_files]
        right_inputs = [(f, plan.right_join_key, plan.right_reader_function) for f in right_files]
        
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

        final_agg_query = "SELECT * FROM partial_results"
        parsed_query = plan.query_ast

        if parsed_query.find(exp.AggFunc):
            final_selects, final_group_bys = [], []
            if group_by_node := parsed_query.find(exp.Group):
                for expr in group_by_node.expressions:
                    final_group_bys.append(expr.name)
                    final_selects.append(expr.name)
            for expr in parsed_query.find(exp.Select).expressions:
                if expr.name in final_group_bys: continue
                agg_func = expr.find(exp.AggFunc)
                if isinstance(agg_func, (exp.Count, exp.Sum)):
                    final_selects.append(f"SUM({expr.alias_or_name}) AS {expr.alias_or_name}")
            final_agg_query = f"SELECT {', '.join(final_selects)} FROM partial_results"
            if final_group_bys: final_agg_query += f" GROUP BY {', '.join(final_group_bys)}"
        
        if order_by := parsed_query.find(exp.Order):
            rewritten_order = order_by.copy()
            for col in rewritten_order.find_all(exp.Column): col.set('table', None)
            final_agg_query += f" {rewritten_order.sql()}"
        
        if limit := parsed_query.find(exp.Limit):
            final_agg_query += f" {limit.sql()}"

        logger.info(f"Coordinator executing final join aggregation: {final_agg_query}")
        return final_con.execute(final_agg_query).fetch_arrow_table()