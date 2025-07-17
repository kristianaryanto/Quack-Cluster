# Quack-Cluster Developer Guide

Welcome to the Quack-Cluster team\! This document is a practical guide for developers who want to deeply understand the code workflow, modify it, or add new features to the system.

-----

## 1\. Query Execution Flow (Detailed Walkthrough)

To truly understand how the system works, let's trace a few example queries from start to finish.

### Walkthrough 1: Simple (Local) Query

This query doesn't require data from disk and can be executed instantly.
**SQL**: `SELECT 42 * 2;`

1.  **Coordinator**: `execute_query` receives the request. `parse_one` converts the SQL into an AST.
2.  **Coordinator**: `resolve_and_execute` is called. Since there are no CTEs, UNIONs, or Subqueries, it proceeds directly to the planning logic.
3.  **Planner**: `create_plan` is called. It detects that this query has no `FROM` clause (no tables).
4.  **Planner**: It returns a `LocalExecutionPlan` that only contains the original AST.
5.  **Executor**: `execute` is called. It sees the plan type is `local` and calls `_execute_local`.
6.  **Executor**: `_execute_local` runs the SQL `SELECT 42 * 2;` using its DuckDB connection and immediately returns the result.
7.  **Coordinator**: The result (an Arrow table) is received and formatted as JSON/Arrow for the response.

### Walkthrough 2: Distributed Scan Query

This query reads data from a single table, filters it, and performs an aggregation.
**SQL**: `SELECT passenger_count, COUNT(*) FROM "yellow_tripdata.parquet" WHERE PULocationID > 100 GROUP BY 1;`

1.  **Coordinator**: Similar to the above, the AST is simplified and handed to the Planner.
2.  **Planner**: `create_plan` is called. It detects one table (`yellow_tripdata.parquet`) and no JOINs.
3.  **Planner**: It returns a `DistributedScanPlan` containing:
      * `table_name`: `"yellow_tripdata.parquet"`
      * `worker_query_template`: `"SELECT * FROM read_parquet({files}) WHERE PULocationID > 100"`
      * `query_ast`: The original AST to be used for the final aggregation.
4.  **Executor**: `execute` calls `_execute_scan`.
5.  **Executor**: `_execute_scan` finds all files matching `yellow_tripdata.parquet*`, creates a Ray worker for each file, and calls `worker.query.remote()` with the template above. All workers operate in parallel to filter the data.
6.  **Executor**: `asyncio.gather` collects all the results (filtered Arrow tables) from the workers. These results are concatenated (`pa.concat_tables`) into one large table named `combined_arrow_table`.
7.  **Executor**: It modifies the original AST: the `WHERE` clause is removed (since it was already applied by the workers) and the table name is replaced with `combined_arrow_table`. The final query becomes: `SELECT passenger_count, COUNT(*) FROM combined_arrow_table GROUP BY 1;`.
8.  **Executor**: This final query is run on the coordinator's DuckDB connection to produce the final aggregation.
9.  **Coordinator**: The result is returned to the user.

### Walkthrough 3: Distributed Shuffle Join Query

This is the most complex flow.
**SQL**: `SELECT c.c_name, o.o_orderstatus FROM customer AS c JOIN orders AS o ON c.c_custkey = o.o_custkey;`

1.  **Coordinator**: The AST is simplified and passed to the Planner.
2.  **Planner**: `create_plan` detects a `JOIN`.
3.  **Planner**: It returns a `DistributedShuffleJoinPlan` containing:
      * Left & right table info (`customer`, `orders`).
      * Alias info (`c`, `o`).
      * Join key info (`c_custkey`, `o_custkey`).
      * `worker_join_sql`: A modified version of the query to be run on workers, for example: `SELECT c.c_name, o.o_orderstatus FROM c_local AS c JOIN o_local AS o ON c.c_custkey = o.o_custkey;`
4.  **Executor**: `execute` calls `_execute_join`. This begins a 3-phase flow:
      * **Phase 1: Map/Partition**:
          * The `Executor` finds all files for `customer` and `orders`.
          * It calls `worker.partition_by_key.remote()` for each file.
          * Each worker reads its data and divides it into `N` partitions based on `HASH(join_key) % N`. All rows with the same join key are guaranteed to end up in the partition with the same ID.
      * **Phase 2: Shuffle & Reduce/Join**:
          * The `Executor` collects all partitions from all workers (this is the "shuffle" that happens in the coordinator's memory). The data is grouped by partition ID.
          * For each partition ID `i`, the `Executor` takes all left and right partitions, then calls `worker.join_partitions.remote()`.
          * The worker receiving this task concatenates all left partitions into a single `c_local` table and all right partitions into a single `o_local` table, then executes the `worker_join_sql` on those two tables.
      * **Phase 3: Finalize**:
          * The `Executor` gathers the join results from each worker.
          * These results are concatenated into a single final table named `partial_results`.
          * If there are aggregations, `ORDER BY`, or `LIMIT` clauses in the original query, the `Executor` will apply them to `partial_results` on the coordinator.
5.  **Coordinator**: The final result is returned to the user.

-----

## 2\. How to Add a New Feature (Case Study: Broadcast Join)

This architecture makes it easy to add features. Let's see how to add a **Broadcast Join** strategy, which is efficient if one table is very small.

#### Step 1: Define the New Plan

Open `quack_cluster/execution_plan.py` and add a new class:

```python
class DistributedBroadcastJoinPlan(BasePlan):
    plan_type: Literal["broadcast_join"] = "broadcast_join"
    large_table_name: str
    small_table_name: str
    small_table_alias: str
    # Add other relevant fields (join keys, etc.)
```

#### Step 2: Update the Planner

Open `quack_cluster/planner.py`. Inside the `create_plan` method, before the `Shuffle Join` logic, add new logic:

```python
# ... inside create_plan, after the LocalExecutionPlan check
if join_expression := query_ast.find(exp.Join):
    # Check table size (e.g., with 'os.path.getsize')
    is_left_small = check_if_table_is_small("left_table_name")
    
    if is_left_small:
        # NEW LOGIC: Create a Broadcast Join Plan
        return DistributedBroadcastJoinPlan(
            # ... fill in all required fields ...
        )

    # ... existing Shuffle Join logic ...
# ...
```

#### Step 3: Implement the Execution

Open `quack_cluster/executor.py`.

1.  Add `elif isinstance(plan, DistributedBroadcastJoinPlan):` in the `execute` method.
2.  Create a new method `async def _execute_broadcast_join(self, plan, con)`:
      * Read the small table (`small_table_name`) into the coordinator's memory as a single Arrow table.
      * Create a worker for each file of the large table (`large_table_name`).
      * Call a new worker method (e.g., `worker.join_with_broadcast.remote()`) and send the small Arrow table as an argument to *every* call. Ray will handle serializing and sending this object efficiently.
      * Gather the results and perform any final aggregations.

#### Step 4: Add the Worker Capability

Open `quack_cluster/worker.py` and add a new method:

```python
def join_with_broadcast(self, large_table_file: str, broadcast_table: pa.Table, broadcast_alias: str, join_sql: str) -> pa.Table:
    # Register the small table received from memory
    self.con.register(broadcast_alias, broadcast_table)
    
    # Read the large file and execute the join
    query = join_sql.format(large_table_file=large_table_file) # Modify SQL as needed
    return self.con.execute(query).fetch_arrow_table()
```

With these four steps, you have integrated a new join strategy without breaking existing flows.