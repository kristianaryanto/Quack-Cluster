# quack_cluster/execution_plan.py
from pydantic import BaseModel
from typing import List, Tuple, Any, Literal,Optional
# from sqlglot import exp
from sqlglot import exp, parse_one
from sqlglot.errors import ParseError

class BasePlan(BaseModel):
    """Kelas dasar untuk semua rencana eksekusi."""
    plan_type: str
    query_ast: exp.Expression # AST asli atau yang sudah dimodifikasi untuk tahap final

    class Config:
        arbitrary_types_allowed = True

class LocalExecutionPlan(BasePlan):
    """Rencana untuk query yang bisa dieksekusi sepenuhnya di koordinator."""
    plan_type: Literal["local"] = "local"

class DistributedScanPlan(BasePlan):
    """Rencana untuk memindai file terdistribusi (SELECT, WHERE, GROUP BY sederhana)."""
    plan_type: Literal["scan"] = "scan"
    table_name: str
    worker_query_template: str


class DistributedShuffleJoinPlan(BasePlan):
    """Rencana untuk shuffle join terdistribusi."""
    plan_type: Literal["join"] = "join"
    
    left_table_name: str
    left_table_alias: str
    left_join_key: str
    right_table_name: str
    right_table_alias: str
    right_join_key: str
    worker_join_sql: str
    
    left_reader_function: str
    right_reader_function: str


    left_where_sql: str
    right_where_sql: str

    final_select_sql: Optional[str] = None

class DistributedBroadcastJoinPlan(BasePlan):
    """Rencana untuk broadcast join terdistribusi."""
    plan_type: Literal["broadcast_join"] = "broadcast_join"
    
    # Informasi tabel besar yang akan dipindai secara terdistribusi
    large_table_name: str
    large_table_alias: str
    large_table_reader_func: str
    
    # Informasi tabel kecil yang akan disiarkan
    small_table_name: str
    small_table_alias: str
    small_table_reader_func: str

    # Kueri yang akan dijalankan di worker (sudah berisi placeholder untuk tabel besar)
    worker_join_sql: str