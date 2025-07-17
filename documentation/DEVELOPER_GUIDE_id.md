# Panduan Developer Quack-Cluster

Selamat datang di tim Quack-Cluster! Dokumen ini adalah panduan praktis untuk developer yang ingin memahami alur kerja kode secara mendalam, memodifikasi, atau menambahkan fitur baru ke sistem.

---

## 1. Alur Eksekusi Query (Walkthrough Detail)

Untuk benar-benar memahami cara kerja sistem, mari kita telusuri beberapa contoh query dari awal hingga akhir.

### Walkthrough 1: Query Sederhana (Lokal)
Query ini tidak memerlukan data dari disk dan dapat dieksekusi secara instan.
**SQL**: `SELECT 42 * 2;`

1.  **Coordinator**: `execute_query` menerima request. `parse_one` mengubah SQL menjadi AST.
2.  **Coordinator**: `resolve_and_execute` dipanggil. Karena tidak ada CTE, UNION, atau Subquery, ia langsung menuju ke logika perencanaan.
3.  **Planner**: `create_plan` dipanggil. Ia mendeteksi bahwa query ini tidak memiliki klausa `FROM` (tidak ada tabel).
4.  **Planner**: Ia mengembalikan `LocalExecutionPlan` yang hanya berisi AST asli.
5.  **Executor**: `execute` dipanggil. Ia melihat tipe rencana adalah `local` dan memanggil `_execute_local`.
6.  **Executor**: `_execute_local` menjalankan SQL `SELECT 42 * 2;` menggunakan koneksi DuckDB-nya dan langsung mengembalikan hasilnya.
7.  **Coordinator**: Hasil (tabel Arrow) diterima dan diformat sebagai JSON/Arrow untuk response.

### Walkthrough 2: Query Scan Terdistribusi
Query ini membaca data dari satu tabel, memfilternya, dan melakukan agregasi.
**SQL**: `SELECT passenger_count, COUNT(*) FROM "yellow_tripdata.parquet" WHERE PULocationID > 100 GROUP BY 1;`

1.  **Coordinator**: Mirip seperti di atas, AST disederhanakan dan diserahkan ke Planner.
2.  **Planner**: `create_plan` dipanggil. Ia mendeteksi ada satu tabel (`yellow_tripdata.parquet`) dan tidak ada JOIN.
3.  **Planner**: Ia mengembalikan `DistributedScanPlan` yang berisi:
    * `table_name`: `"yellow_tripdata.parquet"`
    * `worker_query_template`: `"SELECT * FROM read_parquet({files}) WHERE PULocationID > 100"`
    * `query_ast`: AST asli untuk digunakan di agregasi final.
4.  **Executor**: `execute` memanggil `_execute_scan`.
5.  **Executor**: `_execute_scan` menemukan semua file yang cocok dengan `yellow_tripdata.parquet*`, membuat worker Ray untuk setiap file, dan memanggil `worker.query.remote()` dengan template di atas. Semua worker bekerja secara paralel untuk memfilter data.
6.  **Executor**: `asyncio.gather` mengumpulkan semua hasil (tabel Arrow yang sudah difilter) dari para worker. Hasil-hasil ini digabungkan (`pa.concat_tables`) menjadi satu tabel besar bernama `combined_arrow_table`.
7.  **Executor**: Ia memodifikasi AST asli: klausa `WHERE` dihapus (karena sudah diterapkan oleh worker) dan nama tabel diganti dengan `combined_arrow_table`. Query final menjadi: `SELECT passenger_count, COUNT(*) FROM combined_arrow_table GROUP BY 1;`.
8.  **Executor**: Query final ini dijalankan di koneksi DuckDB koordinator untuk menghasilkan agregasi akhir.
9.  **Coordinator**: Hasil dikembalikan ke user.

### Walkthrough 3: Query Shuffle Join Terdistribusi
Ini adalah alur yang paling kompleks.
**SQL**: `SELECT c.c_name, o.o_orderstatus FROM customer AS c JOIN orders AS o ON c.c_custkey = o.o_custkey;`

1.  **Coordinator**: AST disederhanakan dan diserahkan ke Planner.
2.  **Planner**: `create_plan` mendeteksi adanya `JOIN`.
3.  **Planner**: Ia mengembalikan `DistributedShuffleJoinPlan` yang berisi:
    * Info tabel kiri & kanan (`customer`, `orders`).
    * Info alias (`c`, `o`).
    * Info kunci join (`c_custkey`, `o_custkey`).
    * `worker_join_sql`: Versi query yang dimodifikasi untuk dijalankan di worker, contoh: `SELECT c.c_name, o.o_orderstatus FROM c_local AS c JOIN o_local AS o ON c.c_custkey = o.o_custkey;`
4.  **Executor**: `execute` memanggil `_execute_join`. Ini memulai alur 3 fase:
    * **Fase 1: Map/Partition**:
        * `Executor` menemukan semua file untuk `customer` dan `orders`.
        * Ia memanggil `worker.partition_by_key.remote()` untuk setiap file.
        * Setiap worker membaca datanya dan membaginya menjadi `N` partisi berdasarkan `HASH(join_key) % N`. Semua baris dengan kunci join yang sama dijamin berakhir di partisi dengan ID yang sama.
    * **Fase 2: Shuffle & Reduce/Join**:
        * `Executor` mengumpulkan semua partisi dari semua worker (ini adalah "shuffle" yang terjadi di memori koordinator). Data dikelompokkan berdasarkan ID partisi.
        * Untuk setiap ID partisi `i`, `Executor` mengambil semua partisi kiri dan kanan, lalu memanggil `worker.join_partitions.remote()`.
        * Worker yang menerima tugas ini menggabungkan semua partisi kiri menjadi satu tabel `c_local` dan semua partisi kanan menjadi satu tabel `o_local`, lalu mengeksekusi `worker_join_sql` pada kedua tabel tersebut.
    * **Fase 3: Finalize**:
        * `Executor` mengumpulkan hasil join dari setiap worker.
        * Hasil-hasil ini digabungkan menjadi satu tabel final bernama `partial_results`.
        * Jika ada agregasi, `ORDER BY`, atau `LIMIT` di query asli, `Executor` akan menerapkannya pada `partial_results` di koordinator.
5.  **Coordinator**: Hasil akhir dikembalikan ke user.

---

## 2. Cara Menambah Fitur Baru (Studi Kasus: Broadcast Join)

Arsitektur ini memudahkan penambahan fitur. Mari kita lihat cara menambahkan strategi **Broadcast Join**, yang efisien jika satu tabel sangat kecil.

#### Langkah 1: Definisikan Plan Baru
Buka `quack_cluster/execution_plan.py` dan tambahkan kelas baru:

```python
class DistributedBroadcastJoinPlan(BasePlan):
    plan_type: Literal["broadcast_join"] = "broadcast_join"
    large_table_name: str
    small_table_name: str
    small_table_alias: str
    # Tambahkan field lain yang relevan (join key, dll.)
````

#### Langkah 2: Perbarui Planner

Buka `quack_cluster/planner.py`. Di dalam metode `create_plan`, sebelum logika `Shuffle Join`, tambahkan logika baru:

```python
# ... di dalam create_plan, setelah cek LocalExecutionPlan
if join_expression := query_ast.find(exp.Join):
    # Cek ukuran tabel (misalnya dengan 'os.path.getsize')
    is_left_small = check_if_table_is_small("left_table_name")
    
    if is_left_small:
        # LOGIKA BARU: Buat Broadcast Join Plan
        return DistributedBroadcastJoinPlan(
            # ... isi semua field yang dibutuhkan ...
        )

    # ... logika Shuffle Join yang sudah ada
# ...
```

#### Langkah 3: Implementasikan Eksekusi

Buka `quack_cluster/executor.py`.

1.  Tambahkan `elif isinstance(plan, DistributedBroadcastJoinPlan):` di metode `execute`.
2.  Buat metode baru `async def _execute_broadcast_join(self, plan, con)`:
      * Baca tabel kecil (`small_table_name`) ke memori koordinator menjadi satu tabel Arrow.
      * Buat worker untuk setiap file dari tabel besar (`large_table_name`).
      * Panggil metode worker baru (misal: `worker.join_with_broadcast.remote()`) dan kirim tabel Arrow kecil tersebut sebagai argumen ke *setiap* panggilan. Ray akan menangani serialisasi dan pengiriman objek ini secara efisien.
      * Kumpulkan hasilnya dan lakukan agregasi final.

#### Langkah 4: Tambah Kemampuan Worker

Buka `quack_cluster/worker.py` dan tambahkan metode baru:

```python
def join_with_broadcast(self, large_table_file: str, broadcast_table: pa.Table, broadcast_alias: str, join_sql: str) -> pa.Table:
    # Daftarkan tabel kecil yang diterima dari memori
    self.con.register(broadcast_alias, broadcast_table)
    
    # Baca file besar dan jalankan join
    query = join_sql.format(large_table_file=large_table_file) # Modifikasi SQL sesuai kebutuhan
    return self.con.execute(query).fetch_arrow_table()
```

Dengan empat langkah ini, Anda telah mengintegrasikan strategi join baru tanpa merusak alur yang sudah ada.

```
```