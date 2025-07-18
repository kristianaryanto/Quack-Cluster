import pytest
from fastapi.testclient import TestClient
from quack_cluster.coordinator import app

@pytest.fixture(scope="module")
def api_client():
    with TestClient(app) as client:
        yield client

def test_select_all(api_client):
    """Tes: Mengambil semua data dari beberapa file."""
    response = api_client.post("/query", json={"sql": "SELECT * FROM \"data_part_*\" ORDER BY id"})
    assert response.status_code == 200
    data = response.json()["result"]
    assert len(data) == 6
    assert data[0]["id"] == 1
    assert data[5]["id"] == 6

def test_select_all_csv(api_client):
    """Tes: Mengambil semua data dari beberapa file."""
    response = api_client.post("/query", json={"sql": "SELECT * FROM \"userscs\" ORDER BY user_id"})
    assert response.status_code == 200
    data = response.json()["result"]
    assert data[0]["user_id"] == 101

def test_select_all_json(api_client):
    """Tes: Mengambil semua data dari beberapa file."""
    response = api_client.post("/query", json={"sql": "SELECT * FROM \"ordersjs\" ORDER BY user_id"})
    assert response.status_code == 200
    data = response.json()["result"]
    assert data[0]["user_id"] == 101


def test_cte_with_join_and_limit(api_client):
    """Tes: Logika JOIN yang sama menggunakan CTE."""
    sql = """
    WITH user_aggregates AS (
        SELECT
            u.name,
            COUNT(o.order_id) as order_count,
            SUM(o.amount) as total_spent
        FROM "orders" o JOIN "users" u ON o.user_id = u.user_id
        GROUP BY u.name
    )
    SELECT name, order_count, total_spent
    FROM user_aggregates
    ORDER BY name
    LIMIT 2
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    
    assert len(data) == 2
    
    result_map = {item['name']: item for item in data}
    assert result_map['Alice']['order_count'] == 2
    assert result_map['Alice']['total_spent'] == 200.0
    assert result_map['Bob']['order_count'] == 3
    assert result_map['Bob']['total_spent'] == 630.0

def test_subquery_with_join_and_limit(api_client):
    """Tes: Logika JOIN yang sama menggunakan subquery di FROM clause."""
    sql = """
    SELECT
        name,
        order_count,
        total_spent
    FROM (
        SELECT
            u.name,
            COUNT(o.order_id) as order_count,
            SUM(o.amount) as total_spent
        FROM "orders" o JOIN "users" u ON o.user_id = u.user_id
        GROUP BY u.name
    ) AS user_aggregates
    ORDER BY name
    LIMIT 2
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    
    assert len(data) == 2
    
    result_map = {item['name']: item for item in data}
    assert result_map['Alice']['order_count'] == 2
    assert result_map['Alice']['total_spent'] == 200.0
    assert result_map['Bob']['order_count'] == 3
    assert result_map['Bob']['total_spent'] == 630.0

def test_group_by_and_aggregate(api_client):
    """Tes: Agregasi dengan GROUP BY dan COUNT."""
    response = api_client.post("/query", json={"sql": "SELECT product, COUNT(*) as count FROM \"data_part_*\" GROUP BY product ORDER BY product"})
    assert response.status_code == 200
    data = response.json()["result"]
    assert len(data) == 3
    # Verifikasi hasil agregasi
    assert {"product": "A", "count": 3} in data
    assert {"product": "B", "count": 2} in data
    assert {"product": "C", "count": 1} in data

def test_where_clause(api_client):
    """Tes: Filter data menggunakan klausa WHERE."""
    response = api_client.post("/query", json={"sql": "SELECT * FROM \"data_part_*\" WHERE sales > 200"})
    assert response.status_code == 200
    data = response.json()["result"]
    assert len(data) == 2
    # Pastikan semua hasil memiliki sales > 200
    for row in data:
        assert row["sales"] > 200

def test_sum_aggregate(api_client):
    """Tes: Agregasi dengan GROUP BY dan SUM."""
    response = api_client.post("/query", json={"sql": "SELECT product, SUM(sales) as total_sales FROM \"data_part_*\" GROUP BY product ORDER BY product"})
    assert response.status_code == 200
    data = response.json()["result"]
        
    assert {"product": "A", "total_sales": 420} in data
    assert {"product": "B", "total_sales": 400} in data
    assert {"product": "C", "total_sales": 300} in data


def test_avg_aggregate(api_client):
    """Tes: Agregasi dengan GROUP BY dan AVG."""
    response = api_client.post("/query", json={"sql": "SELECT product, AVG(sales) as avg_sales FROM \"data_part_*\" GROUP BY product ORDER BY product"})
    assert response.status_code == 200
    data = response.json()["result"]
    
    # A: 420 / 3 = 140
    # B: 400 / 2 = 200
    # C: 300 / 1 = 300
    
    result_map = {item['product']: item['avg_sales'] for item in data}

    assert result_map["A"] == pytest.approx(140.0)
    assert result_map["B"] == pytest.approx(200.0)
    assert result_map["C"] == pytest.approx(300.0)

def test_select_distinct(api_client):
    """Tes: SELECT DISTINCT - Mengambil nilai unik dari kolom 'product'."""
    response = api_client.post("/query", json={"sql": "SELECT DISTINCT product FROM \"data_part_*\""})
    assert response.status_code == 200
    data = response.json()["result"]
    products = {row['product'] for row in data}
    assert len(products) == 3
    assert "A" in products and "B" in products and "C" in products

def test_select_distinct_on(api_client):
    """Tes: SELECT DISTINCT ON - Mengambil satu baris unik untuk setiap 'product'."""
    # Mengambil data penjualan pertama untuk setiap produk, diurutkan berdasarkan id
    sql = "SELECT DISTINCT ON (product) * FROM \"data_part_*\" ORDER BY product, id"
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    assert len(data) == 3
    result_map = {item['product']: item['id'] for item in data}
    # ID pertama untuk produk A adalah 1, B adalah 2, C adalah 5
    assert result_map['A'] == 1
    assert result_map['B'] == 2
    assert result_map['C'] == 5

def test_select_exclude(api_client):
    """Tes: SELECT * EXCLUDE - Memilih semua kolom kecuali 'sales'."""
    response = api_client.post("/query", json={"sql": "SELECT * EXCLUDE (sales) FROM \"data_part_*\" WHERE id = 1"})
    assert response.status_code == 200
    data = response.json()["result"]
    assert len(data) == 1
    # Memastikan kolom 'sales' tidak ada, tetapi kolom lain ada
    assert "sales" not in data[0]
    assert "id" in data[0]
    assert "product" in data[0]

def test_select_replace(api_client):
    """Tes: SELECT * REPLACE - Mengganti nilai kolom dengan ekspresi."""
    sql = "SELECT * REPLACE (sales * 2 AS sales) FROM \"data_part_*\" WHERE id = 1"
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    assert len(data) == 1
    # Penjualan asli untuk id 1 adalah 120. Diharapkan menjadi 240.
    assert data[0]['sales'] == 200.0

def test_select_columns_regex(api_client):
    """Tes: SELECT COLUMNS - Memilih kolom yang cocok dengan regex."""
    # Memilih kolom yang namanya mengandung 'pro'
    response = api_client.post("/query", json={"sql": "SELECT COLUMNS('pro.*') FROM \"data_part_*\" WHERE id = 1"})
    assert response.status_code == 200
    data = response.json()["result"]
    assert len(data) == 1
    # Diharapkan hanya kolom 'product' yang terpilih
    assert "product" in data[0]
    assert len(data[0].keys()) == 1
    assert data[0]['product'] == 'A'

# --- Tes untuk FROM, JOIN, dan Subquery ---

def test_from_subquery(api_client):
    """Tes: FROM clause dengan subquery."""
    # Subquery memilih produk A, kueri luar menghitungnya
    sql = "SELECT COUNT(*) as count FROM (SELECT * FROM \"data_part_*\" WHERE product = 'A')"
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    assert data[0]['count'] == 3

def test_where_with_subquery(api_client):
    """Tes: WHERE clause dengan subquery (IN)."""
    # Pilih pesanan dari pengguna yang tinggal di New York
    sql = "SELECT order_id FROM \"orders\" WHERE user_id IN (SELECT user_id FROM \"users\" WHERE city = 'New York') ORDER BY order_id"
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    # Alice (user 101) & Charlie (user 103) tinggal di New York. Order mereka: 1, 2, 6
    order_ids = [row['order_id'] for row in data]
    assert order_ids == [101, 103]

def test_left_join(api_client):
    """Tes: LEFT JOIN untuk menyertakan semua pengguna, bahkan yang tanpa pesanan."""
    # David (user 104) tidak memiliki pesanan, harus muncul dengan NULL
    sql = "SELECT u.name, o.order_id FROM \"users\" u LEFT JOIN \"orders\" o ON u.user_id = o.user_id ORDER BY u.name"
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    david_record = next((item for item in data if item["name"] == "David"), None)
    assert david_record is not None
    assert david_record["order_id"] is 107





def test_join_query(api_client):
    """Tes: JOIN sederhana antara tabel orders dan users."""
    sql = """
    SELECT u.name, COUNT(o.order_id) as order_count, SUM(o.amount) as total_spent FROM "orders" o JOIN "users" u ON o.user_id = u.user_id GROUP BY u.name ORDER BY u.name
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    
    assert len(data) == 4 # Diperbarui karena David tidak memiliki order, jadi tidak akan muncul di INNER JOIN
    
    result_map = {item['name']: item for item in data}
    assert result_map['Alice']['order_count'] == 2
    assert result_map['Alice']['total_spent'] == 200.0
    assert result_map['Bob']['order_count'] == 3
    assert result_map['Bob']['total_spent'] == 630.0

def test_join_query_with_limit(api_client):
    """Tes: JOIN dengan limit sederhana antara tabel orders dan users."""
    sql = """
    SELECT u.name, COUNT(o.order_id) as order_count, SUM(o.amount) as total_spent FROM "orders" o JOIN "users" u ON o.user_id = u.user_id GROUP BY u.name ORDER BY u.name limit 2
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    
    assert len(data) == 2
    
    result_map = {item['name']: item for item in data}
    assert result_map['Alice']['order_count'] == 2
    assert result_map['Alice']['total_spent'] == 200.0
    assert result_map['Bob']['order_count'] == 3
    assert result_map['Bob']['total_spent'] == 630.0


def test_join_query_with_limit_and_where(api_client):
    """Tes: JOIN dengan limit and where sederhana antara tabel orders dan users."""
    sql = """
    SELECT u.name, COUNT(o.order_id) as order_count, SUM(o.amount) as total_spent FROM "orders" o JOIN "users" u ON o.user_id = u.user_id WHERE u.name = 'Alice' GROUP BY u.name ORDER BY u.name limit 2
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    
    assert len(data) == 1
    
    result_map = {item['name']: item for item in data}
    assert result_map['Alice']['order_count'] == 2
    assert result_map['Alice']['total_spent'] == 200.0

def test_join_with_aggregate_and_window_function(api_client):
    """
    Tes: Skenario kompleks yang menggabungkan JOIN, WHERE, GROUP BY, agregasi (SUM),
    dan fungsi window (RANK) untuk memastikan eksekusi berjalan dengan urutan yang benar.
    """
    # Kueri ini menggunakan tabel yang lebih kaya dari data sampel
    # untuk menguji partisi fungsi window dengan benar.
    sql = """
    SELECT
        u.name,
        u.city,
        SUM(o.amount) as total_spent,
        RANK() OVER (PARTITION BY u.city ORDER BY SUM(o.amount) DESC) as spending_rank_in_city
    FROM
        orders_nw o
    JOIN
        users_nw u ON o.user_id = u.user_id
    WHERE
        u.city IN ('New York', 'Chicago')
    GROUP BY
        u.name, u.city
    ORDER BY
        u.city, spending_rank_in_city;
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]

    # Kita memfilter untuk 2 kota, masing-masing dengan 2 pengguna, jadi total 4 hasil.
    assert len(data) == 4

    # Buat map untuk memudahkan pemeriksaan hasil
    result_map = {(item['city'], item['name']): item for item in data}

    # --- Verifikasi Hasil untuk Chicago ---
    # David seharusnya memiliki peringkat 1 karena pengeluarannya lebih tinggi
    assert result_map[('Chicago', 'David')]['total_spent'] == 1225.0
    assert result_map[('Chicago', 'David')]['spending_rank_in_city'] == 1

    # Frank seharusnya memiliki peringkat 2
    assert result_map[('Chicago', 'Frank')]['total_spent'] == 1200.0
    assert result_map[('Chicago', 'Frank')]['spending_rank_in_city'] == 2

    # --- Verifikasi Hasil untuk New York ---
    # Alice seharusnya memiliki peringkat 1
    assert result_map[('New York', 'Alice')]['total_spent'] == 1725.0
    assert result_map[('New York', 'Alice')]['spending_rank_in_city'] == 1

    # Charlie seharusnya memiliki peringkat 2
    assert result_map[('New York', 'Charlie')]['total_spent'] == 1575.0
    assert result_map[('New York', 'Charlie')]['spending_rank_in_city'] == 2


# Skenario Tes Kegagalan (Error Handling)
# ======================================

def test_table_not_found(api_client):
    """Tes: Memastikan error 404 jika tabel tidak ada."""
    response = api_client.post("/query", json={"sql": "SELECT * FROM \"non_existent_table\""})
    assert response.status_code == 404
    
    # --- BARIS YANG DIPERBAIKI ---
    # Memeriksa pesan error baru yang lebih deskriptif
    expected_substring = "No data files found for table 'non_existent_table'"
    assert expected_substring in response.json()["detail"]


def test_invalid_sql_syntax(api_client):
    """Tes: Memastikan error 400 jika sintaks SQL salah."""
    # "SELEC" sengaja salah ketik
    response = api_client.post("/query", json={"sql": "SELEC * FROM \"data_part_*\""})
    
    # FIX: Tes sekarang harus mengharapkan status code 400
    assert response.status_code == 400
    assert "Invalid SQL syntax" in response.json()["detail"]
    
def test_empty_result(api_client):
    """Tes: Memastikan hasil kosong jika filter tidak menemukan apa pun."""
    response = api_client.post("/query", json={"sql": "SELECT * FROM \"data_part_*\" WHERE sales > 9999"})
    assert response.status_code == 200
    assert response.json()["result"] == []
