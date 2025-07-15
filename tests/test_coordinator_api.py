# tests/test_coordinator_api.py

import pytest

# Skenario Tes Sukses
# =====================

def test_select_all(api_client):
    """Tes: Mengambil semua data dari beberapa file."""
    response = api_client.post("/query", json={"sql": "SELECT * FROM \"data_part_*\" ORDER BY id"})
    assert response.status_code == 200
    data = response.json()["result"]
    assert len(data) == 6
    assert data[0]["id"] == 1
    assert data[5]["id"] == 6

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


# Skenario Tes Kegagalan (Error Handling)
# ======================================

def test_table_not_found(api_client):
    """Tes: Memastikan error 404 jika tabel tidak ada."""
    response = api_client.post("/query", json={"sql": "SELECT * FROM \"non_existent_table\""})
    assert response.status_code == 404
    assert "Table 'non_existent_table' not found" in response.json()["detail"]

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
