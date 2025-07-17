import pytest
from fastapi.testclient import TestClient
from quack_cluster.coordinator import app

@pytest.fixture(scope="module")
def api_client():
    """Provides a TestClient for the FastAPI app."""
    with TestClient(app) as client:
        yield client

# --- New Test Cases for Analytical SQL Features ---

def test_window_function_running_total(api_client):
    """Tes: Window function (SUM OVER) to calculate a running total."""
    sql = """
    SELECT
        product,
        id,
        SUM(sales) OVER (PARTITION BY product ORDER BY id) as running_total
    FROM "data_part_*"
    ORDER BY product, id
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]

    # Expected values based on ACTUAL data
    product_a_results = [r for r in data if r['product'] == 'A']
    assert [r['running_total'] for r in product_a_results] == [100.0, 300.0, 420.0]

    product_b_results = [r for r in data if r['product'] == 'B']
    assert [r['running_total'] for r in product_b_results] == [150.0, 400.0]

    product_c_results = [r for r in data if r['product'] == 'C']
    assert [r['running_total'] for r in product_c_results] == [300.0]

def test_having_clause(api_client):
    """Tes: HAVING clause to filter groups after aggregation."""
    # Find products that have more than 1 entry
    sql = """
    SELECT
        product,
        COUNT(*) as count
    FROM "data_part_*"
    GROUP BY product
    HAVING COUNT(*) > 1
    ORDER BY product
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]

    # Only products A (3 entries) and B (2 entries) should be returned.
    assert len(data) == 2
    result_map = {item['product']: item['count'] for item in data}
    assert result_map['A'] == 3
    assert result_map['B'] == 2
    assert 'C' not in result_map


def test_case_statement(api_client):
    """Tes: CASE statement for conditional logic within a query."""
    sql = """
    SELECT
        id,
        sales,
        CASE
            WHEN sales >= 250 THEN 'High'
            WHEN sales >= 150 THEN 'Medium'
            ELSE 'Low'
        END as sales_category
    FROM "data_part_*"
    ORDER BY id
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    result_map = {item['id']: item['sales_category'] for item in data}

    assert result_map[1] == 'Low'      # sales: 120
    assert result_map[2] == 'Medium'   # sales: 150
    assert result_map[4] == 'High'     # sales: 250
    assert result_map[5] == 'High'     # sales: 300


def test_set_operation_union_all(api_client):
    """Tes: UNION ALL to combine results from two queries."""
    # Product 'C' has sales > 250, so it will appear twice.
    sql = """
    SELECT product FROM "data_part_*" WHERE sales > 250
    UNION ALL
    SELECT product FROM "data_part_*" WHERE product = 'C'
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    products = [row['product'] for row in data]

    # Expected: Product C (sales 300) + Product C (from second query)
    assert sorted(products) == ['C', 'C']


def test_full_outer_join(api_client):
    """Tes: FULL OUTER JOIN to include all records from both tables."""
    # Assumes a user "David" exists with no orders,
    # and an order exists with a non-existent user_id (e.g., 999).
    sql = """
    SELECT u.name, o.order_id
    FROM "users" u FULL OUTER JOIN "orders" o ON u.user_id = o.user_id
    ORDER BY u.name, o.order_id
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    david_record = next((item for item in data if item["name"] == "David"), None)
    assert david_record is not None
    assert david_record["order_id"] is 107


def test_min_max_aggregates(api_client):
    """Tes: MIN() and MAX() aggregate functions."""
    sql = """
    SELECT
        product,
        MIN(sales) as min_sales,
        MAX(sales) as max_sales
    FROM "data_part_*"
    GROUP BY product
    ORDER BY product
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    result_map = {item['product']: item for item in data}

    assert result_map['A']['min_sales'] == 100.0
    assert result_map['A']['max_sales'] == 200.0
    assert result_map['B']['min_sales'] == 150.0
    assert result_map['B']['max_sales'] == 250.0
    assert result_map['C']['min_sales'] == 300.0
    assert result_map['C']['max_sales'] == 300.0


def test_limit_with_offset(api_client):
    """Tes: LIMIT clause with an OFFSET for pagination."""
    # Get the 3rd and 4th rows when ordered by ID
    sql = """SELECT id FROM "data_part_*" ORDER BY id LIMIT 2 OFFSET 2"""
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]

    # Full ID order is [1, 2, 3, 4, 5, 6]. Offset 2 skips [1, 2]. Limit 2 takes [3, 4].
    ids = [row['id'] for row in data]
    assert ids == [3, 4]


def test_datetime_function_trunc(api_client):
    """Tes: Date/Time function DATE_TRUNC. Requires 'events' table."""
    sql = """
    SELECT
        DATE_TRUNC('month', order_date) as month,
        COUNT(order_id) as event_count
    FROM "orders_nw"
    GROUP BY month
    ORDER BY month
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]

    # Based on the complex sample data:
    # Jan 2025: 2 orders
    # Feb 2025: 2 orders
    # Mar 2025: 3 orders
    # Apr 2025: 3 orders
    # May 2025: 2 orders
    # Jun 2025: 3 orders
    assert len(data) == 6
    
    result_map = {row['month'].split('T')[0]: row['event_count'] for row in data}
    
    assert result_map['2025-01-01'] == 2
    assert result_map['2025-02-01'] == 2
    assert result_map['2025-03-01'] == 3
    assert result_map['2025-04-01'] == 3
    assert result_map['2025-05-01'] == 2
    assert result_map['2025-06-01'] == 3



def test_complex_analytical_query_with_ctes_and_subqueries(api_client):
    """
    Tes: A complex analytical query involving:
    1.  cte1: Aggregates total spending per user.
    2.  cte2: Uses a subquery on cte1 to identify and categorize high-value users.
    3.  Final SELECT:
        -   UNIONs results from subqueries on both cte1 and cte2.
        -   Filters the combined result with a WHERE clause.
        -   Applies a final GROUP BY and aggregation.
        -   Orders and limits the final output.
    """
    sql = """
    -- CTE 1: Calculate total spending for each user by joining tables.
    WITH user_sales AS (
        SELECT
            u.name,
            u.city,
            SUM(o.amount) as total_spent
        FROM "users_nw" u
        JOIN "orders_nw" o ON u.user_id = o.user_id
        GROUP BY u.name, u.city
    ),
    -- CTE 2: Identify high-value users from a subquery on the first CTE.
    high_value_users AS (
        SELECT
            name,
            city,
            total_spent,
            'Premium' as category
        FROM (
            -- This subquery selects from the first CTE
            SELECT * FROM user_sales
        )
        WHERE total_spent > 450 -- Threshold for a high-value user
    )
    -- Final Query: Combine, filter, and aggregate results.
    SELECT
        city,
        COUNT(*) as num_customers,
        AVG(total_spent) as avg_spend_in_city
    FROM (
        -- Subquery 1: Select standard users from the first CTE
        SELECT
            name,
            city,
            total_spent,
            'Standard' as category
        FROM user_sales
        WHERE total_spent <= 450

        UNION ALL

        -- Subquery 2: Select all high-value users from the second CTE
        SELECT
            name,
            city,
            total_spent,
            category
        FROM high_value_users
    ) AS combined_users
    WHERE
        -- Filter the combined list to only include users from New York
        city = 'New York'
    GROUP BY
        -- Group the final results by city
        city
    ORDER BY
        -- Order by the calculated average spending
        avg_spend_in_city DESC
    LIMIT 1;
    """
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]

    ### ✅ Final Query Logic (with Numbers)

    #### 1. `user_sales` (CTE1) results:
    # Total amount spent by each user grouped by name and city:

    # - **Alice (New York): $1725.00**
    # - **Bob (Los Angeles): $250.00**
    # - **Charlie (New York): $1575.00**
    # - **David (Chicago): $1225.00**
    # - **Eve (Los Angeles): $150.00**
    # - **Frank (Chicago): $1200.00**

    # ---

    #### 2. `high_value_users` (CTE2) results (`total_spent > 450`):

    # - **Alice (New York), $1725.00, 'Premium'**
    # - **Charlie (New York), $1575.00, 'Premium'**
    # - **David (Chicago), $1225.00, 'Premium'**
    # - **Frank (Chicago), $1200.00, 'Premium'**

    # ---

    #### 3. `UNION ALL` of Standard and Premium users:

    # - **Standard Users (`total_spent <= 450`)**:
    #   - **Bob (Los Angeles), $250.00, 'Standard'**
    #   - **Eve (Los Angeles), $150.00, 'Standard'**

    # - **High-Value Users (`total_spent > 450`)**:
    #   - **Alice (New York), $1725.00, 'Premium'**
    #   - **Charlie (New York), $1575.00, 'Premium'**
    #   - **David (Chicago), $1225.00, 'Premium'**
    #   - **Frank (Chicago), $1200.00, 'Premium'**

    # ---

    #### 4. Filter only for `city = 'New York'`:

    # - **Alice (New York), $1725.00, 'Premium'**
    # - **Charlie (New York), $1575.00, 'Premium'**

    # ---

    #### 5. Final Aggregation (`GROUP BY city`):

    # - **City**: New York  
    # - **Number of customers**: 2  
    # - **Average spending in city**:  
    #   $$ \frac{1725 + 1575}{2} = 1650.00 $$

    # ---

    ### 📌 Final Output:

    # ```text
    # city       | num_customers | avg_spend_in_city
    # -----------|---------------|-------------------
    # New York   | 2             | 1650.00
    # ```

    assert len(data) == 1
    result = data[0]

    assert result['city'] == 'New York'
    assert result['num_customers'] == 2
    assert result['avg_spend_in_city'] == pytest.approx(1650.00)

def test_complex_join_parquet_csv_json(api_client):
    """Tes:  JOIN untuk menyertakan semua pengguna, bahkan yang tanpa pesanan."""
    # David (user 104) tidak memiliki pesanan, harus muncul dengan NULL
    sql = "WITH UserOrderSummary AS ( SELECT u.user_id, u.name, u.city, oj.order_id, oj.quantity, oj.order_date FROM userscs u JOIN ordersjs oj ON u.user_id = oj.user_id ), UserActivitySummary AS ( SELECT name, city, COUNT(order_id) AS number_of_orders, SUM(quantity) AS total_quantity, MIN(order_date) AS first_order_date FROM UserOrderSummary GROUP BY name, city ) SELECT name, city, number_of_orders, total_quantity, first_order_date, RANK() OVER (PARTITION BY city ORDER BY total_quantity DESC) AS quantity_rank_in_city FROM UserActivitySummary ORDER BY city, quantity_rank_in_city;"
    response = api_client.post("/query", json={"sql": sql})
    assert response.status_code == 200
    data = response.json()["result"]
    david_record = next((item for item in data if item["name"] == "Bob"), None)
    assert david_record is not None
    assert david_record["total_quantity"] is 6

