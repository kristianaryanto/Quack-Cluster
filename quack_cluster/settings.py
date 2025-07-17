import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime, timedelta



class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')
    DATA_DIR: str = "/app/sample_data"

settings = Settings()

# def generate_sample_data():
#     print(f"Generating sample data in '{settings.DATA_DIR}'...")
#     os.makedirs(settings.DATA_DIR, exist_ok=True)
#     df1 = pd.DataFrame({'id': [1, 2, 3], 'product': ['A', 'B', 'A'], 'sales': [100, 150, 200]})
#     df2 = pd.DataFrame({'id': [4, 5, 6], 'product': ['B', 'C', 'A'], 'sales': [250, 300, 120]})
#     pq.write_table(pa.Table.from_pandas(df1), os.path.join(settings.DATA_DIR, 'data_part_1.parquet'))
#     pq.write_table(pa.Table.from_pandas(df2), os.path.join(settings.DATA_DIR, 'data_part_2.parquet'))
#     print("Sample data generated.")


def generate_sample_data():
    
    print(f"Generating sample data in '{settings.DATA_DIR}'...")
    os.makedirs(settings.DATA_DIR, exist_ok=True)

    # Tabel Users
    users_df = pd.DataFrame({
        'user_id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston']
    })
    users_table = pa.Table.from_pandas(users_df)
    pq.write_table(users_table, os.path.join(settings.DATA_DIR, 'users.parquet'))

    # Tabel Orders
    orders_df = pd.DataFrame({
        'order_id': range(101, 108),
        'user_id': [1, 2, 1, 3, 2, 2, 4],
        'amount': [150, 200, 50, 300, 250, 180, 100]
    })
    orders_table = pa.Table.from_pandas(orders_df)
    pq.write_table(orders_table, os.path.join(settings.DATA_DIR, 'orders.parquet'))
    
    print("Sample data generated.")


    df1 = pd.DataFrame({'id': [1, 2, 3], 'product': ['A', 'B', 'A'], 'sales': [100, 150, 200]})
    df2 = pd.DataFrame({'id': [4, 5, 6], 'product': ['B', 'C', 'A'], 'sales': [250, 300, 120]})
    pq.write_table(pa.Table.from_pandas(df1), os.path.join(settings.DATA_DIR, 'data_part_1.parquet'))
    pq.write_table(pa.Table.from_pandas(df2), os.path.join(settings.DATA_DIR, 'data_part_2.parquet'))
    print("Sample data generated.")

    # 1. Users Table
    users_df = pd.DataFrame({
        'user_id': [101, 102, 103, 104, 105, 106],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
        'city': ['New York', 'Los Angeles', 'New York', 'Chicago', 'Los Angeles', 'Chicago'],
        'join_date': pd.to_datetime(['2024-01-15', '2024-02-20', '2024-03-10', '2024-04-05', '2024-05-12', '2024-06-18'])
    })
    pq.write_table(pa.Table.from_pandas(users_df), os.path.join(settings.DATA_DIR, 'users_nw.parquet'))

    # 2. Products Table
    products_df = pd.DataFrame({
        'product_id': [1, 2, 3, 4, 5],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Webcam'],
        'category': ['Electronics', 'Accessories', 'Accessories', 'Electronics', 'Accessories'],
        'unit_price': [1200.00, 25.00, 75.00, 300.00, 50.00]
    })
    pq.write_table(pa.Table.from_pandas(products_df), os.path.join(settings.DATA_DIR, 'products_nw.parquet'))

    # 3. Orders Table (More detailed)
    # Create a larger and more varied set of orders
    order_data = {
        'order_id': range(1, 16),
        'user_id': [101, 102, 101, 103, 104, 102, 105, 101, 103, 106, 102, 104, 105, 103, 101],
        'product_id': [1, 3, 2, 4, 1, 5, 2, 4, 3, 1, 2, 2, 5, 1, 5],
        'quantity': [1, 2, 1, 1, 1, 3, 2, 1, 1, 1, 1, 1, 2, 1, 4],
        'order_date': [datetime(2025, 1, 20), datetime(2025, 1, 22), datetime(2025, 2, 5),
                       datetime(2025, 2, 10), datetime(2025, 3, 1), datetime(2025, 3, 12),
                       datetime(2025, 3, 15), datetime(2025, 4, 2), datetime(2025, 4, 8),
                       datetime(2025, 4, 20), datetime(2025, 5, 5), datetime(2025, 5, 15),
                       datetime(2025, 6, 1), datetime(2025, 6, 10), datetime(2025, 6, 25)]
    }
    orders_df = pd.DataFrame(order_data)

    # Join with products to calculate the total amount for each order
    orders_df = pd.merge(orders_df, products_df[['product_id', 'unit_price']], on='product_id')
    orders_df['amount'] = orders_df['quantity'] * orders_df['unit_price']
    orders_df.drop(columns=['unit_price'], inplace=True) # Clean up the final table

    pq.write_table(pa.Table.from_pandas(orders_df), os.path.join(settings.DATA_DIR, 'orders_nw.parquet'))


    print("Sample data for users and orders generated.")