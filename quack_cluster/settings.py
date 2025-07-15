import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')
    DATA_DIR: str = "/app/sample_data"

settings = Settings()

def generate_sample_data():
    print(f"Generating sample data in '{settings.DATA_DIR}'...")
    os.makedirs(settings.DATA_DIR, exist_ok=True)
    df1 = pd.DataFrame({'id': [1, 2, 3], 'product': ['A', 'B', 'A'], 'sales': [100, 150, 200]})
    df2 = pd.DataFrame({'id': [4, 5, 6], 'product': ['B', 'C', 'A'], 'sales': [250, 300, 120]})
    pq.write_table(pa.Table.from_pandas(df1), os.path.join(settings.DATA_DIR, 'data_part_1.parquet'))
    pq.write_table(pa.Table.from_pandas(df2), os.path.join(settings.DATA_DIR, 'data_part_2.parquet'))
    print("Sample data generated.")