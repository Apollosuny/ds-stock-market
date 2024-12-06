import pandas as pd
from sqlalchemy import create_engine

from config.settings import POSTGRES_CONN_STRING

engine = create_engine(POSTGRES_CONN_STRING)


def process_data(csv_file_path, table_name, selected_columns, chunk_size=1000):
    try:
        chunks = pd.read_csv(
            csv_file_path, chunksize=chunk_size, usecols=selected_columns
        )
        for chunk in chunks:
            chunk.to_sql(
                name=table_name,
                con=engine,
                if_exists="append",
                index=False,
            )
            print(
                f"Data from {csv_file_path} successfully loaded into {table_name}."
            )
    except Exception as e:
        print(f"Error occurred: {e}")


def transform_product_data():
    process_data(
        "data/amazon-sale-report.csv",
        "products",
        ["SKU", "Style", "Category", "Size", "ASIN"],
    )
