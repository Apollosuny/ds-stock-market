import logging
import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()

# Đọc các giá trị từ biến môi trường
POSTGRES_CONN_STRING = os.getenv("DATABASE_URL")
POSTGRES_SCHEMA = os.getenv("POSTGRESQL_SCHEMA_NAME")

def build_product_data(logger, POSTGRES_CONN_STRING):
    logger.info('Merging product data from CSV into PostgreSQL...')

    # Đọc dữ liệu từ file CSV vào DataFrame
    source_file_path = "/app/plugins/data/amazon-sale-report.csv"
    try:
        df = pd.read_csv(source_file_path)
        logger.info(f"File loaded successfully: {source_file_path}")
    except FileNotFoundError:
        logger.error(f"File not found: {source_file_path}")
        return
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return

    # Kiểm tra nếu file rỗng
    if df.empty:
        logger.warning("The CSV file is empty. No data to process.")
        return

    # Kiểm tra các cột bắt buộc
    required_columns = ["SKU", "Style", "Category", "Size", "ASIN"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"The CSV file is missing required columns: {missing_columns}")
        return

    # Lọc và giữ lại chỉ các cột cần thiết
    df = df[required_columns]
    
    # Đổi tên các cột để khớp với bảng `product`
    df.rename(columns={
        "SKU": "sku",            
        "Style": "style",        
        "Category": "category",  
        "Size": "size",          
        "ASIN": "asin"           
    }, inplace=True)

    # Chỉ giữ các bản ghi có SKU duy nhất
    df = df.drop_duplicates(subset=["sku"])

    # Thêm các trường thời gian
    df["create_at"] = pd.to_datetime("today")  
    df["update_at"] = pd.to_datetime("today")  

    # Tên bảng tạm
    temp_table_id = "pg_temp.temp_product"

    # Kết nối và xử lý PostgreSQL
    engine = create_engine(POSTGRES_CONN_STRING)
    try:
        with engine.connect() as conn:
            logger.info("PostgreSQL connection established.")

            # Tạo bảng tạm
            create_temp_table_query = f"""
            CREATE TEMP TABLE IF NOT EXISTS {temp_table_id} (
                sku VARCHAR,
                style VARCHAR,
                category VARCHAR,
                size VARCHAR,
                asin VARCHAR,
                create_at TIMESTAMP,
                update_at TIMESTAMP
            )
            """
            conn.execute(create_temp_table_query)
            logger.info(f"Temporary table {temp_table_id} created successfully.")

            # Chèn dữ liệu từ DataFrame vào bảng tạm
            data = list(df.itertuples(index=False, name=None))
            insert_query = f"""
            INSERT INTO {temp_table_id} (sku, style, category, size, asin, create_at, update_at)
            VALUES %s
            """
            psycopg2.extras.execute_values(conn.connection.cursor(), insert_query, data)
            logger.info(f"Inserted {len(data)} rows into {temp_table_id}.")

            # Kiểm tra dữ liệu trong bảng tạm
            check_query = f"SELECT COUNT(*) FROM {temp_table_id}"
            result = conn.execute(check_query).fetchone()
            logger.info(f"Temporary table contains {result[0]} rows.")

            # Chèn dữ liệu từ bảng tạm vào bảng đích với `SELECT DISTINCT`
            dest_table_id = f"{POSTGRES_SCHEMA}.product"
            merge_query = f"""
            INSERT INTO {dest_table_id} (sku, style, category, size, asin, "createdAt", "updatedAt")
            SELECT DISTINCT s.sku, s.style, s.category, s.size, s.asin, CURRENT_DATE, CURRENT_DATE
            FROM {temp_table_id} s
            """
            try:
                conn.execute(merge_query)
                logger.info(f"Data merged into {dest_table_id} successfully.")
            except SQLAlchemyError as e:
                logger.error(f"Error executing merge query: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Unexpected error during merge: {e}", exc_info=True)
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("PostgreSQL connection closed.")
