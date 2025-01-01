import logging
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()

# Đọc các giá trị từ biến môi trường
POSTGRES_CONN_STRING = os.getenv("DATABASE_URL")
POSTGRES_SCHEMA = os.getenv("POSTGRESQL_SCHEMA_NAME")

from sqlalchemy import text

def build_dim_date(logger, POSTGRES_CONN_STRING):
    logger.info('Merging order details data into PostgreSQL dim_date...')

    desk = f"{POSTGRES_SCHEMA}.dim_date"

    # Kết nối PostgreSQL
    engine = create_engine(POSTGRES_CONN_STRING)
    try:
        with engine.connect() as conn:
            logger.info("PostgreSQL connection established.")

            # Merge trực tiếp vào dim_date
            merge_query = f"""
            INSERT INTO {desk}
                WITH date_range AS (
                    SELECT DATE '2010-01-01' + sequence_index AS date_value
                    FROM generate_series(0, (DATE '2030-12-31' - DATE '2010-01-01')::INTEGER) AS sequence_index
                )
                SELECT 
                    CAST(to_char(date_value, 'YYYYMMDD') AS INTEGER) AS id,
                    EXTRACT(DOW FROM date_value) AS day_of_week,
                    EXTRACT(DAY FROM date_value) AS day_of_month,
                    EXTRACT(DOY FROM date_value) AS day_of_year,
                    CASE 
                        WHEN EXTRACT(DAY FROM date_value) = EXTRACT(DAY FROM (date_trunc('MONTH', date_value) + interval '1 MONTH - 1 day'))
                        THEN 1 ELSE 0 
                    END AS is_last_day_of_month,
                    CASE WHEN EXTRACT(DOW FROM date_value) IN (6, 7) THEN 1 ELSE 0 END AS is_weekend,
                    CAST(to_char(date_trunc('week', date_value), 'YYYYMMDD') AS INTEGER) AS week_start_id,
                    CAST(to_char(date_trunc('week', date_value) + interval '6 days', 'YYYYMMDD') AS INTEGER) AS week_end_id,
                    EXTRACT(WEEK FROM date_value) AS week_of_year,
                    CAST(to_char(date_value, 'YYYYMM') AS INTEGER) AS month,
                    CONCAT(to_char(date_value, 'YYYY'), 'Q', EXTRACT(QUARTER FROM date_value)) AS quarter,
                    EXTRACT(YEAR FROM date_value) AS year,
                    0 AS is_holiday,  -- Chỉnh sửa để thêm ngày nghỉ lễ nếu cần thiết
                    EXTRACT(EPOCH FROM date_value) AS unix_timestamp,
                    date_value
                FROM date_range;

            """
            
            # Chạy câu lệnh SQL bằng cách sử dụng `text()`
            conn.execute(text(merge_query))
            logger.info("Data merged into dim_date successfully.")

    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("PostgreSQL connection closed.")

