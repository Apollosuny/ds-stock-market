import os

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

# Đọc các giá trị từ biến môi trường
POSTGRES_CONN_STRING = os.getenv("DATABASE_URL")
POSTGRES_SCHEMA = os.getenv("POSTGRESQL_SCHEMA_NAME")


def build_shipping_data(logger, POSTGRES_CONN_STRING):
    logger.info("Merging shipping data from CSV into PostgreSQL...")

    # Đọc dữ liệu từ file CSV vào DataFrame
    source_file_path = "/app/data/amazon-sale-report.csv"
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
    required_columns = [
        "Order ID",
        "ship-service-level",
        "Courier Status",
        "ship-city",
        "ship-state",
        "ship-postal-code",
        "ship-country",
    ]
    missing_columns = [
        col for col in required_columns if col not in df.columns
    ]
    if missing_columns:
        logger.error(
            f"The CSV file is missing required columns: {missing_columns}"
        )
        return

    # Lọc và giữ lại chỉ các cột cần thiết
    df = df[required_columns]

    # Đổi tên các cột để khớp với bảng `shipping`
    df.rename(
        columns={
            "Order ID": "orderId",
            "ship-service-level": "shipServiceLevel",
            "Courier Status": "courierStatus",
            "ship-city": "city",
            "ship-state": "state",
            "ship-postal-code": "postalCode",
            "ship-country": "country",
        },
        inplace=True,
    )

    # Thêm các trường thời gian
    df["create_at"] = pd.to_datetime("today")
    df["update_at"] = pd.to_datetime("today")

    # Tên bảng tạm
    temp_table_id = "pg_temp.temp_shipping"

    # Kết nối và xử lý PostgreSQL
    engine = create_engine(POSTGRES_CONN_STRING)
    try:
        with engine.connect() as conn:
            logger.info("PostgreSQL connection established.")

            # Tạo bảng tạm
            create_temp_table_query = f"""
                CREATE TEMP TABLE IF NOT EXISTS {temp_table_id} (
                    orderId VARCHAR,
                    shipServiceLevel VARCHAR,
                    courierStatus VARCHAR,
                    city VARCHAR,
                    state VARCHAR,
                    postalCode VARCHAR,
                    country VARCHAR,
                    create_at TIMESTAMP,
                    update_at TIMESTAMP
                )
                """
            conn.execute(create_temp_table_query)
            logger.info(
                f"Temporary table {temp_table_id} created successfully."
            )

            # Chèn dữ liệu từ DataFrame vào bảng tạm
            data = list(df.itertuples(index=False, name=None))
            insert_query = f"""
            INSERT INTO {temp_table_id} (orderId, shipServiceLevel, courierStatus, city, state, postalCode, country, create_at, update_at)
            VALUES %s
            """
            psycopg2.extras.execute_values(
                conn.connection.cursor(), insert_query, data
            )
            logger.info(f"Inserted {len(data)} rows into {temp_table_id}.")

            # Kiểm tra dữ liệu trong bảng tạm
            check_query = f"SELECT COUNT(*) FROM {temp_table_id}"
            result = conn.execute(check_query).fetchone()
            logger.info(f"Temporary table contains {result[0]} rows.")

            # Chèn dữ liệu từ bảng tạm vào bảng đích
            dest_table_id = f"{POSTGRES_SCHEMA}.shipping"

            merge_query = f"""
            INSERT INTO {dest_table_id} ("orderId", "shipServiceLevel", "courierStatus", city, state, "postalCode", country, "createdAt", "updatedAt")
            SELECT s.orderId, s.shipServiceLevel, s.courierStatus, s.city, s.state, s.postalCode, s.country, s.create_at, s.update_at
            FROM {temp_table_id} s
            """
            try:
                conn.execute(merge_query)
                logger.info(f"Data merged into {dest_table_id} successfully.")
            except SQLAlchemyError as e:
                logger.error(
                    f"Error executing merge query: {e}", exc_info=True
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error during merge: {e}", exc_info=True
                )
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("PostgreSQL connection closed.")
