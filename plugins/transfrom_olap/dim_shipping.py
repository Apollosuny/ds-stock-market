import logging
import psycopg2
from sqlalchemy import create_engine, text, func
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()

# Đọc các giá trị từ biến môi trường
POSTGRES_CONN_STRING = os.getenv("DATABASE_URL")
POSTGRES_SCHEMA = os.getenv("POSTGRESQL_SCHEMA_NAME")

def build_dim_shipping(logger, POSTGRES_CONN_STRING):
    logger.info('Merging shipping data into PostgreSQL dim_product...')

    source = f"{POSTGRES_SCHEMA}.shipping"
    desk = f"{POSTGRES_SCHEMA}.dim_shipping"

    # Kết nối PostgreSQL
    engine = create_engine(POSTGRES_CONN_STRING)
    try:
        with engine.connect() as conn:
            logger.info("PostgreSQL connection established.")

            merge_query = f"""
                INSERT INTO {desk} ("shippingId", "ServiceLevel", "courierStatus", shipping_city, shipping_country, "createdAt", "updatedAt")
                SELECT 
                    s."shippingId",
                    s."shipServiceLevel" AS "ServiceLevel",
                    s."courierStatus",
                    s.city AS shipping_city,
                    s.country AS shipping_country,
                    s."createdAt",
                    s."updatedAt"
                FROM {source} s
                ON CONFLICT ("shippingId")
                DO UPDATE SET
                    "ServiceLevel" = EXCLUDED."ServiceLevel",
                    "courierStatus" = EXCLUDED."courierStatus",
                    shipping_city = EXCLUDED.shipping_city,
                    shipping_country = EXCLUDED.shipping_country,
                    "createdAt" = EXCLUDED."createdAt",
                    "updatedAt" = EXCLUDED."updatedAt";
                """

            conn.execute(text(merge_query))
            logger.info("Data merged into dim_shipping successfully.")

    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("PostgreSQL connection closed.")
