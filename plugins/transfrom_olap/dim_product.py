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

def build_dim_product(logger, POSTGRES_CONN_STRING):
    logger.info('Merging product data into PostgreSQL dim_product...')

    source = f"{POSTGRES_SCHEMA}.product"
    desk = f"{POSTGRES_SCHEMA}.dim_product"

    # Kết nối PostgreSQL
    engine = create_engine(POSTGRES_CONN_STRING)
    try:
        with engine.connect() as conn:
            logger.info("PostgreSQL connection established.")

            merge_query = f"""
                INSERT INTO {desk} ("productId", sku, style, category, size, asin, "createdAt", "updatedAt")
                SELECT 
                    p."productId",
                    p.sku,
                    p.style,
                    p.category,
                    p.size,
                    p.asin,
                    p."createdAt",
                    p."updatedAt"
                FROM {source} p
                ON CONFLICT (sku)
                DO UPDATE SET
                    "productId" = EXCLUDED."productId",
                    style = EXCLUDED.style,
                    category = EXCLUDED.category,
                    size = EXCLUDED.size,
                    asin = EXCLUDED.asin,
                    "createdAt" = EXCLUDED."createdAt",
                    "updatedAt" = EXCLUDED."updatedAt";
                """
            conn.execute(text(merge_query))
            logger.info("Data merged into dim_product successfully.")

    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("PostgreSQL connection closed.")
