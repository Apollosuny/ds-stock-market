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


def build_fact_order(logger, POSTGRES_CONN_STRING):
    logger.info(
        "Merging order data into PostgreSQL fct_order using SCD Type 1 with LEFT JOIN..."
    )

    # Define schema
    desk = f"{POSTGRES_SCHEMA}.fct_order"
    source_order = f"{POSTGRES_SCHEMA}.order"
    source_order_detail = f"{POSTGRES_SCHEMA}.order_detail"
    source_product = f"{POSTGRES_SCHEMA}.product"
    source_customer = f"{POSTGRES_SCHEMA}.customer"
    source_shipping = f"{POSTGRES_SCHEMA}.shipping"
    source_date = f"{POSTGRES_SCHEMA}.dim_date"

    # Connect to PostgreSQL
    engine = create_engine(POSTGRES_CONN_STRING)
    try:
        with engine.connect() as conn:
            logger.info("PostgreSQL connection established.")

            # Query for SCD Type 1 (Overwrite)
            merge_query = f"""
            INSERT INTO {desk} (
                "orderId", "promotionIds", "productId", "customerId", "shippingId", "dateId", quantity, amount, "createdAt", "updatedAt"
            )
            SELECT 
                o."orderId",
                o."promotionIds",
                p.sku AS "productId",
                c."customerId",
                s."shippingId",
                d."dateId",
                od.quantity,
                od.amount,
                o."createdAt",
                o."updatedAt"
            FROM {source_order} o
            LEFT JOIN {source_order_detail} od ON o."orderId" = od."orderId"
            LEFT JOIN {source_product} p ON od."productId" = p.sku
            LEFT JOIN {source_customer} c ON o."customerId" = c."customerId"
            LEFT JOIN {source_shipping} s ON o."orderId" = s."orderId"
            LEFT JOIN {source_date} d ON DATE(o."date") = d."date";

            """

            # Execute the query
            conn.execute(text(merge_query))
            logger.info("Data merged into fct_order successfully using SCD Type 1.")

    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("PostgreSQL connection closed.")
