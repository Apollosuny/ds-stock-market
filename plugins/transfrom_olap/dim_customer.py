import logging
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()

# Read values from environment variables
POSTGRES_CONN_STRING = os.getenv("DATABASE_URL")
POSTGRES_SCHEMA = os.getenv("POSTGRESQL_SCHEMA_NAME")

import logging
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()

# Read values from environment variables
POSTGRES_CONN_STRING = os.getenv("DATABASE_URL")
POSTGRES_SCHEMA = os.getenv("POSTGRESQL_SCHEMA_NAME")

def build_dim_customer(logger, POSTGRES_CONN_STRING):
    logger.info('Inserting customer data into PostgreSQL dim_customer...')
    
    source = f"{POSTGRES_SCHEMA}.customer"
    desk = f"{POSTGRES_SCHEMA}.dim_customer"

    # Connect to PostgreSQL
    engine = create_engine(POSTGRES_CONN_STRING)
    try:
        with engine.connect() as conn:
            logger.info("PostgreSQL connection established.")

            # Insert data into dim_customer
            insert_query = f"""
            INSERT INTO {desk} ("customerId",name, email, phone, country, city, "createdAt", "updatedAt")
            SELECT "customerId", name, email, phone, country, city, "createdAt", "updatedAt"
            FROM {source}
            """
            
            # Execute the SQL query using text()
            conn.execute(text(insert_query))
            logger.info("Data inserted into dim_customer successfully.")

    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        engine.dispose()
        logger.info("PostgreSQL connection closed.")
