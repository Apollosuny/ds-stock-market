import logging
import os
from dotenv import load_dotenv
from plugins.transfrom_olap import*

# Tải các biến môi trường từ tệp .env
load_dotenv()

# Đọc các giá trị từ biến môi trường
POSTGRES_CONN_STRING = os.getenv("DATABASE_URL")
POSTGRES_SCHEMA = os.getenv("POSTGRESQL_SCHEMA_NAME")

class MERGE_OLAP:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def transform(self, last_timestamp=None):
        if last_timestamp:
            self.logger.info(f"Processing data from last timestamp: {last_timestamp}")
        else:
            self.logger.info("Processing all data from the beginning.")
        
        self.logger.info("Performing dimension transformations.")
        
        # dim table
        # build_dim_date(self.logger, POSTGRES_CONN_STRING)
        # build_dim_customer(self.logger, POSTGRES_CONN_STRING)
        # build_dim_product(self.logger, POSTGRES_CONN_STRING)
        # build_dim_shipping(self.logger, POSTGRES_CONN_STRING)


        #fact table
        build_fact_order(self.logger, POSTGRES_CONN_STRING)

