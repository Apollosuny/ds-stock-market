import logging
import os
from dotenv import load_dotenv
from plugins.transform_erd import*

# Tải các biến môi trường từ tệp .env
load_dotenv()

# Đọc các giá trị từ biến môi trường
POSTGRES_CONN_STRING = os.getenv("DATABASE_URL")
POSTGRES_SCHEMA = os.getenv("POSTGRESQL_SCHEMA_NAME")

class MERGE_ERD:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def transform(self, last_timestamp=None):
        if last_timestamp:
            self.logger.info(f"Processing data from last timestamp: {last_timestamp}")
        else:
            self.logger.info("Processing all data from the beginning.")
        
        self.logger.info("Performing dimension transformations.")
        
        # Gọi hàm build_product_data từ plugins.transform_erd
        build_product_data(self.logger, POSTGRES_CONN_STRING)
        build_customer_data(self.logger, POSTGRES_CONN_STRING)
        build_order_data(self.logger, POSTGRES_CONN_STRING)
        build_shipping_data(self.logger, POSTGRES_CONN_STRING)
        build_orderdetail_data(self.logger, POSTGRES_CONN_STRING)
