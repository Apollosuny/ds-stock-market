from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from models.models import base
import time
import logging
from config.settings import POSTGRES_CONN_STRING


class Initialize:
    def __init__(self, db_conn: str) -> None:
        self.engine = create_engine(db_conn)
        self.init_db()

    def init_db(self):
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
            base.metadata.create_all(self.engine, checkfirst=True)
        else:
            base.metadata.create_all(self.engine, checkfirst=True)
        time.sleep(1)
        logging.info("Sucessfully initialized database")


if __name__ == "__main__":
    print(POSTGRES_CONN_STRING)
    Initialize(POSTGRES_CONN_STRING)
    # Initialize("postgresql+psycopg2://admin:123456789@localhost:5432/dw-amazon-sales")
