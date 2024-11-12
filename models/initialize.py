from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from models.models import base
import time
import logging
from config.settings import POSTGRES_CONN_STRING


class Initialize:
    def __init__(self, db_conn: str) -> None:
        self.engine = create_engine(db_conn)

    def init_db(self):
        if not database_exists(self.engine.url):
            create_database(self.engine.url)
            base.metadata.create_all(self.engine, checkfirst=True)
            self.init_load()
        else:
            base.metadata.create_all(self.engine, checkfirst=True)
            self.init_load()

        time.sleep(1)
        logging.info("Sucessfully initialized database")


if __name__ == "__main__":
    Initialize(POSTGRES_CONN_STRING)
