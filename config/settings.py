import os
from dotenv import load_dotenv

load_dotenv()

POSTGRES_CONN_STRING = os.environ["DATABASE_URL"]
