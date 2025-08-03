# config/settings.py

import os
from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
    "hospital_a": {
        "host": os.getenv("MYSQL_HOST_A"),
        "user": os.getenv("MYSQL_USER_A"),
        "password": os.getenv("MYSQL_PASS_A"),
        "database": os.getenv("MYSQL_DB_A")
    },
    "hospital_b": {
        "host": os.getenv("MYSQL_HOST_B"),
        "user": os.getenv("MYSQL_USER_B"),
        "password": os.getenv("MYSQL_PASS_B"),
        "database": os.getenv("MYSQL_DB_B")
    }
}

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
