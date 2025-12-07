# config.py
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")

DB_CONFIG = {
    "server": os.getenv("AZURE_SQL_SERVER"),
    "database": os.getenv("AZURE_SQL_DATABASE"),
    "schema": os.getenv("AZURE_SQL_SCHEMA"),
    "username": os.getenv("AZURE_SQL_USER"),
    "password": os.getenv("AZURE_SQL_PASSWORD")
}

SMTP_CONFIG = {
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "username": os.getenv("SMTP_USER"),
    "password": os.getenv("SMTP_PASSWORD")
}
