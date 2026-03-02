# config.py
from dotenv import load_dotenv
import os

# Azure automatically sets WEBSITE_INSTANCE_ID.
# So this permits local testing and guards against Azure trying to call something it doesn't have.
if os.getenv("AzureWebJobsStorage") is None:
    from dotenv import load_dotenv
    load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")

DB_CONFIG = {
    "server": os.getenv("AZURE_SQL_SERVER"),
    "database": os.getenv("AZURE_SQL_DATABASE"),
    "username": os.getenv("AZURE_SQL_USER"),
    "password": os.getenv("AZURE_SQL_PASSWORD"),
    "encrypt": os.getenv("AZURE_SQL_ENCRYPT", "yes"),
    "trust_server_certificate": os.getenv("AZURE_SQL_TRUST_SERVER_CERTIFICATE", "no"),
    "timeout": int(os.getenv("AZURE_SQL_TIMEOUT", "30")),
}

DB_SCHEMA = os.getenv("AZURE_SQL_SCHEMA")

SMTP_CONFIG = {
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
    "username": os.getenv("SMTP_USER"),
    "password": os.getenv("SMTP_PASSWORD")
}
