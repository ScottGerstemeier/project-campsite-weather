import pyodbc
from dotenv import load_dotenv
import os
import pandas as pd
from db.connection import AzureSQLDatabase
from db.inserter import DataInserter
from weather.fetcher import WeatherFetcher
# from weather.parser import WeatherParser

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
SERVER = os.getenv("AZURE_SQL_SERVER")
DATABASE = os.getenv("AZURE_SQL_DATABASE")
USERNAME = os.getenv("AZURE_SQL_USER")
PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

conn = AzureSQLDatabase(server=SERVER, database=DATABASE, username=USERNAME, password=PASSWORD).connect()
inserter = DataInserter(conn)
fetcher = WeatherFetcher(API_KEY)

with AzureSQLDatabase(SERVER, DATABASE, USERNAME, PASSWORD).connect() as conn:
    df = pd.read_sql("SELECT * FROM prod.weather.places", conn)
    print(df)
