from sqlalchemy import create_engine
import urllib.parse
import time

class AzureSQLDatabase:
    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        driver: str = "{ODBC Driver 18 for SQL Server}",
        encrypt: str = "yes",
        trust_server_certificate: str = "no",
        timeout: int = 120,
    ):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate
        self.timeout = timeout

    def connect(self, retries: int = 3, delay: int = 20):

        params = urllib.parse.quote_plus(
            f"DRIVER={self.driver};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"Encrypt={self.encrypt};"
            f"TrustServerCertificate={self.trust_server_certificate};"
            f"Connection Timeout={self.timeout};"
        )

        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={params}",
            pool_pre_ping=True,
        )

        for i in range(retries):
            try:
                with engine.connect():
                    return engine
            except Exception:
                if i == retries - 1:
                    raise
                time.sleep(delay)