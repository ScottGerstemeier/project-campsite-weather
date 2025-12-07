from sqlalchemy import create_engine
import urllib

class AzureSQLDatabase:
    def __init__(
        self,
        server: str,
        database: str,
        schema: str,
        username: str,
        password: str,
        driver: str = "{ODBC Driver 17 for SQL Server}",
        encrypt: str = "yes",
        trust_server_certificate: str = "no",
        timeout: int = 30,
    ):
        self.server = server
        self.database = database
        self.schema = schema
        self.username = username
        self.password = password
        self.driver = driver
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate
        self.timeout = timeout

    def connect(self):
        params = urllib.parse.quote_plus(
            f"DRIVER={self.driver};"
            f"SERVER={self.server},1433;"
            f"DATABASE={self.database};"
            f"SCHEMA={self.schema};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"Encrypt={self.encrypt};"
            f"TrustServerCertificate={self.trust_server_certificate};"
            f"Connection Timeout={self.timeout};"
        )

        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        return engine