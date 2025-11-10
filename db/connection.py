import pyodbc

class AzureSQLDatabase:
    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        driver: str = "{ODBC Driver 17 for SQL Server}",
        encrypt: str = "yes",
        trust_server_certificate: str = "no",
        timeout: int = 30
    ):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate
        self.timeout = timeout

    def connect(self):
        conn_str = (
            f"DRIVER={self.driver};"
            f"SERVER={self.server},1433;"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"Encrypt={self.encrypt};"
            f"TrustServerCertificate={self.trust_server_certificate};"
            f"Connection Timeout={self.timeout};"
        )

        try:
            conn = pyodbc.connect(conn_str)
            return conn
        except pyodbc.Error as e:
            print(e)
            raise