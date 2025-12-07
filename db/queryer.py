import pandas as pd

class DataQueryer:
    def __init__(self, connection):
        self.conn = connection

    def query_dataframe(self, cols, table_name, filters=[]):
        col_string = ", ".join(cols)

        where_clause = " AND ".join(filters) if filters else "1=1"

        query = f"""
            SELECT {col_string}
            FROM {table_name}
            WHERE {where_clause}
        """
        df = pd.read_sql(query, self.conn)
        
        return df