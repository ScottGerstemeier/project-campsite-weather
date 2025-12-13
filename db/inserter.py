import math
import pandas as pd

class DataInserter:
    def __init__(self, engine, schema=None):
        self.engine = engine
        self.schema = schema

    def insert_dataframe(self, df, table_name):

        # Get a raw DBAPI connection from SQLAlchemy
        with self.engine.begin() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()

            cols = df.columns.tolist()
            placeholders = ', '.join(['?']*len(cols))
            query = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"

            for _, row in df.iterrows():
                values = tuple(None if (isinstance(x, float) and math.isnan(x)) else x for x in row)
                cursor.execute(query, values)

            raw_conn.commit()
            cursor.close()