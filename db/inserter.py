import math

class DataInserter:
    def __init__(self, connection):
        self.conn = connection

    def insert_dataframe(self, df, table_name):
            cursor = self.conn.cursor()
            cols = df.columns.tolist()
            placeholders = ', '.join(['?']*len(cols))
            query = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"

            for _, row in df.iterrows():
                values = tuple(None if (isinstance(x, float) and math.isnan(x)) else x for x in row)
                cursor.execute(query, values)

            self.conn.commit()
            cursor.close()