from sqlalchemy import create_engine
import re

sql_name = "postgres"
sql_password = "peter"
sql_dbname = "postgres"


class KS:
    conn_string = f"postgresql://{sql_name}:{sql_password}@localhost:5433/{sql_dbname}"

    # t is the threshold value
    def query(self, q_col, q_col_id, t):
        similar_tables_ls = []

        engine = create_engine(self.conn_string)

        try:
            q_col.rename('domain').to_sql(q_col_id, con=engine, schema='q_table')
        except ValueError:
            pass

        with engine.connect() as connection:
            exec_string = f"SELECT DISTINCT UNNEST(q_query_ks('q_table', '{q_col_id}', {t}));"
            similar_tables_raw = connection.execute(exec_string)
            for row in similar_tables_raw:
                schema = re.search('(?<=schema: ).+(?= table_name)', row[0])
                table = re.search('(?<=table_name: ).+(?= col_name)', row[0])
                col = re.search('(?<=col_name: ).+', row[0])
                similar_tables_ls.append({'schema': schema.group(0), 'table': table.group(0), 'column': col.group(0)})

            connection.close()

        return similar_tables_ls
