from sqlalchemy import create_engine
import data_extension.config as cfg
import re

sql_name = "postgres"
sql_password = "peter"
sql_dbname = "postgres"


class LSHE:
    conn_string = f"postgresql://{sql_name}:{sql_password}@localhost:5433/{sql_dbname}"

    # q_col is a pandas series
    # q_col_id is the id of this series
    def query(self, q_col, q_col_id):
        similar_tables_ls = []

        # store the column into postgres
        engine = create_engine(self.conn_string)
        q_col.rename('domain').to_sql(q_col_id, con=engine, schema='q_table')

        with engine.connect() as connection:
            num_hash = 256
            max_k = 4
            max_l = int(num_hash / max_k)
            t = 0.7

            # construct the signature matrix + hash table from the column
            exec_string = f"SELECT q_construct_sig('{q_col_id}', {num_hash});"
            exec_string += f"SELECT q_construct_hash('{q_col_id}'::varchar, {max_k}, {max_l});"

            # query
            exec_string += f"SELECT DISTINCT UNNEST(q_query_lshe({max_k}, {num_hash}, {t}, '{q_col_id}'));"
            similar_tables_raw = connection.execute(exec_string)

            for row in similar_tables_raw:
                schema = re.search('(?<=schema: ).+(?= table_name)', row[0])
                table = re.search('(?<=table_name: ).+(?= col_name)', row[0])
                col = re.search('(?<=col_name: ).+', row[0])
                similar_tables_ls.append({'schema': schema.group(0), 'table': table.group(0), 'column': col.group(0)})

            connection.close()

        return similar_tables_ls

