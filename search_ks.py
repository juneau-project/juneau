from sqlalchemy import create_engine
import re
import copy
import data_extension.config as cfg


class KS:
    def __init__(self, psql_engine):
        self.psql_engine = psql_engine

    # query_col assumes that the column to query is already indexed into Postgres
    # schema, table, col specify the column to query
    # corpus is the name of the schema to query over
    # t is the threshold value
    def query_col(self, schema, table, col, corpus=cfg.corpus, t=0.7):
        similar_tables_ls = []

        with self.psql_engine.begin() as connection:
            exec_string = f"SELECT DISTINCT UNNEST(q_query_ks('{schema}', '{table}', '{col}', '{corpus}', {t}));"
            similar_tables_raw = connection.execute(exec_string)
            for row in similar_tables_raw:
                schema = re.search('(?<=schema: ).+(?= table_name)', row[0])
                table = re.search('(?<=table_name: ).+(?= col_name)', row[0])
                col = re.search('(?<=col_name: ).+', row[0])
                similar_tables_ls.append({'schema': schema.group(0), 'table': table.group(0), 'column': col.group(0)})

        return similar_tables_ls

    def query_cols(self, q_col_list):
        similar_tables_ls = []
        for q_col in q_col_list:
            similar_domain_ls = self.query_col(q_col.get('schema'), q_col.get('table'), q_col.get('col'))
            q_col_with_result = copy.deepcopy(q_col)
            q_col_with_result['similar_domains'] = similar_domain_ls
            similar_tables_ls.append(q_col_with_result)
        return similar_tables_ls


# conn_string = f"postgresql://{cfg.sql_name}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
# engine = create_engine(conn_string)
# lshe = KS(engine)
# print(lshe.query_cols([{'schema': 'utils', 'table': 'count_table', 'col': 'row_count'}]))
