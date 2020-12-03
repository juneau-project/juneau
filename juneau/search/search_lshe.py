from juneau.config import config
import re
import copy


class LSHE:
    def __init__(self, psql_engine):
        self.psql_engine = psql_engine

    # query_col assumes that the domain to look at is already indexed into Postgres
    # schema, table, col specify the domain to query
    # q_sig is the schema where the signature vector of the query domain should be stored
    # q_hash is the schema where the hash table of the query domain should be stored
    # corpus_hash is the schema where the corpus hash tables are stored
    # TODO: ADD A BOOLEAN TO CONTROL WHETHER CREATE SIG OR NOT??
    def query_col(self, schema, table, col, num_hash=256, max_k=4, max_l=64, t=0.7, q_sig=config.lshe.q_sig,
                  q_hash=config.lshe.q_hash, corpus_hash=config.lshe.corpus_hash):
        similar_tables_ls = []

        with self.psql_engine.begin() as connection:
            # construct the signature vector
            # exec_string = f'CREATE TABLE test_json_sig.hello ("hello" varchar);'
            exec_string = f"SELECT q_construct_sig('{schema}', '{table}', '{col}', {num_hash}, '{q_sig}');"
            # construct the hash table
            exec_string += f"SELECT q_construct_hash('{schema}', '{table}', '{col}', {max_k}, {max_l}, '{q_sig}', '{q_hash}');"

            # query
            exec_string += f"SELECT DISTINCT UNNEST(q_query_lshe({max_k}, {num_hash}, {t}, '{schema}', '{table}', '{col}', '{corpus_hash}', '{q_hash}'));"
            similar_tables_raw = connection.execute(exec_string)

            for row in similar_tables_raw:
                schema = re.search('(?<=schema: ).+(?= table_name)', row[0])
                table = re.search('(?<=table_name: ).+(?= col_name)', row[0])
                col = re.search('(?<=col_name: ).+', row[0])
                similar_tables_ls.append({'schema': schema.group(0), 'table': table.group(0), 'column': col.group(0)})

        return similar_tables_ls

    # wrapper built on query_col to perform LSHE over a list of domains
    def query_cols(self, q_col_list):
        similar_tables_ls = []
        for q_col in q_col_list:
            similar_domain_ls = self.query_col(q_col.get('schema'), q_col.get('table'), q_col.get('col'))
            q_col_with_result = copy.deepcopy(q_col)
            q_col_with_result['similar_domains'] = similar_domain_ls
            similar_tables_ls.append(q_col_with_result)
        return similar_tables_ls


# conn_string = f"postgresql://{cfg.sql_username}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
# engine = create_engine(conn_string)
# lshe = LSHE(engine)
# print(lshe.query_cols([{'schema': 'test_json', 'table': 'hours', 'col': 'Monday'}]))


