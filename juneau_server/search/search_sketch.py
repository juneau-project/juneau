from sqlalchemy import create_engine
import data_extension.config as cfg
import re
import copy
import logging
import concurrent.futures
from sqlalchemy import create_engine


class SearchSketch:
    def __init__(self, psql_engine):
        self.num_hash = 128
        self.num_part = 20
        self.k = 4
        self.l = 32
        self.psql_engine = psql_engine

    def query_col(self, schema, table, col, type, t1=0.9, t2=0.9):
        if type == 'str':
            raw_result = self.psql_engine.execute(f"select sketch.query_lshe_col('{schema}', '{table}', '{col}', {self.k}, {self.l}, {t1});")
        else:
            raw_result = []
        #else:
        #    raw_result = self.psql_engine.execute(f"select sketch.query_ks_col('{schema}', '{table}', '{col}', {t2});")

        match_ids = []

        for row in raw_result:
            match_ids += row[0]

        return match_ids

    def query_cols(self, q_col_list, t1=0.8, t2 = 0.9):

        results = []

        for q_col in q_col_list:
            #print(q_col)
            result = self.query_col(q_col.get('schema'), q_col.get('table'), q_col.get('col'), type = q_col.get('type'), t1=t1, t2 = t2)
            logging.info(result)
            results.append(result)
        logging.info(f"{len(results)} results identified in sketch")

        return results


conn_string = f"postgresql://{cfg.sql_name}:{cfg.sql_password}@{cfg.sql_host}:{cfg.sql_port}/{cfg.sql_dbname}"
engine = create_engine(conn_string)


def query_col_parallel(q_col):
    _schema = q_col.get('schema')
    _table = q_col.get('table')
    _col = q_col.get('col')
    ls = []

    with engine.connect() as conn2:
        if q_col.get('type') == 'str':
            #logging.info(f"lshe:{_table}.{_col} start:")
            result = conn2.execute(f'select sketch.query_lshe_col(\'{_schema}\', \'{_table}\', \'{_col}\', 4, 32, 0.85);')
            #print(result)
            for row in result:
                ls += row[0]
            #logging.info(f"lshe:{_table}.{_col} end with {len(ls)} results")
            #logging.info(ls[:5])
        else:
            # return [col1, col1_score, col2, col2_score, ...] (all in strings)
            #logging.info(f"ks:{_table}.{_col} start:")
            result = conn2.execute(f'select sketch.query_ks_col(\'{_schema}\', \'{_table}\', \'{_col}\', 0.9);')
            #print(result)
            for row in result:
                ls += row[0]
            #logging.info(f"ks:{_table}.{_col} end with {len(ls)} results")
            #logging.info(ls[:5])

    engine.dispose()

    # logging.info(ls)
    return ls


def query_cols_parallel(q_cols):
    ls = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=40) as executor:
        final_r_ls = executor.map(query_col_parallel, q_cols)
        for r in final_r_ls:
            ls.append(r)
    return ls



#conn_string = f"postgresql://{cfg.sql_name}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
#engine = create_engine(conn_string)
#lshe = SearchSketch(engine)
#print(lshe.query_col('rowstore', 'rtable5_t8#r1#_ninganalysisofflightsdata', 'AIRLINE','str'))