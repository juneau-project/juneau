from sqlalchemy import create_engine
import data_extension.config as cfg
from psycopg2 import connect


class SqlUtils:

    def __init__(self, psql_engine):
        self.psql_engine = psql_engine

    def join_score(self, _sch1, _tbl1, _col1, _sch2, _tbl2, _col2):
        exec_string = f"select utils.join_score('{_sch1}', '{_tbl1}', '{_col1}', '{_sch2}', '{_tbl2}', '{_col2}');"
        raw_results = self.psql_engine.execute(exec_string)

        for row in raw_results:
            score = row[0]

        return score

    def jaccard(self, _sch1, _tbl1, _col1, _sch2, _tbl2, _col2):
        exec_string = f'SELECT COUNT(*) FROM (select DISTINCT "{_col1}" FROM {_sch1}."{_tbl1}" WHERE "{_col1}" IS NOT NULL INTERSECT SELECT DISTINCT "{_col2}" FROM {_sch2}."{_tbl2}" WHERE "{_col2}" IS NOT NULL) r0;'

        raw_results = self.psql_engine.execute(exec_string)

        for row in raw_results:
            intersect_count = row[0]

        exec_string = f'SELECT COUNT(*) FROM (select DISTINCT "{_col1}" FROM {_sch1}."{_tbl1}" WHERE "{_col1}" IS NOT NULL UNION SELECT DISTINCT "{_col2}" FROM {_sch2}."{_tbl2}" WHERE "{_col2}" IS NOT NULL) r0;'

        raw_results = self.psql_engine.execute(exec_string)

        for row in raw_results:
            union_count = row[0]

        return float(intersect_count) / union_count

    def set_diff(self,  _sch1, _tbl1, _col1, _sch2, _tbl2, _col2):
        exec_string = f'SELECT DISTINCT "{_col1}" FROM {_sch1}."{_tbl1}" WHERE "{_col1}" IS NOT NULL EXCEPT (SELECT DISTINCT "{_col2}" FROM {_sch2}."{_tbl2}" WHERE "{_col2}" IS NOT NULL);'

        result = []

        raw_results = self.psql_engine.execute(exec_string)

        for row in raw_results:
            result.append(row[0])

        return result

    def col_unique(self, _sch, _tbl, _col):
        exec_string = f'SELECT DISTINCT("{_col}") FROM {_sch}."{_tbl}" WHERE "{_col}" IS NOT NULL;'

        raw_results = self.psql_engine.execute(exec_string)

        result = []

        for row in raw_results:
            result.append(row[0])

        return result

    def row_count(self, _sch, _tbl):
        exec_string = f'SELECT COUNT(*) FROM {_sch}."{_tbl}";'

        raw_results = self.psql_engine.execute(exec_string)

        result = []
        for row in raw_results:
            result.append(row[0])

        return result[0]

class SqlUtils2:

    def __init__(self, db_cursor = None):
        self.config = {
            "host":cfg.sql_host,
            "dbname": cfg.sql_dbname,
            "user": cfg.sql_name,
            "password": cfg.sql_password,
            "port": cfg.sql_port
        }
        self.db_cursor = db_cursor

    def col_unique(self, _sch, _tbl, _col):


        exec_string = f'SELECT DISTINCT("{_col}") FROM {_sch}."{_tbl}" WHERE "{_col}" IS NOT NULL;'

        if self.db_cursor:
            self.db_cursor.execute(exec_string)
            raw_results = self.db_cursor.fetchall()

        else:
            with connect(**self.config) as conn:
                with conn.cursor() as cur:
                    cur.execute(exec_string)
                    raw_results = cur.fetchall()

        result = []

        for row in raw_results:
            result.append(row[0])

        return result

    def col_count(self, _sch, _tbl):

        exec_string = f"SELECT COUNT(*) FROM information_schema.columns where table_schema = 'rowstore' and table_name = '{_tbl}';"

        if self.db_cursor:
            self.db_cursor.execute(exec_string)
            raw_results = self.db_cursor.fetchall()
        else:
            with connect(**self.config) as conn:
                with conn.cursor() as cur:
                    cur.execute(exec_string)
                    raw_results = cur.fetchall()

        result = []
        for row in raw_results:
            result.append(row[0])

        return result[0]

#conn_string = f"postgresql://{cfg.sql_username}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
#engine = create_engine(conn_string)
#su = SqlUtils(engine)

# print(su.join_score('test', 'table1', 's', 'test', 'table2', 'n'))
#print(su.jaccard('test', 'table1', 's', 'test', 'table2', 's'))
#print(su.set_diff('test', 'table1', 's', 'test', 'table2', 's'))
#print(su.col_unique('test', 'table1', 's'))

