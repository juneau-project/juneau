import logging
import sys
from sqlalchemy import create_engine
import data_extension.config as cfg


class StoreKS:
    def __init__(self, psql_engine, num_digits=3):
        self.num_digits = num_digits
        self.psql_engine = psql_engine

    # profiles: [[profile_id, vals, len, _type]]
    # _type can only be int/float
    def store_profile_sketches(self, profiles):
        exec_string = ''

        for profile in profiles:
            _id, vals, length, _type = profile

            if _type != 'int' and _type != 'float':
                continue

            vals = vals.replace("'", "''")

            if _type == 'int':
                _type_num = 0
            else:
                _type_num = 1

            exec_string += f'INSERT INTO sketch.profile_hist_{_type}_table VALUES (\'{_id}\', sketch.hist(\'{vals}\', {length}, {self.num_digits}, {_type_num})) ON CONFLICT ("key") DO UPDATE SET hist = sketch.hist(\'{vals}\', {length}, {self.num_digits}, {_type_num});'

        self.psql_engine.execute(exec_string)

    def query_cols(self, table_df, threshold):
        results = []
        table_df = table_df.dropna()

        for col in table_df:

            if not (table_df[col].dtype == int or table_df[col].dtype == float):
                continue

            if col.startswith('#self') or col.startswith('#par'):
                continue

            col_list = list(set(table_df[col].dropna().tolist()))

            # col_list = [t for t in col_list if type(t) is str]

            if len(col_list) == 0:
                continue

            try:
                s = ','.join(col_list).replace("'", "''").replace('%', '%%')
            except:
                logging.info(sys.exc_info())
                continue

            if table_df[col].dtype == int:
                type_num = 0
            else:
                type_num = 1

            match_ids = []

            raw_result = self.psql_engine.execute(
                f"select sketch.query_ks_profile('{s}', {len(col_list)}, {threshold}, {self.num_digits}, {type_num});")

            for row in raw_result:
                match_ids += row[0]

            if len(match_ids) > 0:
                results.append((col, match_ids))

        return results


# conn_string = f"postgresql://{cfg.sql_username}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
# engine = create_engine(conn_string)
# sks = StoreKS(engine)
# sks.store_profile_sketches([[2, '2.0,3.0,4.0,5.9', 4, 'float'], [1, '4, 5, 6, 7', 4, 'int']])
