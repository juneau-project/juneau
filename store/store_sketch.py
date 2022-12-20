import pandas as pd
from sqlalchemy import create_engine
import data_extension.config as cfg
import sys
import logging


"""
- UNIQUE for profile is handled in python by DAGE using SET
* Peter's SQL assumes all profiles input strings are unique-ed already

store + query profile for sketches
"""


class StoreSketch:
    # psql_engine: an engine from sqlalchemy (refer to example below to see how this is set up)
    def __init__(self, psql_engine, num_digits=0):
        self.num_hash = 128
        self.num_part = 20
        self.psql_engine = psql_engine
        self.num_digits = num_digits

    # profiles: [[profile_id, vals, length, _type]]
    def store_profile_sketches(self, profiles):
        exec_string = ''

        partition = False

        for profile in profiles:
            _id, vals, length, _type = profile

            if _type == 'str':
                partition = True
                vals = vals.replace("'", "''")
                exec_string += f'INSERT INTO sketch.profile_hash_table VALUES (\'{_id}\', {length}, sketch.hash(\'{vals}\', {self.num_hash}), NULL) ON CONFLICT ("key") DO UPDATE SET size = {length}, "hashKey" = sketch.hash(\'{vals}\', {self.num_hash});'
            elif _type == 'int' or _type == 'float':
                vals = vals.replace("'", "''")

                if _type == 'int':
                    type_name = 'bigint'
                else:
                    type_name = 'double precision'

                exec_string += f'INSERT INTO sketch.profile_hist_{_type}_table VALUES (\'{_id}\', sketch.hist_{_type}(\'{{{vals}}}\'::{type_name}[], {self.num_digits})) ON CONFLICT ("key") DO UPDATE SET hist = sketch.hist_{_type}(\'{{{vals}}}\'::{type_name}[], {self.num_digits});'

        if partition:
            exec_string += f"select sketch.profile_partition({self.num_part});"

        self.psql_engine.execute(exec_string)

    def query_cols(self, table_df, threshold1, threshold2):

        results = []
        table_df = table_df.dropna()

        for col in table_df:

            if type(col) != str:
                continue

            if col.startswith('#self') or col.startswith('#par'):
                continue

            if table_df[col].dtype == object:
                try:
                    col_list = list(set(table_df[col].dropna().tolist()))
                    col_list = [t for t in col_list if type(t) is str]
                except:
                    continue

                if len(col_list) == 0:
                    continue

                try:
                    s = '#sep#'.join(col_list).replace("'", "''").replace('%', '%%')
                except:
                    logging.info(sys.exc_info())
                    logging.info("here1")
                    logging.info(col_list)
                    continue

                match_ids = []

                raw_result = self.psql_engine.execute(f"select sketch.query_lshe_profile('{s}', 4, 32, {len(col_list)}, {threshold1});")

                for row in raw_result:
                    match_ids += row[0]

                if len(match_ids) > 0:
                    results.append((col, match_ids))

            elif table_df[col].dtype == int or table_df[col].dtype == float:

                col_list = list(set(table_df[col].dropna().tolist()))

                if len(col_list) == 0:
                    continue

                col_list = [str(val) for val in col_list]

                try:
                    s = ','.join(col_list).replace("'", "''").replace('%', '%%')
                except:
                    logging.info(sys.exc_info())
                    logging.info('here2')
                    logging.info(col_list)
                    continue

                if table_df[col].dtype == int:
                    type_name = 'bigint'
                    _type = 'int'
                else:
                    type_name = 'double precision'
                    _type = 'float'

                match_ids = []

                raw_result = self.psql_engine.execute(
                    f"select sketch.query_ks_profile_{_type}(\'{{{s}}}\'::{type_name}[], {self.num_digits}, {threshold2});")

                for row in raw_result:
                    match_ids += row[0]

                if len(match_ids) > 0:
                    results.append((col, match_ids))

        return results


# conn_string = f"postgresql://{cfg.sql_username}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
# engine = create_engine(conn_string)
# ss = StoreSketch(engine)

# _id, vals, length, _type
# profile1 = ['p1', "def#sep#def#sep#def", 3, 'str']
# profile2 = ['p1', "male#sep#male#sep#female#sep#female", 4, 'str']

# profile1 = ['p1', '1,2,3,4,5', 5, 'int']
# profile2 = ['p2', '1,2,3,4,5,5', 6, 'int']

# profile1 = ['p1', '1.1,2.1,3.1,4.1,5.1', 5, 'float']
# profile2 = ['p1', '1.1,2.1,3.1,4.1,5.1,7.1', 6, 'float']

# ss.store_profile_sketches([profile2])


# with engine.begin() as connection:
#     # s = 'male#sep#male#sep#female#sep#female'
#     # raw_results = connection.execute(f"select sketch.query_lshe_profile('{s}', 4, 32, 3, {0.9});")
#
#     # _type = 'int'
#     # s = '10, 11, 12'
#     # type_name = 'bigint'
#
#     # _type = 'float'
#     # s = '1.1,2.1,3.1,4.1,5.1,7.1'
#     # type_name = 'float'
#
#     raw_results = connection.execute(f"select sketch.query_ks_profile_{_type}(\'{{{s}}}\'::{type_name}[], 0, 0.7);")
#
#     for row in raw_results:
#         print(row[0])