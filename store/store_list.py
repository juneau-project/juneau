import pandas as pd
import json
from sqlalchemy import create_engine
import data_extension.config as cfg

"""
- first store all rows of a table in a single list instead of using concatenation of DFs,
we only construct dataframe for each table once for all rows in the list
- the ids of parents beyond 1 level is no longer stored in this version
"""

# set this to be the actual schema where tables are stored
SCHEMA = cfg.sql_dbs


class StoreList:
    def __init__(self):
        self.parsed_tables = {}
        self.table_parents = {}

        # schema -> table_name
        self.table_to_schema = {}

        # table_name -> table_name that the table merges to
        self.merged_table = {}

    # parent_id = parent0_id-parent1_id-...
    # table_name = parent0_table_name-parent1_table_name-...-self_table_name
    def __parse_rec(self, json_list, table_name, parent_id=''):
        # position of the object in the json list
        self_id = 0

        if table_name not in self.parsed_tables:
            self.parsed_tables[table_name] = {'table': []}

        for obj in json_list:

            if type(obj) is not dict:
                continue

            # store the keys which values are not lists or dictionaries
            # non_array_keys = []
            non_array_obj = {}

            actual_self_id = f'{parent_id}-{self_id}' if parent_id else f'{self_id}'

            for key in obj:

                if obj.get(key) is None:
                    continue

                # ignore empty array
                if type(obj[key]) is list and len(obj[key]) == 0:
                    continue

                if key == 'uuid':
                    continue

                value = obj[key]
                value_type = type(value)

                # assume that the list is homogeneous which tends to be the case for JSON data
                # and that there is no lists within lists (not a matrix)
                if value_type is list or value_type is dict:
                    list_type = type(obj[key][0]) if value_type is list else None

                    # handle value = [{}] or {}
                    if list_type is dict or value_type is dict:
                        child_table_name = f'{table_name}-{key}'
                        self.table_parents[child_table_name] = table_name

                        child = obj[key] if value_type is list else [obj[key]]

                        self.__parse_rec(child, child_table_name, parent_id=actual_self_id)
                    # throw error if value = [[]]
                    elif list_type is list:
                        print('unable to handle [[]]')
                    # handle value = [primitive types]
                    else:
                        list_type_str = list_type.__name__

                        # only handle str, int, float
                        transpose_store = False

                        if list_type is int or list_type is float or list_type is str:
                            transpose_store = True

                        child_table_name = f'{table_name}-{list_type_str}'
                        if child_table_name not in self.parsed_tables:
                            self.parsed_tables[child_table_name] = {'table': [], 'transpose': transpose_store}
                            self.table_parents[child_table_name] = table_name

                        for e in obj[key]:
                            self.parsed_tables[child_table_name]['table'].append(
                                {'key': key, 'val': e, '#par': actual_self_id})

                    continue
                else:
                    non_array_obj[key] = obj[key]

                    if table_name not in self.table_to_schema:
                        self.table_to_schema[table_name] = set()
                    self.table_to_schema[table_name].add(f'{key}-{type(non_array_obj[key]).__name__}')

                # it might that elements in a list might be of different types
                # but most likely to be [xx, NaN, xxx] (TODO: handle)

            non_array_obj['#self'] = actual_self_id

            if parent_id:
                non_array_obj['#par'] = parent_id

            if non_array_obj:
                self.parsed_tables[table_name]['table'].append(non_array_obj)

            self_id += 1

    def __construct_join_keys(self):
        join_keys = []
        join_keys_set = set()

        for table_name in self.parsed_tables:
            if table_name not in self.table_parents:
                continue

            parent_table_name = self.table_parents[table_name]

            if table_name in self.merged_table:
                if parent_table_name in self.merged_table:
                    join_key_set = f'{SCHEMA}.{self.merged_table[parent_table_name]}-{SCHEMA}.{self.merged_table[table_name]}'
                    join_key = [f'{SCHEMA}.{self.merged_table[parent_table_name]}', f'{SCHEMA}.{self.merged_table[table_name]}', ['#self', '#par']]
                else:
                    join_key_set = f'{SCHEMA}.{parent_table_name}-{SCHEMA}.{self.merged_table[table_name]}'
                    join_key = [f'{SCHEMA}.{parent_table_name}', f'{SCHEMA}.{self.merged_table[table_name]}', ['#self', '#par']]
            else:
                if parent_table_name in self.merged_table:
                    join_key_set = f'{SCHEMA}.{self.merged_table[parent_table_name]}-{SCHEMA}.{table_name}'
                    join_key = [f'{SCHEMA}.{self.merged_table[parent_table_name]}', f'{SCHEMA}.{table_name}', ['#self', '#par']]
                else:
                    join_key_set = f'{SCHEMA}.{parent_table_name}-{SCHEMA}.{table_name}'
                    join_key = [f'{SCHEMA}.{parent_table_name}', f'{SCHEMA}.{table_name}', ['#self', '#par']]

            # eliminate duplicate
            if join_key_set not in join_keys_set:
                join_keys.append(join_key)
                join_keys_set.add(join_key_set)

        return join_keys

    def parse_tables_json_list(self, json_list, table_name):
        self.parsed_tables = {}
        self.table_parents = {}
        self.table_to_schema = {}
        self.merged_table = {}

        self.__parse_rec(json_list, table_name)

        simp = {}

        r_idx = 0
        for key in self.parsed_tables:
            # r stands for relation
            simp[key] = f'#r{r_idx}#'
            r_idx += 1

        parsed_tables_simp = {}
        table_parents_simp = {}
        table_to_schema_simp = {}

        for key in self.table_parents:
            table_parents_simp[simp[key]] = simp[self.table_parents[key]]
        self.table_parents = table_parents_simp

        for key in self.parsed_tables:
            parsed_tables_simp[simp[key]] = self.parsed_tables[key]
        self.parsed_tables = parsed_tables_simp

        for key in self.table_to_schema:
            table_to_schema_simp[simp[key]] = self.table_to_schema[key]
        self.table_to_schema = table_to_schema_simp

        # merge tables with same schemas together
        for table_name in self.table_to_schema:
            self.table_to_schema[table_name] = '-'.join(list(sorted(self.table_to_schema[table_name])))

        schema_to_table = {}

        for table_name, schema in self.table_to_schema.items():
            if schema in schema_to_table:
                self.parsed_tables[schema_to_table[schema]]['table'] += self.parsed_tables[table_name]['table']
                self.merged_table[table_name] = schema_to_table[schema]
                # del self.parsed_tables[table_name]
            else:
                schema_to_table[schema] = table_name

        join_keys = self.__construct_join_keys()

        for table_name in self.merged_table:
            del self.parsed_tables[table_name]

        # convert list of rows to dataframes
        # table_name is the key
        for table_name in self.parsed_tables:
            self.parsed_tables[table_name]['table'] = pd.json_normalize(self.parsed_tables[table_name]['table'])

        # join_string, join_keys = self.__construct_json_view()

        return self.parsed_tables, join_keys
        # return self.parsed_tables

    # def __construct_json_view(self):
    #     # simplify tables names
    #     simp = {}
    #
    #     r_idx = 0
    #     for key in self.parsed_tables:
    #         # r stands for relation
    #         simp[key] = f'r{r_idx}'
    #         r_idx += 1
    #
    #     parsed_tables_simp = {}
    #     table_parents_simp = {}
    #
    #     for key in self.table_parents:
    #         table_parents_simp[simp[key]] = simp[self.table_parents[key]]
    #     self.table_parents = table_parents_simp
    #
    #     for key in self.parsed_tables:
    #         parsed_tables_simp[simp[key]] = self.parsed_tables[key]
    #     self.parsed_tables = parsed_tables_simp
    #
    #     """
    #     - approach 1: handle the duplicate cols while constructing the view using AS to rename same cols
    #     - approach 2: handle the duplicate cols before constructing the view by doing one pass over all columns
    #     and rename cols with same names (currently using this one)
    #     * reason for approach 2: a) easier to implement b) #cols is small
    #     """
    #
    #     # col_name -> occurrence
    #     all_cols = {}
    #
    #     # handle repeated column names
    #     for key in self.parsed_tables:
    #         cols = self.parsed_tables[key]['table'].columns.values.tolist()
    #         cols_change = {}
    #
    #         for col in cols:
    #             if col == '#self' or col == '#par':
    #                 continue
    #
    #             if col in all_cols:
    #                 cols_change[col] = f'{col}{all_cols[col]}'
    #                 all_cols[col] += 1
    #             else:
    #                 all_cols[col] = 1
    #
    #         if cols_change:
    #             self.parsed_tables[key]['table'].rename(columns=cols_change, inplace=True)
    #
    #     # add join keys
    #     join_keys = []
    #
    #     tables_views = {}
    #
    #     # store the cols of the joined table (col of joined table = parent cols + child cols)
    #     tables_cols = {}
    #
    #     for table_name in reversed(list(self.parsed_tables.keys())):
    #         parent_table_name = self.table_parents.get(table_name)
    #
    #         # the root table has no parent, so return the root table
    #         if parent_table_name is None:
    #             if tables_views.get(table_name):
    #                 return tables_views[table_name], json.dumps(join_keys)
    #             else:
    #                 # handle JSONs with only 1 table
    #                 cols_string = ','.join(map(lambda x: f'"{x}"', self.parsed_tables[table_name]['table'].columns.values.tolist()))
    #                 return f'select {cols_string} from {SCHEMA}."{table_name}"', json.dumps([])
    #
    #         child_cols = tables_cols.get(table_name) if tables_cols.get(table_name) else self.parsed_tables[
    #             table_name]['table'].columns.values.tolist()
    #         parent_cols = tables_cols.get(parent_table_name) if tables_cols.get(parent_table_name) else \
    #             self.parsed_tables[parent_table_name]['table'].columns.values.tolist()
    #
    #         # remove all the keys cols from the child table
    #         child_cols_filtered = list(
    #             filter(lambda x: not ('#self' in x or '#par' in x), child_cols))
    #
    #         # update the cols of the parent/ joined table
    #         tables_cols[parent_table_name] = parent_cols + child_cols_filtered
    #
    #         child_cols_filtered = [f'"{col}"' for col in child_cols_filtered]
    #         parent_cols = [f'"{col}"' for col in parent_cols]
    #
    #         # handle empty child_cols_string
    #         if len(child_cols_filtered) == 0:
    #             child_cols_string = ''
    #         else:
    #             child_cols_string = 't1.' + ', t1.'.join(child_cols_filtered)
    #         parent_cols_string = 't0.' + ', t0.'.join(parent_cols)
    #
    #         if child_cols_string == '':
    #             cols_string = parent_cols_string
    #         else:
    #             cols_string = parent_cols_string + ', ' + child_cols_string
    #
    #         parent_view_string = tables_views.get(parent_table_name)
    #         child_view_string = tables_views.get(table_name)
    #
    #         if parent_view_string:
    #             parent_view_string = f'({parent_view_string})'
    #         else:
    #             parent_view_string = f'{SCHEMA}."{parent_table_name}"'
    #
    #         if child_view_string:
    #             child_view_string = f'({child_view_string})'
    #         else:
    #             if self.parsed_tables[table_name].get('transpose'):
    #                 cols = self.parsed_tables[table_name]['table'].columns.values.tolist()
    #                 key_col = list(filter(lambda x: 'key' in x, cols))[0]
    #                 val_col = list(filter(lambda x: 'val' in x, cols))[0]
    #                 child_view_string = (
    #                     f'(SELECT t2."#par", t2."{key_col}", STRING_AGG(t2."{val_col}"::text, \'#sep#\') AS "{val_col}" FROM {SCHEMA}."{table_name}" t2 GROUP BY t2."#par", t2."{key_col}")')
    #             else:
    #                 child_view_string = f'{SCHEMA}."{table_name}"'
    #
    #         join_string = f'SELECT {cols_string} FROM {parent_view_string} t0 LEFT JOIN {child_view_string} t1 on t0."#self"=t1."#par"'
    #         join_keys.append([f'{SCHEMA}.{parent_table_name}', f'{SCHEMA}.{table_name}', ['#self', '#par']])
    #
    #         # update the parent table to be the joined table
    #         tables_views[parent_table_name] = join_string

# test_obj = {"venue": {
#       "raw": "International Conference on Computer Graphics, Imaging and Visualisation",
#       "id": 2754059319,
#       "type": "C"
#     }}

# test_obj = [
#     {
#         "name": "peter",
#         "doc": {
#             "name": "peter",
#             "grade": ['a', 'b', 'c', 'd'],
#             "num": [1, 2, 3, 4],
#             "interest": {
#                 "age": "16"
#             }
#         }
#     },
#     {
#         "name": "dage",
#         "doc": {
#             "name": "dage",
#             "grade": ['a', 'b', 'c', 'd'],
#             "num2": [1, 2, 3, 4],
#             "interest": {
#                 "age": "27"
#             }
#         }
#     }
# ]


# test_obj = [
#     {
#         "name": "peter",
#         "age": "21",
#         "currentTradingPeriod": {
#               "pre": {
#                 "timezone": "EDT",
#                 "start": 1630310400,
#                 "end": 1630330200,
#                 "gmtoffset": -14400
#               },
#               "regular": {
#                 "timezone": "EDT",
#                 "start": 1630330200,
#                 "end": 1630353600,
#                 "gmtoffset": -14400
#               },
#               "post": {
#                 "timezone": "EDT",
#                 "start": 1630353600,
#                 "end": 1630368000,
#                 "gmtoffset": -14400
#               }
#             }
#     },
#     {
#         "name": "dage",
#         "age": "26",
#         "grade": "A",
#         "currentTradingPeriod": {
#               "pre": {
#                 "timezone": "EDT",
#                 "start": 1630310400,
#                 "end": 1630330200,
#                 "gmtoffset": -14400
#               },
#               "regular": {
#                 "timezone": "EDT",
#                 "start": 1630330200,
#                 "end": 1630353600,
#                 "gmtoffset": -14400
#               },
#               "post": {
#                 "timezone": "EDT",
#                 "start": 1630353600,
#                 "end": 1630368000,
#                 "gmtoffset": -14400
#               }
#             },
#     }
# ]


# conn_string = f"postgresql://{cfg.sql_username}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
# engine = create_engine(conn_string)

# # TODO: DAGE change directory
# with open("/Users/peterchan/Desktop/tmp/airline_info_list_8.json") as f:
#     # for line in f:
#     #     var_obj.append(json.loads(line))

#     var_obj = json.load(f)
#     # print(var_obj[1])
#     sl = StoreList()
#     # print(var_obj[0:3])
#     # parsed_tables, join_view = sl.parse_tables_json_list(test_obj, 'root')
#     # parsed_tables, join_view = sl.parse_tables_json_list([var_obj[641]], 'root')
#     parsed_tables, join_keys = sl.parse_tables_json_list(var_obj, 'root')
#     # parsed_tables, join_keys = sl.parse_tables_json_list([var_obj], 'root')

#     # parsed_tables = sl.parse_tables_json_list(var_obj[:2], 'root')
#     # parsed_tables = sl.parse_tables_json_list(test_obj, 'root')

#     for key in parsed_tables:
#         # print(parsed_tables[key])
#         # TODO: DAGE change directory
#         # parsed_tables[key]['table'].to_sql(name=f'{key}', schema='public', con=engine)
#         parsed_tables[key]['table'].to_csv(f'~/desktop/test_json/{key}.csv', index=False)
#         print(f"{key}-transpose-{parsed_tables[key].get('transpose')}")
#         # print(parsed_tables[key]['table'].columns.values.tolist())

    # print(join_view[0])
    # print(join_keys)
    # print(join_view[1])
    # print(join_view)

# graph = Graph(auth=('neo4j', 'peter'), host="localhost", port=7687, scheme="bolt")

# sl = SearchList("orders", json_list, engine, graph)
# sl.store()
