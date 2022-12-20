import pandas as pd
import json
import numpy as np
import logging
import copy
from data_extension.table_db import connect2db_engine
from data_extension.util import containment_score

from data_extension.type_check_func import check_address, check_age, check_email, check_first_name, check_last_name, \
    check_gender, check_name, check_ssn

function2name = {0:"last_name", 1:"first_name", 2:"full_name", 3:"gender", 4:"age", 5:"email", 6:"ssn", 7:"address"}
function = [check_last_name, check_first_name, check_name, check_gender, check_age, check_email, check_ssn, check_address]
fun_num = len(function2name.items())

bigN = 10000
cache_col_size = 1000
cache_col_update_row_size = 10000

class Map_Profile:

    def __init__(self, dbname):

        self.dbname = dbname
        self.psql_eng = None
        self.bigtable = {} # distinct values
        self.bigtable_dtype = {} #data type
        self.bigtable_num = {} # value number
        self.bigtable_range = {} #range
        self.bigtable_cnt = {} # occurence with mapped neighbours
        self.bigtable_neighbour_cnt = {} # occurrence of neighbours
        self.bigtable_keyscore = {} # key score
        self.bigtable_table = {} # linked tables
        self.bigValue = {}
        self.cached_group_tables = {}
        self.cached_table_depend = {}
        self.cached_table_fdepen = {}
        self.cached_group_cnt = {}

        self.table_stored_str = {}

    def create_necessary_index_table(self, conn):

        try:
            query1 = "CREATE SCHEMA IF NOT EXISTS table_profile;"
            query2 = "CREATE SCHEMA IF NOT EXISTS column_index;"

            try:
                conn.execute(query1)
                conn.execute(query2)
            except:
                logging.error("Loading Index: SCHEMA table_profile ERROR!")
                logging.error("Loading Index: SCHEMA column_index ERROR!")

            query3 = "CREATE TABLE IF NOT EXISTS table_profile.col_value (col VARCHAR(100), value VARCHAR(10000000));"
            query4 = "CREATE TABLE IF NOT EXISTS table_profile.col_dtype (col VARCHAR(100), value VARCHAR(50));"
            query5 = "CREATE TABLE IF NOT EXISTS table_profile.col_num(col VARCHAR(100), value INTEGER);"
            query6 = "CREATE TABLE IF NOT EXISTS table_profile.col_cnt(col VARCHAR(100), value INTEGER);"
            query7 = "CREATE TABLE IF NOT EXISTS table_profile.col_neighbour_cnt(col VARCHAR(100), value VARCHAR(10000000));"
            query8 = "CREATE TABLE IF NOT EXISTS table_profile.col_range(col VARCHAR(100), value VARCHAR(10000000));"
            query9 = "CREATE TABLE IF NOT EXISTS table_profile.col_table(col VARCHAR(100), value VARCHAR(10000000));"
            query10 = "CREATE TABLE IF NOT EXISTS table_profile.key_dep(col VARCHAR(100), value VARCHAR(1000000));"
            query11 = "CREATE TABLE IF NOT EXISTS table_profile.fkey_dep(col VARCHAR(100), value VARCHAR(1000000))"
            query12 = "CREATE TABLE IF NOT EXISTS table_profile.index_cnt(col VARCHAR(100), value INTEGER);"

            try:
                conn.execute(query3)
                conn.execute(query4)
                conn.execute(query5)
                conn.execute(query6)
                conn.execute(query7)
                conn.execute(query8)
                conn.execute(query9)
                conn.execute(query10)
                conn.execute(query11)
                conn.execute(query12)

            except:
                logging.error("Loading Index: CREATE TABLES FAILED\n")

            return True

        except:
            logging.error("Loading Index: DATABASE CONNECTION ERROR!")
            return False

    def store_index(self, tables):
        #logging.info(tables.keys())
        for tname, table in tables.items():

            col = []
            col_value = []
            for t_key in table.keys():
                col.append(t_key)
                if table[t_key] == np.int64:
                    table[t_key] = 'int'
                elif table[t_key] == np.float64:
                    table[t_key] = 'float'
                elif table[t_key] == object:
                    table[t_key] = 'object'

                col_value.append(table[t_key])
            dict_df = {'col':col, 'value':col_value}
            df_store = pd.DataFrame.from_dict(dict_df)
            df_store.to_sql(tname, self.psql_eng, schema="table_profile", if_exists = 'replace', index=False)
        #self.psql_eng = None

    def dumps_json(self, big_dict):
        new_big_dict = {}
        for col in big_dict.keys():
            new_big_dict[col] = json.dumps(big_dict[col])

        return new_big_dict

    def deseries_json(self, big_dict):
        new_big_dict = {}
        for col in big_dict.keys():
            new_big_dict[col] = json.loads(big_dict[col])
        return new_big_dict

    def dtype2str(self, big_dict):
        new_big_dict = {}
        for col in big_dict.keys():
            if big_dict[col] == "int":
                new_big_dict[col] = np.int64
            elif big_dict[col] == "float":
                new_big_dict[col] = np.float64
            elif big_dict[col] == "object":
                new_big_dict[col] = object
        return new_big_dict

    def df2dict(self, big_df, deflg):
        new_big_df = {}
        for iter, row in big_df.iterrows():
            new_big_df[row['col']] = row['value']

        if deflg:
            new_big_df = self.deseries_json(new_big_df)

        return new_big_df

    def loading_index(self):

        if not self.psql_eng:
            self.psql_eng = connect2db_engine(self.dbname)

        conn = self.psql_eng.connect()
        self.create_necessary_index_table(conn)

        self.bigtable = pd.read_sql_table("col_value", conn , schema = "table_profile")
        self.bigtable = self.df2dict(self.bigtable, True)

        self.bigtable_dtype = pd.read_sql_table("col_dtype", conn, schema="table_profile")
        #logging.info(self.bigtable_dtype)
        self.bigtable_dtype = self.dtype2str(self.df2dict(self.bigtable_dtype, False))
        #logging.info(self.bigtable_dtype.keys())

        self.bigtable_num = pd.read_sql_table("col_num", conn, schema="table_profile")
        self.bigtable_num = self.df2dict(self.bigtable_num, False)

        self.bigtable_cnt = pd.read_sql_table("col_cnt", conn, schema = "table_profile")
        self.bigtable_cnt = self.df2dict(self.bigtable_cnt, False)

        self.bigtable_neighbour_cnt = pd.read_sql_table("col_neighbour_cnt", conn, schema="table_profile")
        self.bigtable_neighbour_cnt = self.df2dict(self.bigtable_neighbour_cnt, True)

        self.bigtable_range = pd.read_sql_table("col_range", conn, schema="table_profile")
        self.bigtable_range = self.df2dict(self.bigtable_range, True)

        self.bigtable_table = pd.read_sql_table("col_table", conn, schema = "table_profile")
        self.bigtable_table = self.df2dict(self.bigtable_table, True)


        self.cached_table_depend = pd.read_sql_table("key_dep", conn, schema = "table_profile")
        self.cached_table_depend = self.df2dict(self.cached_table_depend, True)

        self.cached_table_fdepen = pd.read_sql_table("fkey_dep", conn, schema="table_profile")
        self.cached_table_fdepen = self.df2dict(self.cached_table_fdepen, True)

        group_cnt = pd.read_sql_table("index_cnt", conn, schema = "table_profile")
        for iter, row in group_cnt.iterrows():
            self.cached_group_cnt[row['col']] = row['value']

        #self.cached_group_cnt = self.cached_group_cnt.to_dict()

        # for col in self.cached_group_tables.keys():
        #     for it in range(self.cached_group_cnt[col]):
        #         try:
        #             self.cached_group_tables[col][it] = pd.read_sql_table(col + "_index" + str(it), conn, schema="column_index")
        #         except:
        #             continue

        for col in self.bigtable.keys():
            try:
                self.cached_group_tables[col] = pd.read_sql_table(col + "_index0", conn, schema="column_index")
            except:
                continue

        for col in self.bigtable.keys():
            if self.bigtable_num[col] != 0:
                self.bigtable_keyscore[col] = float(len(self.bigtable[col]))/float(self.bigtable_num[col])
            else:
                self.bigtable_keyscore[col] = 0

    def dump_index(self):

        self.bigtable = self.dumps_json(self.bigtable)
        self.bigtable_range = self.dumps_json(self.bigtable_range)
        self.bigtable_neighbour_cnt = self.dumps_json(self.bigtable_neighbour_cnt)

        self.bigtable_table = self.dumps_json(self.bigtable_table)

        self.store_index({'col_value':self.bigtable, 'col_dtype':self.bigtable_dtype, 'col_num':self.bigtable_num, \
                      'col_cnt':self.bigtable_cnt, 'col_neighbour_cnt':self.bigtable_neighbour_cnt, \
                      'col_range':self.bigtable_range, 'col_table':self.bigtable_table})

        count_dict = {}
        for col in self.cached_group_tables.keys():
            cnt = 0
            for id in self.cached_group_tables[col].keys():
                self.cached_group_tables[col][id].to_sql(str(col) + "_index" + str(id), con=self.psql_eng, schema="column_index",
                                                     if_exists="replace", index=False)
                cnt = cnt + 1
            count_dict[col] = cnt

        count_df = pd.DataFrame(count_dict.items(), columns=['col', 'value'])
        count_df.to_sql("index_cnt", con = self.psql_eng, schema = "table_profile", if_exists = "replace", index = False)

    def schema_mapping_float_prov(self, tableA, tableB, partial_sm):
        for keyA in tableA.columns.tolist():
            if keyA in partial_sm:
                continue
            dtypeA = tableA[keyA].dtype
            for keyB in tableB.columns.tolist():
                dtypeB = tableB[keyB].dtype
                if dtypeA != dtypeB:
                    continue
                if keyA == keyB:
                    partial_sm[keyA] = keyB
        return partial_sm

    def mapping_merge_score(self, neighbours_score):
        new_score = {}
        for i in neighbours_score.keys():
            new_score[i] = neighbours_score[i]['context_score'] + neighbours_score[i]['range_overlap'] + neighbours_score[i]['keyscore_diff']
        return new_score

    def create_column_profiling(self, colName, colList):

        profile = {}
        profile['value'] = colList.dropna().tolist()

        profile['dtype'] = ""
        profile['dtype'] = colList.dtype

        profile['range'] = []

        profile['keyscore'] = 0

        profile['name'] = colName

        if profile['dtype'] == object:
            profile['range'] = list(set(colList.dropna().tolist()))
        elif profile['dtype'] == np.int64:
            profile['range'] = list(set(colList.dropna().tolist()))
            #profile['range'] = list(range(min(colList.tolist()), max(colList.tolist()) + 1))

        if len(colList.tolist()) == 0:
            kscore = 0
        else:
            kscore = float(len(set(colList.tolist())))/float(len(colList.tolist()))

        profile['keyscore'] = kscore

        return profile

    def initial_cached_cols(self, cols):
        for col in cols:
            self.bigtable[col] = []

            self.bigtable_num[col] = 0
            self.bigtable_cnt[col] = 0

            self.bigtable_range[col] = []
            self.bigtable_neighbour_cnt[col] = {}
            self.bigtable_dtype[col] = ""
            self.bigtable_keyscore[col] = 0
            self.bigtable_table[col] = []

    def update_cached_columns2(self, mapping, tableA, tableAname):

        if len(mapping.items()) != 0:

            tableA_str = tableA.to_string()
            neighbour_str = ','.join(sorted(tableA.columns.tolist()))

            if tableA_str not in self.table_stored_str:

                for colA in mapping.keys():
                    colB = mapping[colA]

                    if colB not in self.bigtable:
                        if len(self.bigtable.keys()) >= cache_col_size:
                            cnt_cols = sorted(self.bigtable_cnt.items(), key = lambda d:d[1])
                            min_number = cnt_cols[0][1]
                            min_col = cnt_cols[0][0]
                            #if len(mapping.items()) - 1 >= min_number:
                            self.remove_cached_cols([min_col])
                        self.initial_cached_cols([colB])
                        self.bigtable_dtype[colB] = tableA[colA].dtype

                    if tableAname not in self.bigtable_table[colB]:
                        self.bigtable_table[colB].append((tableAname,colA))

                    #self.bigtable_dtype[colB] = tableA[colA].dtype
                    #update # of instances, range, neighbour attributes, occurence with neighbours

                    v_list = tableA[colA].dropna().values
                    self.bigtable_num[colB] = self.bigtable_num[colB] + len(v_list)
                    self.bigtable[colB] = list(np.union1d(self.bigtable[colB], v_list))
                    self.bigtable_range[colB] = list(np.union1d(self.bigtable_range[colB], v_list))

                    if neighbour_str not in self.bigtable_neighbour_cnt[colB]:
                        self.bigtable_neighbour_cnt[colB][neighbour_str] = 1
                    else:
                        self.bigtable_neighbour_cnt[colB][neighbour_str] = self.bigtable_neighbour_cnt[colB][
                                                                           neighbour_str] + 1

                    self.bigtable_cnt[colB] = self.bigtable_cnt[colB] + 1

                    if self.bigtable_num[colB] == 0:
                        self.bigtable_keyscore[colB] = 0
                    else:
                        self.bigtable_keyscore[colB] = float(len(self.bigtable[colB]))/float(self.bigtable_num[colB])

    def update_cached_tindex2(self, mapping, tableA, tableA_name):

        table_str = tableA.dropna().to_string()

        for keyA in mapping.keys():
            logging.info(keyA)

            keyB = mapping[keyA]
            values_cachedB = self.bigtable[keyB]

            # if this column has not been cached
            if keyB not in self.cached_group_tables:
                #logging.info("not")
                # create a cached dictionary for each new table with this columnn
                cached_value = {}
                for iter, item in enumerate(tableA[keyA].tolist()):
                    if item in values_cachedB:
                        if item not in cached_value:
                            cached_value[item] = []
                        cached_value[item].append(iter)

                cached_column = {}
                cached_column[tableA_name] = []
                cnt = 0
                for iter, item in enumerate(values_cachedB):
                    if item in cached_value:
                        cached_column[tableA_name].append(json.dumps(cached_value[item]))
                        cnt = cnt + 1
                    else:
                        cached_column[tableA_name].append(json.dumps([]))

                if cnt != 0:
                    try:
                        columnB = pd.DataFrame(cached_column)
                        self.cached_group_tables[keyB] = {}
                        self.cached_group_tables[keyB][0] = columnB
                    except:
                        print(cached_column)

            else:

                if table_str not in self.table_stored_str:
                    #logging.info("yes nor")
                    match_flg = False
                    for cid in self.cached_group_tables[keyB].keys():
                        #logging.info(self.cached_group_tables[keyB].keys())
                        #logging.info(self.cached_group_tables[keyB][cid].shape)
                        #logging.info(list(self.cached_group_tables[keyB].items())[:5])

                        if self.cached_group_tables[keyB][cid].shape[0] < 100:

                            match_flg = True
                            cached_columnB = self.cached_group_tables[keyB][cid].to_frame()
                            #logging.info(cached_columnB)
                            cached_value = {}
                            for iter, item in enumerate(tableA[keyA].tolist()):
                                if item in values_cachedB:
                                    if item not in cached_value:
                                        cached_value[item] = []
                                    cached_value[item].append(iter)

                            cached_column = {}
                            cached_column[tableA_name] = []
                            cnt = 0

                            for iter, item in enumerate(values_cachedB):
                                if item in cached_value:
                                    cached_column[tableA_name].append(json.dumps(cached_value[item]))
                                    cnt = cnt + 1
                                else:
                                    cached_column[tableA_name].append(json.dumps([]))

                            if cnt != 0:
                                columnB = pd.DataFrame(cached_column)
                                if cached_columnB.shape[1] < 100:
                                    cached_columnB = pd.concat([cached_columnB, columnB], axis=1)
                                    self.cached_group_tables[keyB][cid] = cached_columnB

                                else:
                                    new_id = len(self.cached_group_tables[keyB].keys())
                                    self.cached_group_tables[keyB][new_id] = columnB

                            break
                    #logging.info("match false")
                    if match_flg == False:

                        cached_value = {}
                        for iter, item in enumerate(tableA[keyA].tolist()):
                            if item in values_cachedB:
                                if item not in cached_value:
                                    cached_value[item] = []
                                cached_value[item].append(iter)

                        cached_column = {}
                        cached_column[tableA_name] = []
                        cnt = 0

                        for iter, item in enumerate(values_cachedB):
                            if item in cached_value:
                                cached_column[tableA_name].append(json.dumps(cached_value[item]))
                                cnt = cnt + 1
                            else:
                                cached_column[tableA_name].append(json.dumps([]))

                        if cnt != 0:
                            columnB = pd.DataFrame(cached_column)
                            new_id = len(self.cached_group_tables[keyB].keys())
                            self.cached_group_tables[keyB][new_id] = columnB


                else:
                    #logging.info("yes here")
                    tableB_name = self.table_stored_str[table_str]
                    for cid in self.cached_group_tables[keyB].keys():
                        # logging.info(self.cached_group_tables[keyB][cid].to_frame())
                        if tableB_name in self.cached_group_tables[keyB][cid].to_frame().columns.tolist():

                            cached_columnB_old = self.cached_group_tables[keyB][cid].to_frame()
                            copy_column = copy.deepcopy(cached_columnB_old[[tableB_name]])
                            copy_column.rename(columns={tableB_name: tableA_name}, inplace=True)

                            if cached_columnB_old.shape[1] < 100:
                                cached_columnB = pd.concat([cached_columnB_old, copy_column], axis=1)
                                self.cached_group_tables[keyB][cid] = cached_columnB
                            else:
                                new_id = len(self.cached_group_tables[keyB])
                                self.cached_group_tables[keyB][new_id] = copy_column

                                # self.cached_group_tables[keyB] = cached_columnB

                                # logging.info("Here!")
                                # for col in self.cached_group_tables.keys():
                                # print(col)
                                # print("*****************************")
                                # print(self.cached_group_tables[col])
                                #    self.cached_group_tables[col].to_sql(str(col) + "_index", con = self.psql_eng, schema = "column_index", if_exists = "replace", index = False)

    def compute_candidate_pairs_index(self, tableA, thres):

        mapped_pairs = []

        for (colName, colValue) in tableA.iteritems():

            if colValue.dtype == np.float64:
                continue

            if colValue.dtype == float:
                continue

            mflg = False
            for fun_id in range(fun_num):
                label = function[fun_id](colName, colValue.dropna())
                if label == True:
                    mapped_pairs.append((colName, function2name[fun_id], bigN))
                    mflg = True
                    break

            if mflg:
                continue

            data_profile = self.create_column_profiling(colName, colValue)

            #logging.info("Data Profile Created!")

            neighbours_score = {}
            for att in self.bigtable.keys():

                if data_profile['dtype'] != self.bigtable_dtype[att]:
                    continue

                t_col_names = []

                for a, b in self.bigtable_table[att]:
                    t_col_names.append(b)

                if (len(self.bigtable[att]) < 5) and (data_profile['name'] not in t_col_names):
                    continue

                if (len(set(data_profile['value'])) < 5) and (data_profile['name'] not in t_col_names):
                    continue

                range_overlap_score = containment_score(data_profile['range'], self.bigtable_range[att])

                valid_neighbours = sorted(tableA.columns.tolist())
                neighbours_str  = str(';'.join(valid_neighbours))

                if neighbours_str not in self.bigtable_neighbour_cnt[att]:
                    neighbour_score = 0
                else:
                    neighbour_score = float(self.bigtable_neighbour_cnt[att][neighbours_str] + 1)/float(self.bigtable_cnt[att] + 1)

                neighbours_score[att] = {}
                neighbours_score[att]['context_score'] = 0.2 * neighbour_score
                neighbours_score[att]['range_overlap'] = 0.4 * range_overlap_score
                neighbours_score[att]['keyscore_diff'] = 0.4 * (float(1)/(float(abs(self.bigtable_keyscore[att] - data_profile['keyscore']) + 1)) - 0.5)
                #logging.info(att)
                #logging.info(neighbours_score)
                #print(neighbours_score)
                mapping_score = self.mapping_merge_score(neighbours_score)
                #logging.info(mapping_score)

                if len(mapping_score.keys()) <= 0:
                    continue

                mapping_score = sorted(mapping_score.items(), key=lambda d: d[1], reverse=True)

                for t_key, t_score in mapping_score:
                    if t_score > thres:
                        mapped_pairs.append((colName, t_key, t_score))
                    else:
                        break

        return mapped_pairs