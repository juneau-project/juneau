import json
import pandas as pd
import networkx as nx
import numpy as np
import sys
import psycopg2
import logging
import pickle
from datasketch import MinHash
from sqlalchemy.orm import sessionmaker
from data_extension.search.search_tables import SearchTables
from data_extension.search.Sorted import Sorted_Components
from data_extension.search.Random import Random_Components
from data_extension.search.search_sketch import SearchSketch
from data_extension.search_ks import KS
import os
import data_extension.config as config
from data_extension.util import jaccard_similarity, sigmoid, unique_score
from data_extension.table_db import pre_vars
from data_extension.utils.sql_utils import SqlUtils, SqlUtils2
import timeit
import io
from data_extension.search.search_sketch import query_cols_parallel, query_col_parallel


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

# require each table has different name

no_profile_flg = False
sketch_flg = False
class WithView(SearchTables):

    special_type = ['np', 'pd']

    def __connect2db_init(self, dbname = config.sql_dbname):
        # Define our connection string
        conn_string = "host=" + str(config.sql_host) + " dbname=\'" + dbname + "\' user=\'" + config.sql_name + \
                      "\' password=\'" + config.sql_password + "\'"

        # logging.info the connection string we will use to connect
        logging.info("Connecting to database\n	->%s" % (conn_string))

        # get a connection, if a connect cannot be made an exception will be raised here
        try:
            # conn.cursor will return a cursor object, you can use this cursor to perform queries
            conn = psycopg2.connect(conn_string)
            logging.info("Connecting Database Succeeded!\n")
            cursor = conn.cursor()
            return cursor

        except:
            logging.info("Connecting Database Failed!\n")
            logging.info(str(sys.exc_info()))
            return None

    # the schema design should be changed
    def __read_view_queries(self, view_schema, view_table_name):
        conn = self.eng.connect()
        view_names = pd.read_sql_table(view_table_name, conn, schema=view_schema)

        queries = []
        print(view_names.columns)
        for iter, row in view_names.iterrows():
            #if "#r" in row['view_name']:
            #    continue
            join_keys = json.loads(row['join_keys'])
            queries.append([row['view_name'], row['base_tables'], join_keys])
        return queries

    def __read_utils_tables(self):
        conn = self.eng.connect()
        transpose_table = pd.read_sql_table('transpose_table', conn, schema='utils')
        return transpose_table['_tbl'].tolist()

    def __read_data_profiles(self):
        conn = self.eng.connect()

        tables_should_read = []

        single_profile_info = {}
        single_profile_df = pd.read_sql_table('profile', conn, schema=self.profile_schema)
        for iter, row in single_profile_df.iterrows():

            pid = row['col_id']
            pnames = row['col_name'].split("#sep#")
            ptables = row['tables'].split("#sep#")
            assert len(pnames) == len(ptables)
            pvalues = json.loads(row['values'])
            single_profile_info[pid] = {}
            single_profile_info[pid]['name'] = pnames
            single_profile_info[pid]['tables'] = ['rtable'+tname for tname in ptables]

            single_profile_info[pid]['type'] = row['type']

            if single_profile_info[pid]['type'] == str:
                single_profile_info[pid]['values'] = pvalues
            else:
                pvalue_int = {}
                for key in pvalues.keys():
                    pvalue_int[key] = pvalues[key]
                single_profile_info[pid]['values'] = pvalue_int

            tables_should_read += single_profile_info[pid]['tables']

        #print(tables_should_read)
        logging.info(f'There are {len(list(set(tables_should_read)))} related based on data profile!')
        #self.__read_tables(list(set(tables_should_read)))
        self.all_tables_name += tables_should_read

        group_profile_df = pd.read_sql_table('group_profile', conn, schema=self.profile_schema)
        logging.info("Reading Group Profile")
        group_profile_info = {}
        for iter, row in group_profile_df.iterrows():
            gid = row['group_id']
            members = row['member_ids'].split("#sep#")
            group_profile_info[gid] = members

        logging.info(f"{len(single_profile_info.keys())} profiles registered, {len(group_profile_info.keys())} group profiles.")
#        logging.info(f"{len(self.all_tables_name)} tables involved.")
        #exit()

        return single_profile_info, group_profile_info

    def __reverse_table_profiles(self):

        pair_profiles = {}
        single_profiles = {}
        for pid in self.data_profile_single.keys():

            if int(pid) % 10 == 0:
                logging.info(f"{int(pid)} profiles loaded")

            related_tables = self.data_profile_single[pid]['tables']
            related_columns = self.data_profile_single[pid]['name']

            if self.data_profile_single[pid]['type'] == 'float':
                continue

            if self.data_profile_single[pid]['type'] == 'int':
                continue

            for rid1, r1 in enumerate(related_tables):

                if r1 not in self.all_tables_read:
                    self.all_tables_read[r1] = self.__read_table_faster(r1)

                try:
                    if unique_score(self.all_tables_read[r1][related_columns[rid1]].dropna().tolist()) < 0.7:
                        continue
                except:
                    logging.info('error here')
                    logging.info(self.all_tables_read[r1].columns)
                    logging.info(related_columns[rid1])

                if r1 not in single_profiles:
                    single_profiles[r1] = {}

                for rid2, r2 in enumerate(related_tables):

                    if r1 == r2:
                        continue

                    if rid2 <= rid1:
                        continue

                    if r2 not in self.all_tables_read:
                        self.all_tables_read[r2] = self.__read_table_faster(r2)
                        #self.__read_table(r2)

                    try:
                        #join_score = self.compute_join_score_for_columns_with_sketch(r1, r2,
                        #                           related_columns[rid1], related_columns[rid2])
                        join_score = self.compute_join_score_for_columns(self.all_tables_read[r1], self.all_tables_read[r2],
                                                                         related_columns[rid1], related_columns[rid2])
                        #join_score = self.sql_computation_class.join_score(config.sql_dbs, r1, related_columns[rid1], config.sql_dbs, r2, related_columns[rid2])
                        #logging.info(join_score)
                        #print_list.append([r1, r2, related_columns[rid1], related_columns[rid2]])
                    except:
                        logging.error(f"{r1}; {r2}; {related_columns[rid1]}; {related_columns[rid2]}")
                        logging.error(sys.exc_info())
                        continue

                    if join_score < 0.1:
                        continue

                    if r2 not in single_profiles[r1]:
                        single_profiles[r1][r2] = []

                    single_profiles[r1][r2].append([related_columns[rid1], related_columns[rid2],
                                                    join_score])

                    table_pair = '#sep#'.join(sorted([r1, r2]))
                    if table_pair not in pair_profiles:
                        pair_profiles[table_pair] = {}
                    pair_profiles[table_pair][pid] = [related_columns[rid1], related_columns[rid2]]

                self.all_tables_read.clear()

        with open(f"{self.cache_join_dir}/joinable_tables_single_last.pickle","wb") as outfp:
            pickle.dump(single_profiles, outfp)
            outfp.close()
        with open(f"{self.cache_join_dir}/joinable_tables_pair_last.pickle","wb") as outfp:
            pickle.dump(pair_profiles, outfp)
            outfp.close()
        #with open(f"tables_to_compute_joinscore.json", "w") as outfp:
        #    pickle.dump(print_list, outfp)
        #    outfp.close()

        return pair_profiles, single_profiles

    def __read_table(self, name):

        conn = self.eng.connect()

        if not name.startswith('rtable'):
            name = 'rtable' + name

        try:
            if name.startswith(config.sql_dbs + "."):
                name = name[len(config.sql_dbs) + 1:]
            try:
                table_r = pd.read_sql_table(name, conn, schema=config.sql_dbs)
                if 'Unnamed: 0' in table_r.columns:
                    table_r.drop(['Unnamed: 0'], axis=1, inplace=True)
            except:
                table_r = None

        except ValueError:
            logging.info("Value error, skipping table " + name + ", because " + sys.exc_info())
            table_r = None

        except TypeError:
            logging.info("Type error, skipping table " + name + ", because " + sys.exc_info())
            table_r = None

        except:
            logging.info("Unexpected Error, skipping table " + name + ", because " + sys.exc_info())
            table_r = None

        return table_r

    def __read_table_faster(self, name):
        #conn = self.eng.connect()

        if not name.startswith('rtable'):
            name = 'rtable' + name


        if name.startswith(config.sql_dbs + "."):
            name = name[len(config.sql_dbs) + 1:]

        #name = name.replace("#","\#")
        query = f"SELECT * FROM {config.sql_dbs}.\"{name}\""
        #logging.info(query)

        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER"
        )
            #conn = self.eng.raw_connection()
        cur = self.db_cursor #conn.cursor()
        store = io.StringIO()
        cur.copy_expert(copy_sql, store)
        store.seek(0)
        table_r = pd.read_csv(store)

        if 'Unnamed: 0' in table_r.columns:
            table_r.drop(['Unnamed: 0'], axis=1, inplace=True)
        #except:
        #    logging.info(sys.exc_info())
        #    table_r = None

        return table_r

    def __read_tables(self, names):

        conn = self.eng.connect()

        return_tables = {}

        cnt = 0
        for name in names:

            if not name.startswith('rtable'):
                name = 'rtable' + name

            if name in self.all_tables_read:
                return_tables[name] = self.all_tables_read[name]
                continue

            try:
                if name.startswith(config.sql_dbs + "."):
                    name = name[len(config.sql_dbs) + 1:]
                try:
                    table_r = pd.read_sql_table(name, conn, schema=config.sql_dbs)

                    cnt += 1
                    if 'Unnamed: 0' in table_r.columns:
                        table_r.drop(['Unnamed: 0'], axis=1, inplace=True)

                    self.all_tables_read[name] = table_r
                    return_tables[name] = table_r
                except:
                    pass

            except KeyboardInterrupt:
                continue
            except ValueError:
                logging.info("Value error, skipping table " + name + ", because " + sys.exc_info())
                continue
            except TypeError:
                logging.info("Type error, skipping table " + name + ", because " + sys.exc_info())
                continue
            except:
                logging.info("Unexpected Error, skipping table " + name + ", because " + sys.exc_info())
                continue
        logging.info(f"{cnt} new tables read!")
        return return_tables

    def __read_tables_faster(self, names):

        #conn = self.eng.connect()

        return_tables = {}

        cnt = 0
        for name in names:

            if cnt % 100 == 0:
                logging.info(f"{cnt} tables read!")

            if not name.startswith('rtable'):
                name = 'rtable' + name

            if name in self.all_tables_read:
                return_tables[name] = self.all_tables_read[name]
                continue

            try:
                if name.startswith(config.sql_dbs + "."):
                    name = name[len(config.sql_dbs) + 1:]
                try:
                    # name = name.replace("#","\#")
                    query = f"SELECT * FROM {config.sql_dbs}.\"{name}\""
#                    logging.info(query)

                    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
                        query=query, head="HEADER"
                    )
                    # conn = self.eng.raw_connection()
                    cur = self.db_cursor  # conn.cursor()
                    store = io.StringIO()
                    cur.copy_expert(copy_sql, store)
                    store.seek(0)
                    table_r = pd.read_csv(store)

                    cnt += 1
                    if 'Unnamed: 0' in table_r.columns:
                        table_r.drop(['Unnamed: 0'], axis=1, inplace=True)

                    self.all_tables_read[name] = table_r

                    return_tables[name] = table_r
                except:
                    pass

            except KeyboardInterrupt:
                continue
            except ValueError:
                logging.info("Value error, skipping table " + name + ", because " + sys.exc_info())
                continue
            except TypeError:
                logging.info("Type error, skipping table " + name + ", because " + sys.exc_info())
                continue
            except:
                logging.info("Unexpected Error, skipping table " + name + ", because " + sys.exc_info())
                continue

        logging.info(f"{cnt} new tables read!")
        return return_tables

    # def approximate_join_key(self, tableA, tableB, SM, key, thres_prune):
    #
    #     kyA = key
    #     key_value_A = tableA[key].tolist()
    #     scoreA = float(len(set(key_value_A))) / float(len(key_value_A))
    #     if scoreA == 1:
    #         return 1
    #
    #     kyB = SM[key]
    #     key_value_B = tableB[kyB].tolist()
    #     scoreB = float(len(set(key_value_B))) / float(len(key_value_B))
    #     if scoreB == 1:
    #         return 1
    #
    #     if min(scoreA, scoreB) < thres_prune:
    #         return 0
    #
    #     count_valueA = []
    #     for v in key_value_A:
    #         if v in key_value_B:
    #             count_valueA.append(v)
    #
    #     count_valueB = []
    #     for v in key_value_B:
    #         if v in key_value_A:
    #             count_valueB.append(v)
    #
    #     key_scoreAB = float(len(set(count_valueA)))/float(len(count_valueA))
    #     key_scoreBA = float(len(set(count_valueB)))/float(len(count_valueB))
    #
    #     return max(key_scoreAB, key_scoreBA)

    def query_transpose(self, table):
        t_table = table.groupby(by=["key"], dropna=True)
        print(table)
        transpose_table_tuple = []
        #for id
        print(t_table.groups)

    def __init__(self, dbname = config.sql_dbname, schema = config.sql_dbs, clear_cache = False, window_size = 1, approx = 1):

        super().__init__(dbname, schema, read_flag=False)
        self.profile_schema = 'data_profile'

        # fetch all tables
        self.db_cursor = self.__connect2db_init(config.sql_dbname)
        self.queries = self.__read_view_queries("utils", "view_table")
        self.transpose_table = self.__read_utils_tables()
        #self.initialize_in_database_search_class()

        self.all_tables_read = {}
        self.all_tables_name = []
        self.table_cols_storage = {}
        self.table_col_num = {}

        #self.real_tables = {}
        self.initialize_for_topk()
        #self.initialize_memory_states(clear_cache)

        logging.info('Data Search Extension Prepared!')

        self.window_size = window_size
        self.approx_rate = approx

        self.mapping_for_view_testing = {}
        self.sorted_query_list_for_view_testing = {}
        self.joinable_tables_all = {}

        self.search_lshe = SearchSketch(self.eng)
        self.sql_computation_class = SqlUtils(self.eng)
        self.sql_computation_class2 = SqlUtils2()

        self.data_profile_single, self.data_profile_group = self.__read_data_profiles()

        self.cache_join_dir = '/data2/temp_dir'
        logging.info("Cache_Dir: " + self.cache_join_dir)
        local_file = f"{self.cache_join_dir}/joinable_tables_single_last.pickle"
        local_flag = os.path.exists(local_file)
        if local_flag:
            logging.info("Using local file.")
            with open(f"{self.cache_join_dir}/joinable_tables_single_last.pickle","rb") as infp:
                self.reverse_table_single_profile = pickle.load(infp)
                infp.close()
            with open(f"{self.cache_join_dir}/joinable_tables_pair_last.pickle","rb") as infp:
                self.reverse_table_profile = pickle.load(infp)
                infp.close()
            logging.info(list(self.reverse_table_single_profile.items())[:5])
            logging.info("join score loaded.")
        else:
            logging.info("started!")
            self.reverse_table_profile, self.reverse_table_single_profile = self.__reverse_table_profiles()
            logging.info("join score computation finished.")

        #self.load_sketches()

    def load_sketches_for_view_testing(self, table_names):
        #self.table_sketches = {}
        for table in table_names:

            if table in self.table_sketches:
                continue

            if table not in self.all_tables_read:
                self.__read_tables([table])

            m_matrix = {}
            for col in self.all_tables_read[table].columns:
                #col_dtype = self.all_tables_read[table][col].dtype
                #if col_dtype != object and col_dtype != int:
                #    continue
                m = MinHash(num_perm=256)
                values = list(set(self.all_tables_read[table][col].dropna().tolist()))
                for item in values:
                    m.update(str(item).encode('utf-8'))
                m_matrix[col] = m
            self.table_sketches[table] = m_matrix

            #with open('temp_data/sketches.pickle', 'wb') as outfp:
            #    pickle.dump(table_sketches, outfp, pickle.HIGHEST_PROTOCOL)
            #    outfp.close()
        #else:
            #with open('temp_data/sketches.pickle', 'rb') as infp:
            #    table_sketches = pickle.load(infp)
            #    infp.close()

        #return table_sketches

    def save_sketches(self):

        with open(f'{self.cache_join_dir}/sketches.pickle', 'wb') as outfp:
            pickle.dump(self.table_sketches, outfp, pickle.HIGHEST_PROTOCOL)
            outfp.close()

    def load_sketches(self):

        local_file = f"{self.cache_join_dir}/sketches.pickle"
        local_flag = os.path.exists(local_file)

        if local_flag:
            with open(f'{self.cache_join_dir}/sketches.pickle', 'rb') as infp:
                self.table_sketches = pickle.load(infp)
                infp.close()
        else:
            self.table_sketches = {}

    def load_sketches_for_query_table_mapping(self, query, query_name):

        additional_mapping = {}
        new_tables = []
        sk_queries = []
        for col in query.columns:

            if query[col].dtype != object and query[col].dtype != int and query[col].dtype != float:
                continue

            if query_name in self.table_cols_storage:
                if col in self.table_cols_storage[query_name]:
                    continue

            if query[col].dtype == object:
                logging.info("here given str.")
                sk_queries.append({'schema': config.sql_dbs, 'table': query_name, 'type':'str', 'col':col})
            else:
                sk_queries.append({'schema': config.sql_dbs, 'table': query_name, 'type':'int', 'col':col})

        time1 = timeit.default_timer()
        results = self.search_lshe.query_cols(sk_queries)
        time2 = timeit.default_timer()
        #logging.info(results)
        for ress in results:
            #logging.info(ress)
            o_col = ress[0]['col']
            for res in ress[1]:
                res_string = res.split('#sep#')
                new_tables.append(res_string[1])
                if res_string[1] not in additional_mapping:
                    additional_mapping[res_string[1]] = {}
                if o_col not in additional_mapping[res_string[1]]:
                    additional_mapping[res_string[1]][o_col] = []

                if res_string[1] not in self.table_cols_storage:
                    self.table_cols_storage[res_string[1]] = {}
                    self.table_col_num[res_string[1]] = self.sql_computation_class2.col_count(config.sql_dbs, res_string[1])

                if res_string[2] not in self.table_cols_storage[res_string[1]]:
                    self.table_cols_storage[res_string[1]][res_string[2]] = \
                        self.sql_computation_class2.col_unique(config.sql_dbs, res_string[1], res_string[2])

                if query_name not in self.table_cols_storage:
                    self.table_cols_storage[query_name] = {}
                    self.table_col_num[query_name] = self.sql_computation_class2.col_count(config.sql_dbs, query_name)

                if o_col not in self.table_cols_storage[query_name]:
                    self.table_cols_storage[query_name][o_col] = \
                    self.sql_computation_class2.col_unique(config.sql_dbs, query_name, o_col)

                score = jaccard_similarity(self.table_cols_storage[query_name][o_col], self.table_cols_storage[res_string[1]][res_string[2]])
                additional_mapping[res_string[1]][o_col].append([res_string[2], score])
        time3 = timeit.default_timer()

        new_tables = list(set(new_tables))
        logging.info(f"Need to read {len(new_tables)} new tables.")
        return additional_mapping, new_tables, time2 - time1, time3 - time2

    def load_sketches_for_query_mapping(self, query_names, all_additional_mapping, profile_flg = False, pflg = False):

        new_tables = []
        sk_queries = []
        for query_name in query_names:

            query = self.all_tables_read[query_name]

            if profile_flg:
                if query_name in all_additional_mapping:
                    rtables_num = 0
                    for temp_col in all_additional_mapping[query_name].keys():
                        rtables_num += len(all_additional_mapping[query_name][temp_col].items())
                    if rtables_num > 100:
                        continue

            qcnt = 0
            for col in query.columns:

                if qcnt >= 30:
                    break

                if 'self' in col:
                    continue

                if 'par' in col:
                    continue

                if profile_flg:
                    if query_name in all_additional_mapping:
                        if col in all_additional_mapping[query_name]:
                            #if len(all_additional_mapping[query_name][col].items()) > 20:
                            qcnt += 1
                            continue

                if query[col].dtype == object:
                    #logging.info("here given str.")
                    sk_queries.append({'schema': config.sql_dbs, 'table': query_name, 'type':'str', 'col':col})
                    qcnt += 1

                elif query[col].dtype == int or query[col].dtype == float:
                    sk_queries.append({'schema': config.sql_dbs, 'table': query_name, 'type':'int', 'col':col})
                    qcnt += 1

            # icnt = qcnt
            # for col in query.columns:
            #
            #     if icnt >= 50:
            #         break
            #
            #     if 'self' in col:
            #         continue
            #     if 'par' in col:
            #         continue
            #
            #     if query_name in all_additional_mapping:
            #         if col in all_additional_mapping[query_name]:
            #             continue
            #
            #     if query[col].dtype == int or query[col].dtype == float:
            #         sk_queries.append({'schema': config.sql_dbs, 'table': query_name, 'type':'int', 'col':col})
            #         icnt += 1

        time1 = timeit.default_timer()
        if True:
            results = query_cols_parallel(sk_queries)
        else:
            results = []
            for sk_query in sk_queries:
                results.append(query_col_parallel(sk_query)) #query_cols(sk_queries)
        time2 = timeit.default_timer()
        logging.info(time2 - time1)

        #logging.info(results)
        time3 = 0
        for res_id, ress in enumerate(results):

            old_col = sk_queries[res_id]['col']  # ress[0]['col']
            query_name = sk_queries[res_id]['table']
            new_dtype = sk_queries[res_id]['type']

            additional_mapping = {}
            check_time = 0
            for sid, single_res in enumerate(ress):

                if check_time >= 300:
                    break

                res_string = single_res.split('#sep#')
                new_table = res_string[1]
                new_col = res_string[2]

                new_tables.append(new_table)

                if new_table not in additional_mapping:
                    additional_mapping[new_table] = {}

                if old_col not in additional_mapping[new_table]:
                    additional_mapping[new_table][old_col] = []

                if new_table not in self.table_cols_storage:
                    self.table_cols_storage[new_table] = {}
                    self.table_col_num[new_table] = self.sql_computation_class2.col_count(config.sql_dbs, new_table)

                if query_name not in self.table_cols_storage:
                    self.table_cols_storage[query_name] = {}
                    self.table_col_num[query_name] = self.sql_computation_class2.col_count(config.sql_dbs, query_name)

                time4 = timeit.default_timer()
                if old_col not in self.table_cols_storage[query_name]:
                    self.table_cols_storage[query_name][old_col] = \
                    self.sql_computation_class2.col_unique(config.sql_dbs, query_name, old_col)
                else:
                    if self.table_cols_storage[query_name][old_col] is None:
                        try:
                            self.table_cols_storage[query_name][old_col] = \
                                self.sql_computation_class2.col_unique(config.sql_dbs, query_name, old_col)
                        except:
                            self.table_cols_storage[query_name][old_col] = []

                if new_dtype == 'str':
                    if new_col not in self.table_cols_storage[new_table]:
                        try:
                            self.table_cols_storage[new_table][new_col] = \
                                self.sql_computation_class2.col_unique(config.sql_dbs, new_table, new_col)
                            check_time += 1
                        except:
                            self.table_cols_storage[new_table][new_col] = []

                else:
                    if new_col not in self.table_cols_storage[new_table]:
                        self.table_cols_storage[new_table][new_col] = None
                        check_time += 1
                time5 = timeit.default_timer()

                time3 += time5 - time4

                score = 1
                #score = jaccard_similarity(self.table_cols_storage[query_name][old_col], self.table_cols_storage[res_string[1]][res_string[2]])
                additional_mapping[res_string[1]][old_col].append([res_string[2], score])

                if query_name not in all_additional_mapping:
                    all_additional_mapping[query_name] = additional_mapping
                else:
                    for rtable in additional_mapping.keys():
                        if rtable not in all_additional_mapping[query_name]:
                            all_additional_mapping[query_name][rtable] = additional_mapping[rtable]
                        else:
                            for qcol in additional_mapping[rtable].keys():
                                if qcol not in all_additional_mapping[query_name][rtable]:
                                    all_additional_mapping[query_name][rtable][qcol] = additional_mapping[rtable][qcol]
                                else:
                                    #if additional_mapping[rtable][qcol] not in all_additional_mapping[query_name][rtable][qcol]:
                                    all_rtable_cols = []
                                    for rcol, rscore in all_additional_mapping[query_name][rtable][qcol]:
                                        all_rtable_cols.append(rcol)

                                    for rcol, rscore in additional_mapping[rtable][qcol]:
                                        if rcol not in all_rtable_cols:
                                            all_additional_mapping[query_name][rtable][qcol].append([rcol, rscore])

        new_tables = list(set(new_tables))
        logging.info(f"Need to read {len(new_tables)} new tables.")
        return all_additional_mapping, new_tables, time2 - time1, time3

    def get_mapping_from_profile(self, queries, profile_flg = False, float_skip_flg = True):

        self.__read_tables_faster(queries)

        schema_mapping_for_base_tables = {}
        for query in queries:
            schema_mapping_for_base_tables[query] = {}

        logging.info(queries)
        logging.info(f'local sketch flag: {profile_flg}')

        # if sketch_flag:
        #     self.load_sketches_for_view_testing(queries)
        # else:
        #     for query_table in queries:
        #         if query_table not in schema_mapping_for_base_tables:
        #             logging.info(self.all_tables_read[query_table].head())
        #             self.load_sketches_for_query_mapping(self.all_tables_read[query_table], query_table)

        tables_to_read = []

        if profile_flg:
            for pid in self.data_profile_single.keys():

                #if float_skip_flg:
                #if self.data_profile_single[pid]['type'] == 'float':
                #    continue

                related_tables = self.data_profile_single[pid]['tables']
                related_columns = self.data_profile_single[pid]['name']

                for query_table in queries:

                    if query_table not in related_tables:
                        continue

                    tables_to_read += related_tables

                    q_index = related_tables.index(query_table)

                    for tid, table in enumerate(related_tables):

                        if tid == q_index:
                            continue

                        if table not in schema_mapping_for_base_tables[query_table]:
                            schema_mapping_for_base_tables[query_table][table] = {}

                        if related_columns[q_index] not in schema_mapping_for_base_tables[query_table][table]:
                            schema_mapping_for_base_tables[query_table][table][related_columns[q_index]] = []
                        schema_mapping_for_base_tables[query_table][table][related_columns[q_index]].append([related_columns[tid], len(related_tables)])

                        if query_table not in self.table_cols_storage:
                            self.table_cols_storage[query_table] = {}
                            try:
                                self.table_col_num[query_table] = self.sql_computation_class2.col_count(config.sql_dbs, query_table)
                            except:
                                logging.info(query_table)
                                logging.info(sys.exc_info())

                        if related_columns[q_index] not in self.table_cols_storage[query_table]:
                            column_to_store = related_columns[q_index]
                            try:
                                self.table_cols_storage[query_table][column_to_store] = \
                                self.sql_computation_class2.col_unique(config.sql_dbs, query_table, column_to_store)
                            except:
                                self.table_cols_storage[query_table][column_to_store] = []
                                logging.info(sys.exc_info())

                        if table not in self.table_cols_storage:
                            self.table_cols_storage[table] = {}
                            try:
                                self.table_col_num[table] = self.sql_computation_class2.col_count(config.sql_dbs, table)
                            except:
                                logging.info(table)
                                logging.info(sys.exc_info())

                        if related_columns[tid] not in self.table_cols_storage[table]:
                            column_to_store = related_columns[tid]
                            try:
                                self.table_cols_storage[table][column_to_store] = \
                                    self.sql_computation_class2.col_unique(config.sql_dbs, table, column_to_store)
                            except:
                                self.table_cols_storage[table][column_to_store] = []
                                logging.info(sys.exc_info())

        related_table_cnt = []
        for query_table in schema_mapping_for_base_tables.keys():
            related_cnt = len(schema_mapping_for_base_tables[query_table].keys())
            related_table_cnt.append(related_cnt)
        logging.info("After loading data profile:")
        logging.info(related_table_cnt)

        all_time1 = 0
        all_time2 = 0

        if True:
            #for query_table in queries:
            #if query_table not in schema_mapping_for_base_tables:
                #logging.info(f"Empty Table: {query_table}")
            #    schema_mapping_for_base_tables[query_table] = {}
            sketch_mapping, temp_new_tables, sketch_time, reading_time = self.load_sketches_for_query_mapping(queries, schema_mapping_for_base_tables,
                                                                                                              profile_flg, True)
            #self.load_sketches_for_query_mapping(self.all_tables_read[query_table],
                                                                         #          query_table)
            #logging.info("mapping from sketch:")
            #logging.info(sketch_mapping)
            #logging.info(f"require time {sketch_time}")

            all_time1 += sketch_time
            all_time2 += reading_time

            # if query_table not in schema_mapping_for_base_tables:
            #     schema_mapping_for_base_tables[query_table] = sketch_mapping
            # else:
            #     for rtable in sketch_mapping.keys():
            #         if rtable not in schema_mapping_for_base_tables[query_table]:
            #             schema_mapping_for_base_tables[query_table][rtable] = sketch_mapping[rtable]
            #         else:
            #             for qcol in sketch_mapping[rtable].keys():
            #                 if qcol not in schema_mapping_for_base_tables[query_table][rtable]:
            #                     schema_mapping_for_base_tables[query_table][rtable][qcol] = sketch_mapping[rtable][qcol]
            #                 else:
            #                     schema_mapping_for_base_tables[query_table][rtable][qcol] += sketch_mapping[rtable][qcol]

            tables_to_read += temp_new_tables

            related_table_cnt = []
            for query_table in schema_mapping_for_base_tables.keys():
                related_cnt = len(schema_mapping_for_base_tables[query_table].keys())
                related_table_cnt.append(related_cnt)

            logging.info("After loading sketch.")
            logging.info(related_table_cnt)

        tables_to_read = list(set(tables_to_read))
        logging.info(f"Overall {len(tables_to_read)} tables should be read!")
        self.all_tables_name += tables_to_read
        #logging.info(self.table_sketches.keys())
        #logging.info(schema_mapping_for_base_tables)

        time1 = timeit.default_timer()
        schema_mapping_to_return = {}
        for qtable in schema_mapping_for_base_tables.keys():

            schema_mapping_to_return[qtable] = {}
            for rtable in schema_mapping_for_base_tables[qtable].keys():

                #self.__read_tables_faster([rtable])

                #if sketch_flg:
                #    self.load_sketches_for_view_testing([rtable])

#                rtable_df = self.all_tables_read[rtable]
                #logging.info(rtable_df.columns)
                rtable_all_stored = self.table_cols_storage[rtable]

                schema_mapping_to_return[qtable][rtable] = {}
                for qcol in schema_mapping_for_base_tables[qtable][rtable].keys():

                    if qtable not in self.table_cols_storage:
                        self.table_cols_storage[qtable] = {}
                        try:
                            self.table_col_num[qtable] = self.sql_computation_class2.col_count(config.sql_dbs, qtable)
                        except:
                            logging.info(sys.exc_info())
                            logging.info(f'Error: {qtable}')

                    if qcol not in self.table_cols_storage[qtable]:
                        try:
                            self.table_cols_storage[qtable][qcol] = \
                                self.sql_computation_class2.col_unique(config.sql_dbs, qtable, qcol)
                        except:
                            logging.info(sys.exc_info())
                            logging.info(f'Error in finding column for query table: {qtable}:{qcol}')
                            self.table_cols_storage[qtable][qcol] = []

                    qtable_all_stored = self.table_cols_storage[qtable]

                    col_match_scores = {}
                    rcols = schema_mapping_for_base_tables[qtable][rtable][qcol]
                    for rcol, rcnt in rcols:

                        if rcol not in rtable_all_stored or qcol not in qtable_all_stored:
                            match_score = 0
                        else:
                            if not rtable_all_stored[rcol]:
                                match_score = 0.85
                            else:
                                match_score = jaccard_similarity(qtable_all_stored[qcol],
                                                            #qtable_df[qcol].dropna().tolist(),
                                                             rtable_all_stored[rcol])
                                                             #rtable_df[rcol].dropna().tolist())
                        #col_match_score = self.sql_computation_class.jaccard(config.sql_dbs, )
                        if match_score < 0.85:
                            continue

                        col_match_scores[rcol] = match_score
                    col_match_scores = sorted(col_match_scores.items(), key=lambda d: d[1], reverse=True)

                    if len(col_match_scores) > 0:
                        colA = col_match_scores[0][0]

                        if qcol not in self.table_cols_storage[qtable]:
                            q_value = []
                        else:
                            q_value = self.table_cols_storage[qtable][qcol]

                        if colA not in self.table_cols_storage[rtable]:
                            r_value = []
                        else:
                            if not self.table_cols_storage[rtable][colA]:
                                r_value = q_value
                            else:
                                r_value = self.table_cols_storage[rtable][colA]

                            #r_value = self.all_tables_read[rtable][colA].dropna().tolist()


                        key_score = (len(q_value) + len(r_value))/(len(np.union1d(q_value, r_value)) + 1)
                        new_score = len(set(r_value).difference(set(q_value)))/(len(r_value) + 1)
                    else:
                        key_score = 0
                        new_score = 0
                        colA = None

                        #for rcol, rcnt in rcols:
                        #    match_score = self.table_sketches[qtable][qcol].jaccard(self.table_sketches[rtable][rcol])
                        #    col_match_scores[rcol] = match_score
                        #col_match_scores = sorted(col_match_scores.items(), key=lambda d: d[1], reverse=True)
                        #colA = col_match_scores[0][0]
                        #key_score = 0
                        #new_score = 0

                    if colA:
                        schema_mapping_to_return[qtable][rtable][qcol] = [colA, col_match_scores[0][1],
                                                                      1/np.log(len(rcols) + 1) * col_match_scores[0][1], key_score, new_score]
                #if rtable not in queries:
                #    del self.all_tables_read[rtable]
        time2 = timeit.default_timer()
        logging.info(f"pre_compute time1-sk: {all_time1}")
        logging.info(f"pre_compute time2-io: {all_time2}")
        logging.info(f"pre_compute time2-ja: {time2 - time1}")

        return schema_mapping_to_return, all_time1, all_time2 + time2 - time1

    def compute_join_score_for_tables(self, tableA, tableB):

        if tableA + "#sep#" + tableB in self.reverse_table_profile:
            colA, colB = self.reverse_table_profile[tableA + "#sep#" + tableB]
        elif tableB + '#sep#' + tableA in self.reverse_table_profile:
            colB, colA = self.reverse_table_profile[tableB + "#sep#" + tableB]
        return self.compute_join_score_for_columns(tableA, tableB, colA, colB)

    def compute_join_score_for_columns_with_sketch(self, tableA, tableB, colA, colB):
        return self.join_score_computation_class.join_score(config.sql_dbs, tableA, colA, config.sql_dbs, tableB, colB)

    def compute_join_score_for_columns(self, tableA, tableB, colA, colB):
        valueA = list(set(tableA[colA].dropna().tolist()))
        valueA = [str(v) for v in valueA]
        valueB = list(set(tableB[colB].dropna().tolist()))
        valueB = [str(v) for v in valueB]

        return len(np.intersect1d(valueA, valueB)) / len(valueA)

    def schema_mapping_for_view_testing(self, queries, threshold = 0.8):

        sketch_mapping = {}
        for qid, table1 in enumerate(queries):
            table_mapping = {}
            for tid, table2 in enumerate(self.real_table_sketches.keys()):
                if table1 == table2:
                    continue
                column_mapping = {}
                for col1 in self.real_table_sketches[table1].keys():
                    ranking = []
                    for col2 in self.real_table_sketches[table2].keys():
                        #print(self.real_tables.keys())
                        if self.real_tables[table1][col1].dtype != self.real_tables[table2][col2].dtype:
                            continue
                        overlap = self.real_table_sketches[table1][col1].jaccard(self.real_table_sketches[table2][col2])
                        if overlap > threshold:
                            ranking.append([col2, overlap])
                    if len(ranking) > 0:
                        ranking = sorted(ranking, key=lambda d: d[1], reverse=True)
                        column_mapping[col1] = {}
                        column_mapping[col1][ranking[0][0]] = ranking[0][1]
                table_mapping[table2] = column_mapping
            sketch_mapping[table1] = table_mapping

        return sketch_mapping

    def joinable_columns_for_view_testing(self, threshold = 0.7, store_flag = False):


        joinable_tables_all = {}
        if store_flag:
            for tid1, table1 in enumerate(self.real_table_sketches.keys()):

                joinable_tables = {}

                for tid2, table2 in enumerate(self.real_table_sketches.keys()):

                    if tid1 == tid2:
                        continue

                    for col1 in self.real_table_sketches[table1].keys():

                        if self.real_tables[table1][col1].dtype != object:
                            continue

                        col1_sketch = self.real_table_sketches[table1][col1]

                        col_pairs = []

                        for col2 in self.real_table_sketches[table2].keys():

                            if self.real_tables[table2][col2].dtype != object:
                                continue

                            if self.real_tables[table1][col1].dtype != self.real_tables[table2][col2].dtype:
                                continue

                            col2_sketch = self.real_table_sketches[table2][col2]

                            overlap = col1_sketch.jaccard(col2_sketch)

                            if overlap > threshold:
                                col_pairs.append((col2, overlap))

                        if len(col_pairs) > 0:
                            col_pairs = sorted(col_pairs, key=lambda d: d[1], reverse=True)
                            mapped_col = col_pairs[0][0]
                            mapped_score = col_pairs[0][1]

                            if table2 not in joinable_tables:
                                joinable_tables[table2] = []

                            joinable_tables[table2].append([col1, mapped_col, mapped_score])
                        else:
                            continue
                joinable_tables_all[table1] = joinable_tables
            with open('temp_data/joinable.pickle', 'wb') as outfp:
                pickle.dump(joinable_tables_all, outfp, pickle.HIGHEST_PROTOCOL)
                outfp.close()
        else:
            with open('temp_data/joinable.pickle', 'rb') as infp:
                joinable_tables_all = pickle.load(infp)
                infp.close()

        return joinable_tables_all

    def search_additional_training_data_for_view_test(self, query, k, candidate_table_list = None, alpha = 0.1, beta = 0.9):

        if query not in self.sorted_query_list_for_view_testing:

            if not candidate_table_list:
                candidate_table_list = list(self.all_tables_read.keys())

            sorted_list = {}
            for table in candidate_table_list:

                if table == query:
                    continue

                #if table not in self.mapping_for_view_testing[query]:
                #    continue

                mapping = self.mapping_for_view_testing[query][table]

                key_ranking = {}
                for q_col in mapping.keys():
                    r_col = list(mapping[q_col].keys())[0]
                    r_col_score = mapping[q_col][r_col]
                    # col2_values = self.real_tables[table][col2].dropna()

                    # if col2_values.shape[0] == 1:
                    #    continue

                    # col2_values = col2_values.squeeze().to_list()
                    # col2_key_score = len(set(col2_values))/len(col2_values)
                    key_ranking[q_col] = r_col_score

                key_ranking = sorted(key_ranking.items(), key = lambda d:d[1], reverse=True)

                if len(key_ranking) == 0:
                    continue

                key_col1 = key_ranking[0][0]
                key_col2 = mapping[key_col1]
                key_col1_values = self.real_tables[query][key_col1].dropna().squeeze().values
                key_col2_values = self.real_tables[table][key_col2].dropna().squeeze().values

                row_sim = len(np.intersect1d(key_col1_values, key_col2_values))/len(np.union1d(key_col1_values, key_col2_values))
                col_sim = len(mapping.items())/len(self.real_tables[table].columns)
                sorted_list[table] = alpha * row_sim + beta * col_sim

            sorted_list = sorted(sorted_list.items(), key = lambda d:d[1], reverse=True)
            self.sorted_query_list_for_view_testing[query] = sorted_list
        else:
            sorted_list = self.sorted_query_list_for_view_testing[query]

        if k > len(sorted_list):
            k = len(sorted_list)
        return sorted_list[:k]

    def search_unionable_data_for_table(self, query, k, candidate_table_list=None, alpha=0.6, beta=0.3):

        if query not in self.sorted_query_list_for_view_testing:

            if not candidate_table_list:
                candidate_table_list = list(self.table_cols_storage.keys())
                #list(self.all_tables_read.keys())

            #logging.info(f"In Single Search for {query}, {len(self.all_tables_read.keys())} candidate tables.")
            sorted_list = {}

            if query not in self.mapping_for_view_testing:
                logging.info(f"{query} not detected in mapping.")
                return []
            #logging.info(self.mapping_for_view_testing[query])
            #exit()
            for table in candidate_table_list:

                if table == query:
                    continue

                if table not in self.mapping_for_view_testing[query]:
                    sorted_list[table] = 0
                    continue

                mapping = self.mapping_for_view_testing[query][table]

                if len(list(mapping.keys())) == 0:
                    sorted_list[table] = 0
                    continue

                key_ranking = []
                col_score = 0
                for q_col in mapping.keys():

                    r_col, r_col_score, r_col_score_reg, key_score, new_score = mapping[q_col]
                    col_score += r_col_score #self.table_sketches[query][q_col].jaccard(self.table_sketches[table][r_col])

                    key_ranking.append([q_col, key_score, r_col_score, new_score])
                key_ranking = sorted(key_ranking, key=lambda d: d[1], reverse=True)

                key_col1 = key_ranking[0][0]
                key_col2 = mapping[key_col1][0]

                row_sim = key_ranking[0][2]
                col_sim = col_score / self.table_col_num[table] #self.all_tables_read[table].shape[1]
                new_score = key_ranking[0][3]

                sorted_list[table] = sigmoid(alpha * col_sim + beta * new_score + (1 - alpha - beta) * row_sim)

            sorted_list = sorted(sorted_list.items(), key=lambda d: d[1], reverse=True)
            self.sorted_query_list_for_view_testing[query] = sorted_list
        else:
            sorted_list = self.sorted_query_list_for_view_testing[query]

        if k > len(sorted_list):
            k = len(sorted_list)
        return sorted_list[:k]

    def search_unionable_data_for_table_sketch(self, query, k, candidate_table_list=None, alpha=0.6, beta=0.3):

        if query not in self.sorted_query_list_for_view_testing:

            if not candidate_table_list:
                candidate_table_list = list(set(self.all_tables_name))

            logging.info(len(candidate_table_list))
            sorted_list = {}

            if query not in self.mapping_for_view_testing:
                return []

            for table in candidate_table_list:

                if table == query:
                    continue

                if table not in self.mapping_for_view_testing[query]:
                    continue

                mapping = self.mapping_for_view_testing[query][table]

                if len(list(mapping.keys())) == 0:
                    continue

                #key_ranking = []
                col_score = []
                for q_col in mapping.keys():
                    r_col, r_col_score, r_col_score_reg, key_score, new_score = mapping[q_col]
                    jaccard_sim = self.table_sketches[query][q_col].jaccard(self.table_sketches[table][r_col])
                    col_score.append(jaccard_sim)

                #if abs(np.mean(col_score) - 1) < 0.001:
                #    continue

                sorted_list[table] = np.mean(col_score)  # sigmoid(alpha * col_sim + beta * new_score + (1 - alpha - beta) * row_sim)

                    #col_score += r_col_score_reg
                    #key_ranking.append([q_col, key_score, r_col_score, new_score])
                #key_ranking = sorted(key_ranking, key=lambda d: d[1], reverse=True)

                #key_col1 = key_ranking[0][0]
                #key_col2 = mapping[key_col1][0]

                #row_sim = key_ranking[0][2]
                #col_sim = col_score / self.all_tables_read[table].shape[1]
                #new_score = key_ranking[0][3]


            sorted_list = sorted(sorted_list.items(), key=lambda d: d[1], reverse=True)
            self.sorted_query_list_for_view_testing[query] = sorted_list
        else:
            sorted_list = self.sorted_query_list_for_view_testing[query]

        if k > len(sorted_list):
            k = len(sorted_list)
        return sorted_list[:k]

    def initialize_in_database_search_class(self):

        self.search_lshe = SearchSketch(self.eng)
        self.search_ks = KS(self.eng)

    def initialize_for_topk(self):
        self.memory_mapping = {}
        self.memory_sas = {}
        for i in range(4):
            self.memory_sas[i] = {}

        self.memory_ras = {}
        for i in range(4):
            self.memory_ras[i] = {}

    def initialize_memory_states(self, cflg):
        try:
            query1 = "DROP SCHEMA IF EXISTS " + config.sql_schema_ra_states + " CASCADE;"
            query2 = "CREATE SCHEMA IF NOT EXISTS " + config.sql_schema_ra_states + ";"
            query3 = "DROP SCHEMA IF EXISTS " + config.sql_schema_sa_states + " CASCADE;"
            query4 = "CREATE SCHEMA IF NOT EXISTS " + config.sql_schema_sa_states + ";"

            conn = self.eng.connect()

            if cflg:
                try:
                    conn.execute(query1)
                    conn.execute(query3)
                except:
                    logging.error("Topk State Memory: DROP SCHEMA FAILED!\n")
                    logging.info(sys.exc_info())

            try:
                conn.execute(query2)
                conn.execute(query4)
            except:
                logging.error("Topk State Memory: CREATE SCHEMA FAILED\n")
                logging.info(sys.exc_info())

            finally:
                conn.close()

            return True
        except:
            logging.error("Topk State Memory: Connecting Database Failed!\n")
            logging.info(sys.exc_info())
            return False

    def search_tables_from_database(self, query, SM_test, lsh_flag = True, workflow_flag = True):

        tableA = query.value
        mapping = {}
        if lsh_flag:
            for col in tableA.columns:
                #print(col, tableA[col].dtype)
                if tableA[col].dtype == 'object':
                    c_table = self.search_lshe.query_cols([{'schema':config.sql_dbs, 'table':query.name, 'col':col}])[0]
                    if len(c_table['similar_domains']) > 0:
                        if col not in mapping:
                            mapping[col] = []
                        for res in c_table['similar_domains']:
                            mapping[col].append([res['table'], res['column']])
                elif tableA[col].dtype == 'int' or tableA[col].dtype == 'float':
                    c_table = self.search_ks.query_cols([{'schema':config.sql_dbs, 'table':query.name, 'col':col}])[0]
                    #print(c_table)
                    if len(c_table['similar_domains']) > 0:
                        if col not in mapping:
                            mapping[col] = []
                        for res in c_table['similar_domains']:
                            mapping[col].append([res['table'], res['column']])

        logging.info("Searching: partial mapping")
        partial_mapping, candidate_keys, partial_mapping_cached = SM_test.mapping_to_columns_keys_search(tableA, 2)

        all_mapping = mapping
        for col in partial_mapping.keys():
            if col not in all_mapping:
                all_mapping[col] = partial_mapping[col]
            else:
                for val in partial_mapping[col]:
                    if val not in all_mapping[col]:
                        all_mapping[col].append(val)
        all_tables = []
        for col in all_mapping.keys():
            for val in all_mapping[col]:
                all_tables.append(val[0])
        all_tables = list(set(all_tables))

        self.real_tables = self.__read_tables_faster(all_tables)

        self.real_tables[query.name] = tableA

        if workflow_flag:
            schema_map_partial = SM_test.add_mapping_by_workflow(query.name, all_mapping, self.real_tables, self.table_group)
        else:
            schema_map_partial = all_mapping

        return schema_map_partial, candidate_keys, partial_mapping_cached

    def schema_mapping(self, tableA, tableB, meta_mapping, gid):
        s_mapping = {}
        t_mapping = {}
        for i in tableA.columns.tolist():
            if i not in meta_mapping[gid]:
                continue
            t_mapping[self.schema_linking[gid][meta_mapping[gid][i]]] = i

        for i in tableB.columns.tolist():
            if self.schema_linking[gid][i] in t_mapping:
                if tableB[i].dtype != tableA[t_mapping[self.schema_linking[gid][i]]].dtype:
                    continue
                s_mapping[t_mapping[self.schema_linking[gid][i]]] = i

        max_valueL = []
        for i in s_mapping.keys():
            j = s_mapping[i]
            max_valueL.append(self.row_similarity(tableA[i], tableB[j]))

        if len(max_valueL) > 0:
            mv = max(max_valueL)
        else:
            mv = 0

        return s_mapping, mv

    def update_left_combined(self, new_results, update_tables, old_bounds):
        new_bounds = []
        for tid, tuple in enumerate(old_bounds):
            triple_id1 = tuple[0]
            triple_id2 = tuple[1]
            triple_id3 = tuple[2]

            temp_bounds = tuple[3]
            temp_bounds_flag = tuple[4]

            if triple_id1 != 'incomplete':
                new_bounds.append(tuple)
                continue

            if triple_id2 not in update_tables:
                new_bounds.append(tuple)
                continue

            for uid in update_tables[triple_id2]:
                table_ids = new_results[uid][0]
                table_ids1_str = ""
                for t_tid, table_id in enumerate(table_ids):
                    table_ids1_str = table_ids1_str + "&" + str(table_id)

                temp_bounds[0] = new_results[uid][1]
                temp_bounds_flag[0] = True

                new_bounds.append([table_ids1_str, triple_id2, triple_id3, temp_bounds, temp_bounds_flag])
        return new_bounds

    def update_right_single(self, update_tables, old_bounds):

        for tid, tuple in enumerate(old_bounds):
            triple_id1 = tuple[0]
            triple_id2 = tuple[1]
            triple_id3 = tuple[2]

            temp_bounds = tuple[3]
            temp_bounds_flag = tuple[4]

            if triple_id2 in update_tables:
                if not temp_bounds_flag[1]:
                    temp_bounds[1] = update_tables[triple_id2]
                    temp_bounds_flag[1] = True
                    old_bounds[tid] = [triple_id1, triple_id2, triple_id3, temp_bounds, temp_bounds_flag]
        return old_bounds

    def update_pair_columns(self, subqids, subqueries, visited, col_ids, new_sorted_lists, sorted_access_read, \
                            possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound):
        #logging.info('update')
        qid1 = subqids[0]
        qid2 = subqids[1]

        qtid1 = subqueries[0]
        qtid2 = subqueries[1]

        table_id1 = new_sorted_lists[qtid1][col_ids[qtid1]][0]

        if table_id1 not in visited[qid1]:
            visited[qid1][table_id1] = {}

        # )


        if table_id1 in self.reverse_table_single_profile:

            for table_id2 in self.reverse_table_single_profile[table_id1].keys():

                if table_id2 in visited[qid1][table_id1]:
                    continue

                #logging.info(table_id1)
                #logging.info(table_id2)
                #logging.info(self.reverse_table_single_profile[table_id1][table_id2])

                visited[qid1][table_id1][table_id2] = True

                if qid1 < qid2:
                    possible_view = str(table_id1) + "&" + str(table_id2)

                    lower_bound = [new_sorted_lists[qtid1][col_ids[qtid1]][1]]
                    lower_bound_flag = [True]
                    upper_bound = [new_sorted_lists[qtid1][col_ids[qtid1]][1]]
                    upper_bound_flag = [True]

                    if table_id2 in sorted_access_read[qid2]:
                        lower_bound += [sorted_access_read[qid2][table_id2]]
                        lower_bound_flag += [True]
                        upper_bound += [sorted_access_read[qid2][table_id2]]
                        upper_bound_flag += [True]

                    else:
                        lower_bound += [0]
                        lower_bound_flag += [False]
                        upper_bound += [new_sorted_lists[qtid2][-1][1]]
                        upper_bound_flag += [False]

                    joinscore = max([t[2] for t in self.reverse_table_single_profile[table_id1][table_id2]])
                    #joinscore = 1
                    lower_bound += [joinscore]
                    lower_bound_flag += [True]
                    upper_bound += [joinscore]
                    upper_bound_flag += [True]

                    if possible_view not in possible_views_dictionary:
                        possible_views_dictionary[possible_view] = ""
                        possible_views_lower_bound.append([table_id1, table_id2, lower_bound, lower_bound_flag])
                        possible_views_upper_bound.append([table_id1, table_id2, upper_bound, upper_bound_flag])

                else:
                    possible_view = str(table_id2) + "&" + str(table_id1)

                    if table_id2 in sorted_access_read[qid2]:
                        lower_bound = [sorted_access_read[qid2][table_id2]]
                        upper_bound = [sorted_access_read[qid2][table_id2]]
                        lower_bound_flag = [True]
                        upper_bound_flag = [True]
                    else:
                        lower_bound = [0]
                        lower_bound_flag = [False]
                        #print(new_sorted_lists)
                        upper_bound = [new_sorted_lists[qtid2][-1][1]]
                        upper_bound_flag = [False]

                    lower_bound += [new_sorted_lists[qtid1][col_ids[qtid1]][1]]
                    lower_bound_flag += [True]
                    upper_bound += [new_sorted_lists[qtid1][col_ids[qtid1]][1]]
                    upper_bound_flag += [True]

                    joinscore = max([t[2] for t in self.reverse_table_single_profile[table_id1][table_id2]])
                    #joinscore = 1
                    lower_bound += [joinscore]
                    lower_bound_flag += [True]
                    upper_bound += [joinscore]
                    upper_bound_flag += [True]

                    if possible_view not in possible_views_dictionary:
                        possible_views_dictionary[possible_view] = ""
                        possible_views_lower_bound.append([table_id2, table_id1, lower_bound, lower_bound_flag])
                        possible_views_upper_bound.append([table_id2, table_id1, upper_bound, upper_bound_flag])

            #logging.info(possible_views_lower_bound)
        return possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound, visited

    def topk_stop_condition(self, possible_views_lower_bound, possible_views_upper_bound, topk, score_index):
        #score_index = 3
#        logging.info(possible_views_lower_bound)
#        logging.info(possible_views_upper_bound)
        time1 = timeit.default_timer()
        possible_views_upper_bound = sorted(possible_views_upper_bound, key = lambda d: np.sum(d[score_index]), reverse=True)
        possible_views_lower_bound = sorted(possible_views_lower_bound, key = lambda d: np.sum(d[score_index]), reverse=True)
        time2 = timeit.default_timer()

        #logging.info("************lower******")
        #logging.info(possible_views_lower_bound)
        #logging.info("***********upper*******")
        #logging.info(possible_views_upper_bound)

        top_lower_bound_views = []
        for tid, tuple in enumerate(possible_views_lower_bound):
            if tid >= topk:
                break
            if score_index == 3:
                top_lower_bound_views.append(str(tuple[0]) + "&" + str(tuple[1]) + "#sep#" + str(tuple[2]))
            elif score_index == 2:
                top_lower_bound_views.append(str(tuple[0]) + "&" + str(tuple[1]))

        new_possible_views_upper_bound = []
        for tuple in possible_views_upper_bound:
            if score_index == 3:
                if str(tuple[0]) + "&" + str(tuple[1]) + "#sep#" + str(tuple[2]) not in top_lower_bound_views:
                    new_possible_views_upper_bound.append(tuple)
            elif score_index == 2:
                if str(tuple[0]) + "&" + str(tuple[1]) not in top_lower_bound_views:
                    new_possible_views_upper_bound.append(tuple)

        time3 = timeit.default_timer()
        new_possible_views_upper_bound = sorted(new_possible_views_upper_bound, \
                                                key = lambda d: np.sum(d[score_index]), reverse=True)
        time4 = timeit.default_timer()

        self.sort_time = self.sort_time + time4 - time3 + time2 - time1

        if np.sum(possible_views_lower_bound[topk - 1][score_index]) >= np.sum(new_possible_views_upper_bound[0][score_index]):
            return possible_views_lower_bound, possible_views_upper_bound
        else:
            return None, None

    def sorted_joinkey(self):
        sorted_tuple = []
        for table1 in self.reverse_table_single_profile.keys():
            for table2 in self.reverse_table_single_profile[table1]:
                for r1, r2, rscore in self.reverse_table_single_profile[table1][table2]:
                    sorted_tuple.append([table1, r1, table2, r2, rscore])
        sorted_key = sorted(sorted_tuple, key = lambda d:d[4], reverse=True)
        self.sorted_jk_list = sorted_key

    def search_additional_training_data(self, SM_test, query, k, candidate_table_list = None, alpha = 0.1, beta = 0.9, theta = 1):

        schema_map_partial, candidate_keys, partial_mapping_cached = self.search_tables_from_database(query, SM_test, False, False)
        logging.info("Searching: Longer Mapping Detected!")

        if candidate_table_list == None:
            candidate_table_list = self.tables

        candidate_tables = {}
        for name in candidate_table_list:
            if name not in self.real_tables:
                continue
            candidate_tables[name] = self.real_tables[name]
        logging.info("There are " + str(len(candidate_tables.items())) + " candidate tables!")

        if len(candidate_tables.items()) == 1:
            return []

        if query.name in self.memory_sas:
            previous_state = self.memory_sas[0][query.name]
        else:
            previous_state = None

        #Introducing Sorted Access
        sorted_access = Sorted_Components(schema_map_partial, candidate_tables, self.Graphs, previous_state)

        # Computing Sorted Access
        rank_candidate, rank_score_only, sorted_state = sorted_access.merge_additional_training(query, alpha, beta)
        self.memory_sas[0][query.name] = sorted_state
        #print(rank_candidate)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        if query.name in self.memory_ras:
            previous_state = self.memory_ras[0][query.name]
        else:
            previous_state = None

        random_access = Random_Components(self.real_tables, SM_test, previous_state)
        random_scores = {}
        top_tables = []
        for i in range(ks):
            tableB_name = rank_candidate[i][0]
            sm = rank_candidate[i][2]
            sorted_score = rank_score_only[i][1]
            score, scores = random_access.merge_additional_training(sorted_score, query, tableB_name, sm, candidate_keys, partial_mapping_cached, alpha, beta)
            random_scores[tableB_name] = scores
            top_tables.append((rank_candidate[i][0], score))

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
        min_value = top_tables[-1][1]
        ks = ks - 1
        id = 0
        while (True):

            if ks + id >= len(rank_candidate):
                break

            threshold = rank_candidate[ks + id][1]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableB_name = rank_candidate[ks + id][0]
                sm = rank_candidate[ks + id][2]
                sorted_score = rank_score_only[ks + id][1]

                new_score, new_scores = random_access.merge_additional_training(sorted_score, query, tableB_name, sm, candidate_keys, partial_mapping_cached, alpha, beta)

                random_scores[tableB_name] = new_scores

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        cnt = 0
        rtables = []
        for i, j in top_tables:
            rtables.append([i, self.real_tables[i]])
            cnt = cnt + 1
            if cnt == k:
                break

        random_state = random_access.save_state(query, random_scores, 0, self.db_eng, self.db_cursor)
        self.memory_ras[0][query.name] = random_state

        return rtables

    def search_pair_tables_combined(self, sid, eid, old_results, new_sorted_list, possible_views_dictionary, \
                           possible_views_lower_bound, possible_views_upper_bound, col_ids, topk):
        #logging.info("orginal first:")
        #logging.info(possible_views_upper_bound)
        visited = {0:{}, 1:{}}

        # there is only one input single stream
        sorted_access_read = {}
        sorted_access_read[1] = {}

        # score of the single new list
        for tuple in new_sorted_list:
            sorted_access_read[1][tuple[0]] = tuple[1]

        # score of the combined new lists
        update_tables = {0:{}, 1:{}}
        for tid, tuple in enumerate(old_results):

            if tid < col_ids[sid]:
                continue

            for table_id in tuple[0]:
                if table_id not in update_tables[0]:
                    update_tables[0][table_id] = []
                update_tables[0][table_id].append(tid)

        if len(possible_views_lower_bound) != 0:
            new_possible_views_lower_bound = self.update_left_combined(old_results, update_tables[0], possible_views_lower_bound)
        else:
            new_possible_views_lower_bound = []

        if len(possible_views_upper_bound) != 0:
            new_possible_views_upper_bound = self.update_left_combined(old_results, update_tables[0], possible_views_upper_bound)
        else:
            new_possible_views_upper_bound = []

        #logging.info("new here")
        #logging.info(new_possible_views_upper_bound)
        for tid, tuple in enumerate(new_sorted_list):
            if tid < col_ids[eid]:
                continue
            update_tables[1][tuple[0]] = tuple[1]

        if len(new_possible_views_lower_bound) != 0:
            new_possible_views_lower_bound = self.update_right_single(update_tables[1], new_possible_views_lower_bound)
        if len(new_possible_views_upper_bound) != 0:
            new_possible_views_upper_bound = self.update_right_single(update_tables[1], new_possible_views_upper_bound)

        for tid, tuple in enumerate(new_possible_views_upper_bound):
            triple_id1 = tuple[0]
            triple_id2 = tuple[1]
            triple_id3 = tuple[2]

            temp_bounds = tuple[3]
            temp_bounds_flag = tuple[4]

            if not temp_bounds_flag[0]:
                temp_bounds[0] = old_results[-1][1]
            if not temp_bounds_flag[1]:
                temp_bounds[1] = new_sorted_list[-1][1]

            new_possible_views_upper_bound[tid] = [triple_id1, triple_id2, triple_id3, temp_bounds, temp_bounds_flag]

        if len(new_possible_views_lower_bound) > topk:
            old_new_possible_views_lower_bound = new_possible_views_lower_bound
            old_new_possible_views_upper_bound = new_possible_views_upper_bound

            new_possible_views_lower_bound, new_possible_views_upper_bound = self.topk_stop_condition(new_possible_views_lower_bound, \
                                                                new_possible_views_upper_bound, topk, 3)
            if new_possible_views_lower_bound != None:
                return possible_views_dictionary, new_possible_views_lower_bound, new_possible_views_upper_bound, col_ids, True
            else:
                new_possible_views_upper_bound = old_new_possible_views_upper_bound
                new_possible_views_lower_bound = old_new_possible_views_lower_bound

        possible_views_lower_bound = new_possible_views_lower_bound
        possible_views_upper_bound = new_possible_views_upper_bound
        #print(possible_views_lower_bound)
        #print(possible_views_upper_bound)
        while(True):

            if col_ids[sid] >= len(old_results) or col_ids[eid] >= len(new_sorted_list):
                break

            table_ids1 = old_results[col_ids[sid]][0]
            table_ids1_str = ""

            joinable_tables = {}
            joinable_tables_oids = {}
            for tid, table_id in enumerate(table_ids1):
                # To be modified
                #schema_map_partial, _, _ = self.search_tables_from_database(self.real_tables[table_id], SM_test)
                # no score in schema_map_partial in current implementation
                if table_id in self.reverse_table_single_profile:
                    joinable_tables[table_id] = self.reverse_table_single_profile[table_id]
                    joinable_tables_oids[table_id] = tid
                    if table_ids1_str != "":
                        table_ids1_str = table_ids1_str + "&" + str(table_id)
                    else:
                        table_ids1_str = str(table_id)


            if table_ids1_str not in visited[0]:
                visited[0][table_ids1_str] = {}

            for tid2, table_id2 in enumerate(joinable_tables.keys()):
                for tid3, table_id3 in enumerate(joinable_tables[table_id2].keys()):

                    if str(table_id2) + "#sep#" + str(table_id3) in visited[0][table_ids1_str]:
                         continue

                    visited[0][table_ids1_str][str(table_id2) + "#sep#" + str(table_id3)] = True

                    possible_view = table_ids1_str + "&" + str(table_id3)

                    if possible_view in possible_views_dictionary:
                        continue

                    lower_bound = [old_results[col_ids[sid]][1]]
                    lower_bound_flag = [True]
                    upper_bound = [old_results[col_ids[sid]][1]]
                    upper_bound_flag = [True]

                    if table_id3 in sorted_access_read[1]:
                        lower_bound += [sorted_access_read[1][table_id3]]
                        lower_bound_flag += [True]
                        upper_bound += [sorted_access_read[1][table_id3]]
                        upper_bound_flag += [True]
                    else:
                        lower_bound += [0]
                        lower_bound_flag += [False]
                        upper_bound += [new_sorted_list[-1][1]]
                        upper_bound_flag += [False]

                    joinscore = max([t[2] for t in joinable_tables[table_id2][table_id3]])


                    lower_bound += [joinscore]
                    lower_bound_flag += [True]
                    upper_bound += [joinscore]
                    upper_bound_flag += [True]

                    if possible_view not in possible_views_dictionary:
                        possible_views_dictionary[possible_view] = ""
                        possible_views_lower_bound.append([table_ids1_str, table_id2, table_id3, lower_bound, lower_bound_flag])
                        possible_views_upper_bound.append([table_ids1_str, table_id2, table_id3, upper_bound, upper_bound_flag])

            table_id1 = new_sorted_list[col_ids[1]][0]
            #To be modified
            #schema_map_partial, _, _ = self.search_tables_from_database(self.real_tables[table_id1], SM_test)
            # no score in schema_map_partial in current implementation
            if table_id1 in self.reverse_table_single_profile:
                joinable_tables = self.reverse_table_single_profile[table_id1]

                if table_id1 not in visited[1]:
                    visited[1][table_id1] = {}

                for table_id2 in joinable_tables.keys():

                    if table_id2 in visited[1][table_id1]:
                        continue

                    visited[1][table_id1][table_id2] = True

                    r_flag = False
                    possible_view_tuples = []
                    joinable_tables_oids = {}
                    for tid, tuple in enumerate(old_results):
                        table_ids = tuple[0]
                        if table_id2 in table_ids:
                            r_flag = True
                            possible_view_tuples.append(tuple)
                            joinable_tables_oids[tid] = table_ids.index(table_id2)

                    for tid, tuple in enumerate(old_results):
                        if tuple in possible_view_tuples:

                            table_ids = tuple[0]
                            table_ids1_str = ""
                            for t_tid, t_table_id in enumerate(table_ids):
                                if table_ids1_str == "":
                                    table_ids1_str += str(t_table_id)
                                else:
                                    table_ids1_str += "&" + str(t_table_id)

                            possible_view = table_ids1_str + "&" + str(table_id1)

                            if possible_view in possible_views_dictionary:
                                continue

                            lower_bound = [tuple[1]]
                            lower_bound_flag = [True]
                            upper_bound = [tuple[1]]
                            upper_bound_flag = [True]

                            lower_bound += [new_sorted_list[col_ids[eid]][1]]
                            lower_bound_flag += [True]
                            upper_bound += [new_sorted_list[col_ids[eid]][1]]
                            upper_bound_flag += [True]

                            joinscore = max([t[2] for t in joinable_tables[table_id2]])
                            #logging.info(joinable_tables[table_id2])
                            #logging.info("here2")
                            #logging.info(joinscore)

                            lower_bound += [joinscore]
                            lower_bound_flag += [True]
                            upper_bound += [joinscore]
                            upper_bound_flag += [True]

                            if possible_view not in possible_views_dictionary:
                                possible_views_dictionary[possible_view] = ""
                                possible_views_lower_bound.append([table_ids1_str, table_id2, table_id1, lower_bound, lower_bound_flag])
                                possible_views_upper_bound.append([table_ids1_str, table_id2, table_id1, upper_bound, upper_bound_flag])

                    if not r_flag:
                        table_ids1_str = "incomplete"
                        possible_view = table_ids1_str + "&" + str(table_id2) + "#sep#" + str(table_id1)
                        if possible_view in possible_views_dictionary:
                            continue

                        lower_bound = [0]
                        lower_bound_flag = [False]
                        upper_bound = [old_results[-1][1]]
                        upper_bound_flag = [False]

                        lower_bound += [new_sorted_list[col_ids[eid]][1]]
                        lower_bound_flag += [True]
                        upper_bound += [new_sorted_list[col_ids[eid]][1]]
                        upper_bound_flag += [True]

                        joinscore = max([t[2] for t in joinable_tables[table_id2]])

                        lower_bound += [joinscore]
                        lower_bound_flag += [True]
                        upper_bound += [joinscore]
                        upper_bound_flag += [True]

                        if possible_view not in possible_views_dictionary:
                            possible_views_dictionary[possible_view] = ""
                            possible_views_lower_bound.append([table_ids1_str, table_id2, table_id1, lower_bound, lower_bound_flag])
                            possible_views_upper_bound.append([table_ids1_str, table_id2, table_id1, upper_bound, upper_bound_flag])

            if len(possible_views_lower_bound) > topk:
                old_possible_views_lower_bound = possible_views_lower_bound
                old_possible_views_upper_bound = possible_views_upper_bound

                possible_views_lower_bound, possible_views_upper_bound = self.topk_stop_condition(
                    possible_views_lower_bound, \
                    possible_views_upper_bound, topk, 3)
                if possible_views_lower_bound != None:
                    return possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound, col_ids, True
                else:
                    col_ids[sid] += 1
                    col_ids[eid] += 1
                    possible_views_upper_bound = old_possible_views_upper_bound
                    possible_views_lower_bound = old_possible_views_lower_bound

            else:
                col_ids[sid] += 1
                col_ids[eid] += 1

        return possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound, col_ids, False

    def search_pair_tables(self, sid, eid, new_sorted_lists, possible_views_dictionary, possible_views_lower_bound, \
                           possible_views_upper_bound, col_ids, topk):

        # always come with two streams
        visited = {0:{}, 1:{}}
        sorted_access_read = {0:{}, 1:{}}

        #new_sorted_lists: all base streams read
        for qid in new_sorted_lists.keys():
            if qid == sid:
                for tuple in new_sorted_lists[qid]:
                    sorted_access_read[0][tuple[0]] = tuple[1]
            elif qid == eid:
                for tuple in new_sorted_lists[qid]:
                    sorted_access_read[1][tuple[0]] = tuple[1]

        sub_queries = [sid, eid]

        # READ DATA FROM SORTED ACCESSED LISTS (streams)
        updated_tables = {}
        for qid, q_tid in enumerate(sub_queries):
            updated_tables[qid] = {}
            for tid, tuple in enumerate(new_sorted_lists[q_tid]):
                if tid < col_ids[q_tid]:
                    continue
                updated_tables[qid][tuple[0]] = tuple[1]

        # UPDATE LOWER BOUND AND UPPER BOUND GIVEN NEW READ DATA
        for tid, tuple in enumerate(possible_views_lower_bound):
            pair_id1 = tuple[0]
            pair_id2 = tuple[1]
            temp_lower_bounds = tuple[2]
            temp_lower_bounds_flag = tuple[3]

            if pair_id1 in updated_tables[0]:
                if not temp_lower_bounds_flag[0]:
                    temp_lower_bounds[0] = updated_tables[0][pair_id1]
                    temp_lower_bounds_flag[0] = True

            if pair_id2 in updated_tables[1]:
                if not temp_lower_bounds_flag[1]:
                    temp_lower_bounds[1] = updated_tables[1][pair_id2]
                    temp_lower_bounds_flag[1] = True

            possible_views_lower_bound[tid] = [pair_id1, pair_id2, temp_lower_bounds, temp_lower_bounds_flag]

        for tid, tuple in enumerate(possible_views_upper_bound):
            pair_id1 = tuple[0]
            pair_id2 = tuple[1]
            temp_upper_bounds = tuple[2]
            temp_upper_bounds_flag = tuple[3]

            if pair_id1 in updated_tables[0]:
                if not temp_upper_bounds_flag[0]:
                    temp_upper_bounds[0] = updated_tables[0][pair_id1]
                    temp_upper_bounds_flag[0] = True
            else:
                if not temp_upper_bounds_flag[0]:
                    temp_upper_bounds[0] = new_sorted_lists[0][-1][1]

            if pair_id2 in updated_tables[1]:
                if not temp_upper_bounds_flag[1]:
                    temp_upper_bounds[1] = updated_tables[1][pair_id2]
                    temp_upper_bounds_flag[1] = True
            else:
                if not temp_upper_bounds_flag[1]:
                    temp_upper_bounds[1] = new_sorted_lists[1][-1][1]

            possible_views_upper_bound[tid] = [pair_id1, pair_id2, temp_upper_bounds, temp_upper_bounds_flag]

        if len(possible_views_lower_bound) > topk:
            old_possible_views_lower_bound = possible_views_lower_bound
            old_possible_views_upper_bound = possible_views_upper_bound

            possible_views_lower_bound, possible_views_upper_bound = self.topk_stop_condition(possible_views_lower_bound, \
                                                                possible_views_upper_bound, topk, 2)
            if possible_views_lower_bound:
                return possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound, col_ids, True
            else:
                possible_views_lower_bound = old_possible_views_lower_bound
                possible_views_upper_bound = old_possible_views_upper_bound

        #logging.info("here1")

        while(True):
            #logging.info(sorted_access_read)

            if col_ids[sid] >= len(sorted_access_read[0].items()) or col_ids[eid] >= len(sorted_access_read[1].items()):
                break

            qid1 = 0
            qid2 = 1

            if col_ids[sid] < len(sorted_access_read[0].items()):
                possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound, visited = \
                    self.update_pair_columns([qid1, qid2], [sid, eid], visited, col_ids, new_sorted_lists, sorted_access_read, \
                              possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound)
            if col_ids[eid] < len(sorted_access_read[1].items()):
                possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound, visited = \
                    self.update_pair_columns([qid2, qid1], [eid, sid], visited, col_ids, new_sorted_lists, sorted_access_read, \
                                possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound)

            #logging.info(possible_views_lower_bound)

            if len(possible_views_lower_bound) > topk:
                old_possible_views_lower_bound = possible_views_lower_bound
                old_possible_views_upper_bound = possible_views_upper_bound
                possible_views_lower_bound, possible_views_upper_bound = self.topk_stop_condition(possible_views_lower_bound, \
                                                                possible_views_upper_bound, topk, 2)

               # logging.info(possible_views_lower_bound)
                if possible_views_lower_bound:
                    return possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound, col_ids, True
                else:
                    col_ids[sid] += 1
                    col_ids[eid] += 1
                    possible_views_lower_bound = old_possible_views_lower_bound
                    possible_views_upper_bound = old_possible_views_upper_bound
            else:
                col_ids[sid] += 1
                col_ids[eid] += 1

        return possible_views_dictionary, possible_views_lower_bound, possible_views_upper_bound, col_ids, False

    def search_initialization(self, queries, sketch_flag):
        self.mapping_for_view_testing = {}
        self.table_cols_storage = {}
        self.table_col_num = {}
        self.mapping_for_view_testing, time1, time2 = self.get_mapping_from_profile(queries, profile_flg=sketch_flag)
        return time1, time2

    def search_multiple_tables(self, queries, return_top_num):

        #self.real_table_sketches = self.load_sketches_for_view_testing(False)
        # self.mapping_for_view_testing = self.schema_mapping_for_view_testing(queries)

        #################### To Do ######################
        #self.joinable_tables_all = self.joinable_columns_for_view_testing(store_flag=False)
        #################################################
        self.sort_time = 0

        self.sorted_query_list_for_view_testing = {}
        start_time = timeit.default_timer()
        topks = {} # recording the batches for each column (goal)
        round_cnt = {} # recording the batches for each column (real)
        topk_read = {} # how many rows have been read from each stream
        col_ids = {} # how may rows have been read by the algorithm
        topk_key = {}

        possible_lower_bounds = {}
        possible_upper_bounds = {}
        possible_view_dictionaries = {}

        for qid in range(len(queries)):

            topk_read[qid] = []
            topk_key[qid] = []
            col_ids[qid] = {}
            for qqid in range(len(queries)):
                col_ids[qid][qqid] = 0

            topks[qid] = 1
            round_cnt[qid] = 1

            possible_lower_bounds[qid] = []
            possible_upper_bounds[qid] = []
            possible_view_dictionaries[qid] = {}

        last_results = []
        last_last_results = []
        success_flag = False

        iter_num = 0
        while(True):

            if iter_num % 10 == 0:
                logging.info(f"Iter: {iter_num}")
            #for qid in possible_lower_bounds.keys():
            #    logging.info(possible_lower_bounds[qid][:5])
            #logging.info("Bounds Output Finished.")
            #logging.info(queries)
            #logging.info("info ended.")

            for qid, query in enumerate(queries):

                if qid == len(queries) - 1:
                    break

                if qid == 0:

                    old_len0 = len(topk_read[qid])
                    old_len1 = len(topk_read[qid + 1])

                    topk_read[qid] = self.search_unionable_data_for_table(query, round_cnt[qid] * self.window_size)
                    topk_read[qid + 1] = self.search_unionable_data_for_table(queries[qid + 1], round_cnt[qid] * self.window_size)

                    #if iter_num == 0:
                    #logging.info(f'Found {len(topk_read[qid])} related tables for Table Q{qid}')
                    #logging.info(f'Found {len(topk_read[qid + 1])} related tables for Table Q{qid + 1}')

                    if len(topk_read[qid]) == 0:
                        return None
                    if len(topk_read[qid + 1]) == 0:
                        return None

                    # If there is no more input from source streams
                    if len(topk_read[qid]) - old_len0 + len(topk_read[qid + 1]) - old_len1 == 0:

                        last_results = []

                        for tid, tuple in enumerate(possible_lower_bounds[qid]):
                            if tid > topks[qid] * self.window_size - 1:
                                break
                            last_results.append([[tuple[0], tuple[1]], np.sum(tuple[2])])

                        if qid == len(queries) - 2:
                            success_flag = True
                            break
                        #last_last_results = last_results
                        continue

                    if qid == len(queries) - 2:
                        possible_view_dictionary, possible_view_lower_bound, possible_view_upper_bound, col_id, success = \
                            self.search_pair_tables(qid, qid + 1, topk_read, possible_view_dictionaries[qid],
                                                    possible_lower_bounds[qid], possible_upper_bounds[qid],
                                                    col_ids[qid], return_top_num)
                    else:
                        possible_view_dictionary, possible_view_lower_bound, possible_view_upper_bound, col_id, success = \
                            self.search_pair_tables(qid, qid + 1, topk_read, possible_view_dictionaries[qid], possible_lower_bounds[qid], possible_upper_bounds[qid], col_ids[qid], topks[qid] * self.window_size)

                    possible_view_dictionaries[qid] = possible_view_dictionary
                    possible_lower_bounds[qid] = possible_view_lower_bound
                    possible_upper_bounds[qid] = possible_view_upper_bound
                    col_ids[qid] = col_id

                    #logging.info(f"flag: {success}")
                    #logging.info(len(possible_view_lower_bound))
                    #logging.info(possible_view_lower_bound[:5])

                    if success:
                        last_results = []
                        for tid, tuple in enumerate(possible_view_lower_bound):
                            if tid > topks[qid] * self.window_size - 1:
                                break
                            last_results.append([[tuple[0], tuple[1]], np.sum(tuple[2])])
                        #logging.info(f'{len(last_results)} combinations for Q{qid} and Q{qid + 1} detected!')

                        if qid == len(queries) - 2:
                            success_flag = True
                            break

                        #last_last_results = last_results
                        #print(last_results)
                    else:
                        round_cnt[qid] += 1
                        break

                else:
                   # logging.info("#####last_results#####")
                   # logging.info(last_results)

                    old_len1 = len(topk_read[qid + 1])
                    topk_read[qid + 1] = self.search_unionable_data_for_table(queries[qid + 1], round_cnt[qid + 1] * self.window_size)

                    #if iter_num == 0:
                    #logging.info(f'Found {len(topk_read[qid + 1])} related tables for Table Q{qid + 1}')

                    if len(topk_read[qid + 1]) <= return_top_num:
                       if qid > 1:
                           end_time = timeit.default_timer()
                           logging.info(f'Not Succeed: {success_flag}')
                           logging.info(end_time - start_time)
                           return end_time - start_time - self.sort_time

                    old_len0 = len(last_last_results)
                    #logging.info(topk_read[qid + 1])

                    if len(last_results) - old_len0 + len(topk_read[qid + 1]) - old_len1 == 0:

                        last_results = []

                        for tid, tuple in enumerate(possible_lower_bounds[qid]):
                            if tid > topks[qid] * self.window_size - 1:
                                break
                            last_results.append([[tuple[0], tuple[2]], np.sum(tuple[3])])

                        if qid == len(queries) - 2:
                            success_flag = True
                            break

                        continue

                    last_last_results = last_results

                    if qid == len(queries) - 2:
                        possible_view_dictionary, possible_view_lower_bound, possible_view_upper_bound, col_id, success = \
                            self.search_pair_tables_combined(qid, qid + 1, last_results, topk_read[qid + 1],
                                                             possible_view_dictionaries[qid], \
                                                             possible_lower_bounds[qid], possible_upper_bounds[qid],
                                                             col_ids[qid], return_top_num)
                    else:
                        possible_view_dictionary, possible_view_lower_bound, possible_view_upper_bound, col_id, success = \
                            self.search_pair_tables_combined(qid, qid + 1, last_results, topk_read[qid + 1], possible_view_dictionaries[qid], \
                                                    possible_lower_bounds[qid], possible_upper_bounds[qid], col_ids[qid], \
                                                    topks[qid] * self.window_size)

                    possible_view_dictionaries[qid] = possible_view_dictionary
                    possible_lower_bounds[qid] = possible_view_lower_bound
                    possible_upper_bounds[qid] = possible_view_upper_bound
                    col_ids[qid] = col_id

                    #logging.info("here:")
                    logging.info(f"flag: {success}")
                    #logging.info(possible_view_lower_bound[:5])
                    #logging.info(success)

                    if success:
                        last_results = []
                        for tid, tuple in enumerate(possible_view_lower_bound):
                            if tid > topks[qid] * self.window_size - 1:
                                break
                            #print(tuple)
                            last_results.append([[tuple[0], tuple[2]], np.sum(tuple[3])])

                        #last_last_results = last_results
                        if qid == len(queries) - 2:
                            success_flag = True
                            break
                    else:
                        for qqid in range(qid + 1):
                            round_cnt[qqid] += 1
                            topks[qqid] += 1
                        round_cnt[qid + 1] += 1

                        break

            iter_num  = iter_num + 1
            if success_flag:
                #last_results = []
                #for tid, tuple in enumerate(possible_view_lower_bound):
                #    if tid > topks[qid] * self.window_size - 1:
                #        break
                    # print(tuple)
                #    last_results.append([[tuple[0], tuple[2]], np.sum(tuple[3])])

                break

        end_time = timeit.default_timer()
        logging.info(f'Succeed: {success_flag}')
        logging.info(end_time - start_time)
        logging.info(f"Sorted Time: {self.sort_time}")
        #self.save_sketches()
        print(last_results)
        return end_time - start_time - self.sort_time

        #print(last_results)

    #
    # def multiple_table_union_score(self, tables):
    #
    #     join_scores = 0
    #     for tid in range(len(tables)):
    #
    #         if tid + 1 == len(tables):
    #             break
    #
    #         table1 = tables[tid]
    #         if table1 not in self.reverse_table_single_profile:
    #             continue
    #         table2 = tables[tid + 1]
    #         if table2 not in self.reverse_table_single_profile[tables[tid]]:
    #             continue
    #         join_score = min([t[2] for t in self.reverse_table_single_profile[table1][table2]])
    #         join_scores += join_score
    #
    #     return join_scores

    def multiple_table_union_score(self, tables):

        join_scores = 0
        for tid1 in range(len(tables)):
            for tid2 in range(len(tables)):
                table1 = tables[tid1]
                if table1 not in self.reverse_table_single_profile:
                    continue
                table2 = tables[tid2]
                if table2 not in self.reverse_table_single_profile[tables[tid1]]:
                    continue
                join_score = min([t[2] for t in self.reverse_table_single_profile[table1][table2]])
                join_scores += join_score
        return join_scores

    def baseline_two_loops(self, queries, return_top_num, times):
        self.sorted_query_list_for_view_testing = {}
        start_time = timeit.default_timer()
        batch_size = times * return_top_num
        from itertools import permutations

        single_results = []
        for query in queries:
            result = self.search_unionable_data_for_table(query, batch_size)
            #self.search_unionable_data_for_table_sketch(query, batch_size)
            r_len = len(result)
            if r_len == 0:
                single_results.append([])
                continue
            if r_len < batch_size:
                for i in range(batch_size - r_len):
                    result.append(result[-1])
            single_results.append(result)
        all_lens = [len(t) for t in single_results]
        logging.info("lens of all lists:")
        logging.info(single_results)
        logging.info(all_lens)
        perms = permutations(list(range(batch_size)), len(queries))
        perm_score = []
        for perm in list(perms):
            score = 0
            tables = []
            for lid, list_loc_id in enumerate(perm):
                if list_loc_id >= len(single_results[lid]):
                    continue
                score += single_results[lid][list_loc_id][1]
                tables.append(single_results[lid][list_loc_id][0])
            score +=self.multiple_table_union_score(tables)
            perm_score.append([perm, score])

        perm_score = sorted(perm_score, key = lambda d:d[1], reverse=True)[:return_top_num]
        for tuple in perm_score:
            for llid, lid in enumerate(tuple[0]):
                try:
                    logging.info(single_results[llid][lid])
                except:
                    pass
            logging.info("\n")

        logging.info("******end*****")
        logging.info(perm_score)
        end_time = timeit.default_timer()
        logging.info(end_time - start_time)

        return end_time - start_time
    # def baseline_join(self):
    #
    #
    #





