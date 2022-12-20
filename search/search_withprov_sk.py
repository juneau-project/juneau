import json
import logging
import sys
import timeit

import networkx as nx
import numpy as np
import pandas as pd
import psycopg2
from py2neo import NodeMatcher
from sqlalchemy import create_engine

import data_extension.config as cfg

from data_extension.search.Query import Query
from data_extension.search.Random import Random_Components
from data_extension.search.Sorted import Sorted_Components
from data_extension.search.search_sketch import SearchSketch
from data_extension.search_ks import KS
from data_extension.search.search_tables import SearchTables
from data_extension.table_db import pre_vars

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class WithProv_Cached(SearchTables):

    special_type = ['np', 'pd']

    def __connect2db_init(self):
        # Define our connection string
        conn_string = "host=" + cfg.sql_host + " dbname=\'" + cfg.sql_dbname + "\' user=\'" + cfg.sql_name + \
                      "\' password=\'" + cfg.sql_password + "\'"

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

    def __read_tables(self, names):

        conn = self.eng.connect()

        return_tables = {}

        for name in names:

            if name[:6] != 'rtable':
                name = 'rtable' + name

            if name in self.all_tables_read:
                return_tables[name] = self.all_tables_read[name]
                continue

            try:
                table_r = pd.read_sql_table(name, conn, schema=cfg.sql_dbs)
                if 'Unnamed: 0' in table_r.columns:
                    table_r.drop(['Unnamed: 0'], axis=1, inplace=True)
                self.all_tables_read[name] = table_r
                return_tables[name] = table_r

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

        return return_tables

    def approximate_join_key(self, tableA, tableB, SM, key, thres_prune):

        kyA = key
        key_value_A = tableA[key].tolist()
        scoreA = float(len(set(key_value_A))) / float(len(key_value_A))
        if scoreA == 1:
            return 1

        kyB = SM[key]
        key_value_B = tableB[kyB].tolist()
        scoreB = float(len(set(key_value_B))) / float(len(key_value_B))
        if scoreB == 1:
            return 1

        if min(scoreA, scoreB) < thres_prune:
            return 0

        count_valueA = []
        for v in key_value_A:
            if v in key_value_B:
                count_valueA.append(v)

        count_valueB = []
        for v in key_value_B:
            if v in key_value_A:
                count_valueB.append(v)

        key_scoreAB = float(len(set(count_valueA)))/float(len(count_valueA))
        key_scoreBA = float(len(set(count_valueB)))/float(len(count_valueB))

        return max(key_scoreAB, key_scoreBA)

    def read_graph_of_notebook(self):
        Graphs = {}
        conn = self.eng.connect()
        try:
            dependency = pd.read_sql_table('dependen', conn, schema = cfg.sql_graph)#, schema='graph_model')
            line2cid = pd.read_sql_table('line2cid', conn, schema = cfg.sql_graph)#, schema='graph_model')
            lastliid = pd.read_sql_table('lastliid', conn, schema = cfg.sql_graph)
            conn.close()
        except:
            conn.close()
        finally:
            if conn:
                conn.close()

        dependency_store = {}
        line2cid_store = {}
        lastliid_store = {}

        for index, row in dependency.iterrows():
            dependency_store[row['view_id']] = json.loads(row['view_cmd'])

        for index, row in line2cid.iterrows():
            line2cid_store[row['view_id']] = json.loads(row['view_cmd'])

        for index, row in lastliid.iterrows():
            lastliid_store[row['view_id']] = json.loads(row['view_cmd'])

        error_cnt = 0
        for nid in dependency_store.keys():
            try:
                line_id = lastliid_store[nid]
                nid_name = nid.split("_")[-1]
                Graph = self.__generate_graph(nid_name, dependency_store[nid], line2cid_store[nid])

                var_name = "_".join(nid.split("_")[1:-1])
                query_name = 'var_' + var_name + '_' + str(line2cid_store[nid][str(line_id)]) + "_" + str(nid_name)
                query_node = pre_vars(query_name, Graph)
                Graphs[nid] = query_node
            except:
                error_cnt = error_cnt + 1
                #logging.error("Can not generate the graph!!! " + str(sys.exc_info()))

        logging.error(str(error_cnt) + " Tables have Error of Provenance!")

        return Graphs, line2cid_store

    def __generate_graph(self, nid, dependency, line2cid):
        G = nx.DiGraph()
        for i in dependency.keys():
            left = dependency[i][0]

            pair_dict = {}
            right = []
            for pa, pb in dependency[i][1]:
                if pa + ';' + pb not in pair_dict:
                    pair_dict[pa + ';' + pb] = 0
                    right.append([pa, pb])


            left_node = []
            for ele in left:
                if type(ele) is tuple or type(ele) is list:
                    ele = ele[0]
                left_node.append('var_' + ele + '_' + str(line2cid[i]) + '_' + str(nid))


            for ele in left:
                if type(ele) is tuple or type(ele) is list:
                    ele = ele[0]

                new_node = 'var_' + ele + '_' + str(line2cid[i]) + '_' + str(nid)
                G.add_node(new_node, cell_id=line2cid[i], line_id=i, var=ele)
                # print(nbname)
                # print(right)
                for dep, ename in right:
                    candidate_list = G.nodes
                    rankbyline = []
                    for cand in candidate_list:
                        # print('cand', cand)

                        if G.nodes[cand]['var'] == dep:
                            if cand in left_node:
                                # print(cand)
                                continue
                            rankbyline.append((cand, int(G.nodes[cand]['line_id'])))
                    rankbyline = sorted(rankbyline, key=lambda d: d[1], reverse=True)

                    if len(rankbyline) == 0:
                        if dep not in self.special_type:
                            candidate_node = 'var_' + dep + '_*_' + str(nid)
                            G.add_node(candidate_node, cell_id=0, line_id=0, var=dep)
                        else:
                            candidate_node = 'sep_' + dep + '_' + str(nid)
                            G.add_node(candidate_node, cell_id=0, line_id=0, var=dep)

                    else:
                        candidate_node = rankbyline[0][0]

                    G.add_edge(new_node, candidate_node, label=ename)
        #print(G.nodes)
        return G

    def init_schema_mapping(self):

        matcher = NodeMatcher(self.geng)

        tables_touched = []
        tables_connected = []
        logging.info("Init Schema Mapping: ")

        for i in self.tables:
            print(i)
            if i[6:] not in set(tables_touched):
                #logging.info(i)
                current_node = matcher.match("Var", name = i[6:]).first()
                connected_tables = super().dfs(current_node)
                print(connected_tables)

                if i[6:] not in connected_tables:
                    connected_tables.append(i[6:])

                tables_touched = tables_touched + connected_tables
                tables_connected.append(connected_tables)

        self.schema_linking = {}
        self.schema_element = {}
        self.schema_element_count = {}
        self.schema_element_dtype = {}

        self.table_group = {}
        self.table_name_group = {}

        # assign each table a group id
        for idi, i in enumerate(tables_connected):
            for j in i:
                self.table_group[j] = idi
                self.table_name_group[idi] = j

        #
        # for idi, i in enumerate(tables_connected):
        #     self.schema_linking[idi] = {}
        #     self.schema_element[idi] = {}
        #     self.schema_element_dtype[idi] = {}
        #     self.schema_element_count[idi] = {}
        #
        #     for j in i:
        #         tname = 'rtable' + j
        #
        #         if tname not in self.real_tables:
        #             continue
        #         for col in self.real_tables[tname].columns:
        #             if col not in self.schema_linking[idi]:
        #                 if len(self.schema_linking[idi].keys()) == 0:
        #                     sid = 0
        #                 else:
        #                     sid = max(list(self.schema_linking[idi].values())) + 1
        #
        #                 self.schema_linking[idi][col] = sid
        #                 self.schema_element_dtype[idi][col] = self.real_tables[tname][col].dtype
        #                 self.schema_element_count[idi][col] = 1
        #                 self.schema_element[idi][col] = []
        #                 self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()
        #                 self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
        #             else:
        #                 self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()
        #                 self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
        #                 self.schema_element_count[idi][col] += 1

        logging.info('There are %s groups of tables.'%len(tables_connected))

    def __init__(self, dbname = cfg.sql_dbname, schema = cfg.sql_dbs, clear_cache = False):

        super().__init__(dbname, schema, read_flag=False)
        self.index()

        self.db_eng = self.eng
        self.db_cursor = self.__connect2db_init()
        self.initialize_in_database_search_class()

        self.all_tables_read = {}
        self.real_tables = {}
        self.initialize_for_topk()
        self.initialize_memory_states(clear_cache)

        logging.info('Data Search Extension Prepared!')

    def index(self):
        logging.info('Reading Graph of Notebooks.')
        self.Graphs, self.n_l2cid = self.read_graph_of_notebook()

    def initialize_in_database_search_class(self):
        self.search_lshe = SearchSketch(self.db_eng)
        self.search_ks = KS(self.db_eng)

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
            query1 = "DROP SCHEMA IF EXISTS topk_ra_states CASCADE;"
            query2 = "CREATE SCHEMA IF NOT EXISTS topk_ra_states;"
            query3 = "DROP SCHEMA IF EXISTS topk_sa_states CASCADE;"
            query4 = "CREATE SCHEMA IF NOT EXISTS topk_sa_states;"

            conn = self.db_eng.connect()

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

    def search_tables_from_database(self, query, SM_test):

        tableA = query.value
        mapping = {}
        for col in tableA.columns:
            #print(col, tableA[col].dtype)
            if tableA[col].dtype == 'object':
                c_table = self.search_lshe.query_cols([{'schema':cfg.sql_dbs, 'table':query.name, 'col':col}])[0]
                if len(c_table['similar_domains']) > 0:
                    if col not in mapping:
                        mapping[col] = []
                    for res in c_table['similar_domains']:
                        mapping[col].append([res['table'], res['column']])
            elif tableA[col].dtype == 'int' or tableA[col].dtype == 'float':
                c_table = self.search_ks.query_cols([{'schema':cfg.sql_dbs, 'table':query.name, 'col':col}])[0]
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

        self.real_tables = self.__read_tables(all_tables)

        self.real_tables[query.name] = tableA
        schema_map_partial = SM_test.add_mapping_by_workflow(query.name, all_mapping, self.real_tables, self.table_group)

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

    def search_additional_training_data_other(self, SM_test, query_name, k, code, var_name, beta, theta, tflag):

        query = self.real_tables[query_name]

        #introduce the schema mapping class
        start_time = timeit.default_timer()
        schema_map_partial = {}
        #do partial schema mapping
        partial_mapping, _ = SM_test.mapping_to_columns_search(query, 2)
        #partial_mapping = {}
        logging.info("Searching: partial mapping")
        #logging.info(partial_mapping)

        #get groups
        connected_tables = []
        connected_tables_mapped_col = {}
        for pkey in partial_mapping.keys():
            for pt in partial_mapping[pkey]:
                connected_tables.append(pt[0][6:])
                if pt[0][6:] not in connected_tables_mapped_col:
                    connected_tables_mapped_col[pt[0][6:]] = pt[1]
                if pt[0][6:] not in schema_map_partial:
                    schema_map_partial[pt[0][6:]] = {}
                schema_map_partial[pt[0][6:]][pkey] = pt[1]

        connected_groups = []
        connected_groups.append(self.table_group[query_name[6:]])

        for ct in connected_tables:
            connected_groups.append(self.table_group[ct])
        connected_groups = list(set(connected_groups))

        #do search
        # for i in self.real_tables.keys():
        #
        #     if i == query_name:
        #         continue
        #
        #     tname = i[6:]
        #
        #     tableB = self.real_tables[i]
        #
        #     gid = self.table_group[tname]
        #
        #     if gid not in connected_groups:
        #         continue
        #     else:
        #         #if tname not in schema_map_partial:
        #         #    better_mapping = self.schema_mapping_float_prov(query, tableB, {})
        #         #else:
        #         #    better_mapping = self.schema_mapping_float_prov(query, tableB, schema_map_partial[tname])
        #
        #         #schema_map_partial[tname] = better_mapping

        logging.info("Searching: Longer Mapping Detected!")

        prov_class = SearchProv(self.Graphs)
        query_node = self.__generate_query_node_from_code(var_name, code)

        # Compute Provenance Similarity
        logging.info("Compute Provenance Similarity!")
        table_prov_rank = prov_class.search_score_rank(query_node)
        table_prov_score = {}

        for i, j in table_prov_rank:
            table_prov_score["rtable" + i] = j

        top_tables = []
        rank_candidate = []
        rank2 = []

        for i in self.real_tables.keys():

            if i == query_name:
                continue

            tname = i[6:]

            if i not in table_prov_score:
                prov_score = 0
            else:
                prov_score = float(1)/float(table_prov_score[i] + 1)

            tableA = query
            tableB = self.real_tables[i]

            if tname in schema_map_partial:
                SM = schema_map_partial[tname]
            else:
                SM = {}

            rank_candidate.append((i, prov_score, SM))
            upp_col_sim = float(min(tableA.shape[1], tableB.shape[1])) / float(max(tableA.shape[1], tableB.shape[1]))
            rank2.append(upp_col_sim)

        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)
        rank2 = sorted(rank2, reverse=True)

        if len(rank_candidate) == 0:
            if tflag:
                end_time = timeit.default_timer()
                return [], end_time - start_time
            else:
                return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableA = query
            tableB = self.real_tables[rank_candidate[i][0]]
            SM = rank_candidate[i][2]

            SM_full = SM_test.continue_full_mapping(tableA, tableB, SM)
            score = float(beta) * self.col_similarity(tableA, tableB, SM_full, 1) + float(1 - beta) * rank_candidate[i][1]
            top_tables.append((rank_candidate[i][0], score))

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):

            if ks + id >= len(rank_candidate):
                break

            threshold = float(beta) * rank2[ks + id] + float(1 - beta) * rank_candidate[ks + id][1]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableA = query
                tableB = self.real_tables[rank_candidate[ks + id][0]]
                SM = rank_candidate[ks + id][2]
                SM_full = SM_test.continue_full_mapping(tableA, tableB, SM)
                new_score = float(beta) * self.col_similarity(tableA, tableB, SM_full, 1) + float(1 - beta) * rank_candidate[ks + id][1]

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        #rtables_names = self.remove_dup2(top_tables, ks)

        cnt = 0
        rtables = []
        for i, j in top_tables:
            rtables.append(i)
            cnt = cnt + 1
            if cnt == k:
                break
        end_time = timeit.default_timer()
        if tflag:
            running_time = end_time - start_time
            return rtables, running_time
        else:
            return rtables

    def search_additional_training_data_test(self, SM_test, query_name, k, code, var_name, alpha, beta, theta, gflag, tflag):

        query = self.real_tables[query_name]

        logging.info("Searching: partial mapping")
        start_time = timeit.default_timer()
        start_time1 = timeit.default_timer()
        schema_map_partial = {}
        partial_mapping, candidate_keys, partial_mapping_cached = SM_test.mapping_to_columns_keys_search(query, 2)
        end_time_sep = timeit.default_timer()
        logging.info("Matching Profiles: " + str(end_time_sep - start_time1))

        #get groups
        connected_tables = []
        connected_tables_mapped_col = {}
        for pkey in partial_mapping.keys():
            for pt in partial_mapping[pkey]:
                connected_tables.append(pt[0][6:])
                if pt[0][6:] not in connected_tables_mapped_col:
                    connected_tables_mapped_col[pt[0][6:]] = pt[1]
                if pt[0][6:] not in schema_map_partial:
                    schema_map_partial[pt[0][6:]] = {}
                schema_map_partial[pt[0][6:]][pkey] = pt[1]

        if gflag == True:
            connected_groups = []
            connected_groups.append(self.table_group[query_name[6:]])

            for ct in connected_tables:
                connected_groups.append(self.table_group[ct])
            connected_groups = list(set(connected_groups))
        else:
            connected_groups = list(self.table_name_group.keys())


        #do search
        for i in self.real_tables.keys():

            if i == query_name:
                continue

            tname = i[6:]

            tableB = self.real_tables[i]

            gid = self.table_group[tname]

            if gid not in connected_groups:
                continue
            else:

                if tname not in schema_map_partial:
                    better_mapping = self.schema_mapping_float_prov(query, tableB, {})
                else:
                    better_mapping = self.schema_mapping_float_prov(query, tableB, schema_map_partial[tname])

                schema_map_partial[tname] = better_mapping

        logging.info("Searching: Longer Mapping Detected!")

        prov_class = SearchProv(self.Graphs)
        query_node = self.__generate_query_node_from_code(var_name, code)

        # Compute Provenance Similarity
        logging.info("Compute Provenance Similarity!")
        table_prov_rank = prov_class.search_score_rank(query_node)
        table_prov_score = {}

        for i, j in table_prov_rank:
            table_prov_score["rtable" + i] = j

        rank_candidate = []
        rank2 = []
        rank3 = {}

        for i in self.real_tables.keys():

            if i == query_name:
                continue

            tname = i[6:]

            if i not in table_prov_score:
                prov_score = 0
            else:
                prov_score = float(1 - sigmoid(table_prov_score[i]))

            tableA = query
            tableB = self.real_tables[i]

            if tname not in schema_map_partial:
                inital_mapping = {}
                new_data_rate = 0
            else:
                inital_mapping = schema_map_partial[tname]

                if len(candidate_keys) == 0:
                    new_data_rate = 1
                    #candidate = list(inital_mapping.keys())
                else:
                    candidate = candidate_keys
                    new_data_rate = 0
                    for ckey in candidate:
                        Alen = tableA[ckey].dropna().values
                        Blen = tableB[inital_mapping[ckey]].dropna().values
                        try:
                            new_data_rate_temp = float(1) - float(len(np.intersect1d(Alen, Blen)))/float(len(Alen))
                        except:
                            new_data_rate_temp = 0

                        if new_data_rate_temp > new_data_rate:
                            new_data_rate = new_data_rate_temp

            prov_score = alpha * prov_score  #+ float(beta) * self.col_similarity(tableA, tableB, inital_mapping, 1)

            rank_candidate.append((i, prov_score, inital_mapping))

            upp_col_sim = float(1 - alpha - beta) * new_data_rate + float(beta) * float(min(tableA.shape[1], tableB.shape[1])) / float(max(tableA.shape[1], tableB.shape[1]))

            rank2.append(upp_col_sim)

            rank3[i] = new_data_rate

        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)
        rank2 = sorted(rank2, reverse=True)
        end_time1 = timeit.default_timer()


        if len(rank_candidate) == 0:
            if tflag:
                end_time = timeit.default_timer()
                logging.info("Before Topk: " + str(end_time1 - start_time1))
                return [], end_time - start_time
            else:
                return []

        logging.info("Before Topk: " + str(end_time1 - start_time1))
        before_topk_time = end_time1 - start_time1
        timek = []
        rtablesk = []

        #print(rank_candidate[:15])

        for sk in k:

            #logging.info(sk)
            top_tables = []

            SM_test.cached_table_depend = {}

            start_time2 = timeit.default_timer()

            if len(rank_candidate) > sk:
                ks = sk
            else:
                ks = len(rank_candidate)

            for i in range(ks):
                tableA = query
                tableB = self.real_tables[rank_candidate[i][0]]
                tableB_name = rank_candidate[i][0]
                SM = rank_candidate[i][2]

                if len(candidate_keys) == 0:
                    candidates = list(SM.keys())
                    key_return, col_depend, _, _ = SM_test.detect_key_constraints(query_name, tableB_name, SM,
                                                                                  partial_mapping_cached, candidates,
                                                                                  tableA, tableB)
                else:
                    key_return = candidate_keys

                if len(key_return) != 0:

                    new_data_rate = 0
                    for ckey_return in key_return:
                        if ckey_return in SM:
                            Alen = tableA[ckey_return].dropna().values
                            Blen = tableB[SM[ckey_return]].dropna().values
                            try:
                                new_data_rate_temp = float(1) - float(len(np.intersect1d(Alen, Blen)))/float(len(Alen))
                            except:
                                new_data_rate_temp = 0
                        else:
                            new_data_rate_temp = 0

                        if new_data_rate_temp > new_data_rate:
                            new_data_rate = new_data_rate_temp
                else:
                    new_data_rate = 0

                if rank3[tableB_name] < new_data_rate:
                    new_data_rate = rank3[tableB_name]

                SM_full = SM #SM_test.continue_full_mapping(tableA, tableB, SM)

                score = float(beta) * self.col_similarity(tableA, tableB, SM_full, 1) + rank_candidate[i][1] + float(1 - beta - alpha) * new_data_rate

                top_tables.append((rank_candidate[i][0], score))

            top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
            min_value = top_tables[-1][1]

            ks = ks - 1
            id = 0
            while (True):

                if ks + id >= len(rank_candidate):
                    break

                threshold = rank2[ks + id] +  rank_candidate[ks + id][1]
                #logging.info("Threshold: " + str(threshold))
                #logging.info("Mini_Value: " + str(min_value))

                if threshold <= min_value * theta:
                    break
                else:
                    id = id + 1
                    if ks + id >= len(rank_candidate):
                        break

                    tableA = query
                    tableB = self.real_tables[rank_candidate[ks + id][0]]
                    tableB_name = rank_candidate[ks + id][0]
                    SM = rank_candidate[ks + id][2]

                    if len(candidate_keys) == 0:
                        candidate = list(SM.keys())
                        key_return, col_depend, _, _ = SM_test.detect_key_constraints(query_name, tableB_name, SM,
                                                                                  partial_mapping_cached,
                                                                                  candidate,
                                                                                  tableA, tableB)
                    else:
                        key_return = candidate_keys
                    #key_return, col_depend, _, _ = SM_test.detect_key_constraints(query_name, tableB_name, SM,
                    #                                                              partial_mapping_cached,
                    #                                                              candidate_keys, tableA, tableB)
                    if len(key_return) != 0:
                        new_data_rate = 0
                        for ckey_return in key_return:
                            if ckey_return in SM:
                                Alen = tableA[ckey_return].dropna().values
                                Blen = tableB[SM[ckey_return]].dropna().values

                                try:
                                    new_data_rate_temp = float(1) - float(len(np.intersect1d(Alen, Blen))) / float(len(Alen))
                                except:
                                    new_data_rate_temp = 0

                                if new_data_rate_temp > new_data_rate:
                                    new_data_rate = new_data_rate_temp
                    else:
                        new_data_rate = 0

                    SM_full = SM #SM_test.continue_full_mapping(tableA, tableB, SM)

                    if rank3[tableB_name] < new_data_rate:
                        new_data_rate = rank3[tableB_name]

                    new_score = float(beta) * self.col_similarity(tableA, tableB, SM_full, 1) + rank_candidate[ks + id][1] + float(1- beta- alpha) * new_data_rate
                    #

                    if new_score <= min_value:
                        continue
                    else:
                        top_tables.append((rank_candidate[ks + id][0], new_score))
                        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                        min_value = top_tables[ks][1]
            cnt = 0
            rtables = []
            for i, j in top_tables:
                rtables.append(i)
                cnt = cnt + 1
                if cnt == k:
                    break

            end_time2 = timeit.default_timer()
            timek.append(end_time2 - start_time2 + before_topk_time)
            rtablesk.append(rtables)

        if tflag:
            return rtablesk, timek
        else:
            return rtablesk

    #def search_additional_training_data_msq(self, SM_test, query_name, k, code, var_name, alpha, beta, theta):

    def search_alternative_features_basic(self, SM_test, query_name, k, code, var_name, alpha, beta, theta, tflag, gflag):

        query = self.real_tables[query_name]

        schema_map_partial = {}

        start_time = timeit.default_timer()
        partial_mapping, candidate_keys, partial_mapping_cached = SM_test.mapping_to_columns_keys_search(query, 2)
        logging.info("Searching: partial mapping")

        connected_tables = []
        connected_tables_mapped_col = {}
        for pkey in partial_mapping.keys():
            for pt in partial_mapping[pkey]:
                connected_tables.append(pt[0][6:])
                if pt[0][6:] not in connected_tables_mapped_col:
                    connected_tables_mapped_col[pt[0][6:]] = pt[1]
                if pt[0][6:] not in schema_map_partial:
                    schema_map_partial[pt[0][6:]] = {}
                schema_map_partial[pt[0][6:]][pkey] = pt[1]

        connected_groups = []
        connected_groups.append(self.table_group[query_name[6:]])

        if gflag == False:
            connected_groups = list(self.table_name_group.keys())
        else:
            for ct in connected_tables:
                connected_groups.append(self.table_group[ct])
            connected_groups = list(set(connected_groups))
            logging.info(connected_groups)


        #do search
        for i in self.real_tables.keys():

            if i == query_name:
                continue

            tname = i[6:]

            tableB = self.real_tables[i]

            if tname not in schema_map_partial:
                better_mapping = self.schema_mapping_float_prov(query, tableB, {})
            else:
                better_mapping = self.schema_mapping_float_prov(query, tableB, schema_map_partial[tname])

            schema_map_partial[tname] = better_mapping

        logging.info("Searching: better partial mapping")
        #logging.info(schema_map_partial)

        prov_class = SearchProv(self.Graphs)
        query_node = self.__generate_query_node_from_code(var_name, code)

        # Compute Provenance Similarity
        logging.info("Compute Provenance Similarity!")
        table_prov_rank = prov_class.search_score_rank(query_node)
        table_prov_score = {}

        for i, j in table_prov_rank:
            table_prov_score["rtable" + i] = j

        top_tables = []
        rank_candidate = []
        rank2 = []

        for i in self.real_tables.keys():

            if i == query_name:
                continue

            tname = i[6:]
            tableA = query
            tableB = self.real_tables[i]

            if tname not in schema_map_partial:
                continue
            else:
                inital_mapping = schema_map_partial[tname]

            if len(inital_mapping.keys()) == 0:
                continue

            if len(candidate_keys) == 0:
                candidates = list(inital_mapping.keys())
            else:
                candidates = candidate_keys

            key_return, col_depend, _, _ = SM_test.detect_key_constraints(query_name, i, inital_mapping,
                                                                              partial_mapping_cached,
                                                                              candidates, tableA, tableB)
            if len(key_return) == 0:
                continue

            key_return = key_return[0]

            key_selectivity = float(
                len(np.intersect1d(tableA[key_return].values, tableB[inital_mapping[key_return]].values))) / float(
                min(tableA.shape[0], tableB.shape[0]))

            #better_mapping = inital_mapping#self.schema_mapping_float_prov(tableA, tableB, inital_mapping)

            temp_score = key_selectivity

            rank_candidate.append((i, temp_score))

        rank_candidate = sorted(rank_candidate, key = lambda d:d[1], reverse=True)
        logging.info(rank_candidate[:5])

        return_tables = []
        for i in range(k):

            if i == len(rank_candidate):
                break

            return_tables.append(rank_candidate[i][0])

        return return_tables

    def search_additional_training_data(self, SM_test, query, k, candidate_table_list = None, alpha = 0.1, beta = 0.9, theta = 1):

        schema_map_partial, candidate_keys, partial_mapping_cached = self.search_tables_from_database(query, SM_test)
        logging.info("Searching: Longer Mapping Detected!")
        #logging.info(schema_map_partial)

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

    def search_alternative_features(self, candidate_table_list, SM_test, query_name, k, code, var_name, alpha, beta, theta):

        query = self.real_tables[query_name]
        query_node = self.__generate_query_node_from_code(var_name, code)
        query = Query_Info(query_name, query, query_node)
        tableA = query.value

        # Pre-computing
        schema_map_partial, candidate_keys, partial_mapping_cached = self.search_tables_from_database(query, SM_test)
        logging.info("Searching: Longer Mapping Detected!")
        logging.info(schema_map_partial)

        candidate_tables = {}
        for name in candidate_table_list:
            candidate_tables[name] = self.real_tables[name]

        if query.name in self.memory_sas:
            previous_state = self.memory_sas[1][query.name]
        else:
            previous_state = None

        #Introducing Sorted Access
        sorted_access = Sorted_Components(schema_map_partial, candidate_tables, self.Graphs, previous_state)

        # Computing Sorted Access
        rank_candidate, rank_score_only, sorted_state = sorted_access.merge_feature_engineering(query, alpha, beta)
        self.memory_sas[1][query.name] = sorted_state

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        if query.name in self.memory_ras:
            previous_state = self.memory_ras[1][query.name]
        else:
            previous_state = None

        random_access = Random_Components(self.real_tables, SM_test, previous_state)
        random_scores = {}
        top_tables = []
        for i in range(ks):
            tableB_name = rank_candidate[i][0]
            sm = rank_candidate[i][2]
            sorted_score = rank_score_only[i][1]
            score, scores = random_access.merge_additional_feature(sorted_score, query, tableB_name, sm,
                                                                    candidate_keys, partial_mapping_cached, alpha, beta)
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

                new_score, new_scores = random_access.merge_additional_feature(sorted_score, query, tableB_name, sm,
                                                                                candidate_keys, partial_mapping_cached,
                                                                                alpha, beta)

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

        random_state = random_access.save_state(query, random_scores, 1, self.db_eng)
        self.memory_ras[1][query.name] = random_state

        return rtables

    def search_joinable_data(self, SM_test, query_name, k, var_name_full, beta, theta, gflag):

        query = self.real_tables[query_name]

        schema_map_partial = {}


        # do partial schema mapping
        partial_mapping, candidate_keys, partial_mapping_cached = SM_test.mapping_to_columns_keys_search(query,2)
        logging.info("Searching: partial mapping")

        connected_tables = []
        connected_tables_mapped_col = {}
        for pkey in partial_mapping.keys():
            for pt in partial_mapping[pkey]:
                connected_tables.append(pt[0][6:])
                if pt[0][6:] not in connected_tables_mapped_col:
                    connected_tables_mapped_col[pt[0][6:]] = pt[1]
                if pt[0][6:] not in schema_map_partial:
                    schema_map_partial[pt[0][6:]] = {}
                schema_map_partial[pt[0][6:]][pkey] = pt[1]

        connected_groups = []
        connected_groups.append(self.table_group[query_name[6:]])

        for ct in connected_tables:
            connected_groups.append(self.table_group[ct])
        connected_groups = list(set(connected_groups))

        logging.info("Candidate Tables and Groups: ")
        #logging.info(schema_map_partial)
        #logging.info(connected_groups)

        for i in self.real_tables.keys():

            if i == query_name:
                continue

            tname = i[6:]

            tableB = self.real_tables[i]
            gid = self.table_group[tname]

            if gflag:
                if gid not in connected_groups:
                    continue
            else:
                if tname not in schema_map_partial:
                    better_mapping = self.schema_mapping_float_prov(query, tableB, {})
                else:
                    better_mapping = self.schema_mapping_float_prov(query, tableB, schema_map_partial[tname])

                schema_map_partial[tname] = better_mapping

        logging.info("Searching: better partial mapping")

        top_tables = []
        rank_candidate = []
        rank2 = []

        for i in self.real_tables.keys():

            tname = i[6:]
            if tname == var_name_full:
                continue

            tableA = query
            tableB = self.real_tables[i]

            gid = self.table_group[tname]

            if gflag:
                if gid not in connected_groups:
                    continue
            else:

                if tname not in schema_map_partial:
                    continue

                inital_mapping = schema_map_partial[tname]

                if len(inital_mapping.items()) == 0:
                    continue

                if len(candidate_keys) == 0:
                    continue

                #candidate_keys = list(inital_mapping.keys())
                key_return, col_depend = SM_test.detect_kfjey_constraints(query_name, i, inital_mapping,
                                                                        partial_mapping_cached, candidate_keys, tableA,
                                                                        tableB)
                #logging.info(key_return)

                if len(key_return) == 0:
                    continue
                if min(tableA.shape[0], tableB.shape[0]) == 0:
                    continue
                if tableB.shape[1] == 0:
                    continue

                key_selectivity = float(
                    len(np.intersect1d(tableA[key_return].dropna().values, tableB[inital_mapping[key_return]].dropna().values))) / float(
                    min(tableA.shape[0], tableB.shape[0]))

                rank_candidate.append((i, key_return, key_selectivity, col_depend, inital_mapping))
                upp_new_score = min(tableB.shape[1], tableA.shape[1])

                #upp_new_score = float(max(0, tableB.shape[1] - len(col_depend))) / float(tableB.shape[1])

                rank2.append(upp_new_score)

        rank_candidate = sorted(rank_candidate, key=lambda d: d[2], reverse=True)
        rank2 = sorted(rank2, reverse=True)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableA = query
            tableB = self.real_tables[rank_candidate[i][0]]

            key = rank_candidate[i][1]
            SM_DEP = rank_candidate[i][3]
            SM_real = rank_candidate[i][4]
            #SM_real = SM_test.continue_full_mapping(tableA, tableB, SM_real)
            if key in partial_mapping_cached:
                SM_DEP = SM_test.continue_full_foreign_dependency(tableA, tableB, query_name, rank_candidate[i][0], key,
                                                      SM_real, partial_mapping_cached, SM_DEP, 0.9)

            score = float(beta) * rank_candidate[i][2] + float(1 - beta) * float(len(SM_DEP))
            top_tables.append((rank_candidate[i][0], score))

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)

        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):

            if ks + id >= len(rank_candidate):
                break

            threshold = float(1 - beta) * rank2[ks + id] + float(beta) * rank_candidate[ks + id][2]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableR = self.real_tables[rank_candidate[ks + id][0]]
                SM_real = rank_candidate[ks + id][4]
                #SM_real = SM_test.continue_full_mapping(query, tableR, SM_real)
                key = rank_candidate[ks + id][1]
                SM_DEP = rank_candidate[ks + id][3]

                if key in partial_mapping_cached:
                    SM_DEP = SM_test.continue_full_foreign_dependency(query, tableR, query_name, rank_candidate[ks + id][0], key,
                                                          SM_real, partial_mapping_cached, SM_DEP, 0.9)

                # new_score = float(beta) * self.col_similarity(query, tableR, SM_real, 1) + float(1-beta)*rank_candidate[i][1]
                new_score = float(beta) * rank_candidate[ks + id][2] + float(1 - beta) * float(len(SM_DEP))

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

#        rtables_names = self.remove_dup2(top_tables, ks)


        cnt = 0
        rtables = []
        for i, j in top_tables:
            rtables.append([i, self.real_tables[i]])
            cnt = cnt + 1
            if cnt == k:
                break

        return rtables

    def search_alternative_data_clean(self, candidate_table_list, SM_test, query_name, k, code, var_name, alpha, beta, gamma, theta):

        query = self.real_tables[query_name]
        query_node = self.__generate_query_node_from_code(var_name, code)
        query = Query_Info(query_name, query, query_node)
        tableA = query.value

        # Pre-computing
        schema_map_partial, candidate_keys, partial_mapping_cached = self.search_tables_from_database(query, SM_test)
        logging.info("Searching: Longer Mapping Detected!")
        logging.info(schema_map_partial)

        candidate_tables = {}
        for name in candidate_table_list:
            candidate_tables[name] = self.real_tables[name]

        if query.name in self.memory_sas:
            previous_state = self.memory_sas[2][query.name]
        else:
            previous_state = None

        #Introducing Sorted Access
        sorted_access = Sorted_Components(schema_map_partial, candidate_tables, self.Graphs, previous_state)

        # Computing Sorted Access
        rank_candidate, rank_score_only, sorted_state = sorted_access.merge_data_cleaning(query, alpha, beta, gamma)
        self.memory_sas[2][query.name] = sorted_state

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        if query.name in self.memory_ras:
            previous_state = self.memory_ras[2][query.name]
        else:
            previous_state = None

        random_access = Random_Components(self.real_tables, SM_test, previous_state)
        random_scores = {}
        top_tables = []
        for i in range(ks):
            tableB_name = rank_candidate[i][0]
            sm = rank_candidate[i][2]
            sorted_score = rank_score_only[i][1]
            score, scores = random_access.merge_data_cleaning(sorted_score, query, tableB_name, sm,
                                                                    candidate_keys, partial_mapping_cached, alpha, beta, gamma)
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

                new_score, new_scores = random_access.merge_data_cleaning(sorted_score, query, tableB_name, sm,
                                                                                candidate_keys, partial_mapping_cached,
                                                                                alpha, beta, gamma)

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

        random_state = random_access.save_state(query, random_scores, 2, self.db_eng, self.db_cursor)
        self.memory_ras[2][query.name] = random_state

        return rtables

    def search_alternative_data_clean_basic(self, SM_test, query_name, k, alpha, beta, theta, tflag):

        query = self.real_tables[query_name]

        schema_map_partial = {}

        start_time = timeit.default_timer()
        partial_mapping, candidate_keys, partial_mapping_cached = SM_test.mapping_to_columns_keys_search(query, 2)
        logging.info("Searching: partial mapping")

        SM_test.cached_table_depend = {}
        logging.info(partial_mapping_cached)

        connected_tables = []
        connected_tables_mapped_col = {}
        for pkey in partial_mapping.keys():
            for pt in partial_mapping[pkey]:
                connected_tables.append(pt[0][6:])
                if pt[0][6:] not in connected_tables_mapped_col:
                    connected_tables_mapped_col[pt[0][6:]] = pt[1]
                if pt[0][6:] not in schema_map_partial:
                    schema_map_partial[pt[0][6:]] = {}
                schema_map_partial[pt[0][6:]][pkey] = pt[1]

        connected_groups = []
        connected_groups.append(self.table_group[query_name[6:]])

        for ct in connected_tables:
            connected_groups.append(self.table_group[ct])
        connected_groups = list(set(connected_groups))

        #do search
        for i in self.real_tables.keys():

            if i == query_name:
                continue

            tname = i[6:]

            tableB = self.real_tables[i]
            gid = self.table_group[tname]

            if gid not in connected_groups:
                continue
            else:
                if tname not in schema_map_partial:
                    better_mapping = self.schema_mapping_float_prov(query, tableB, {})
                else:
                    better_mapping = self.schema_mapping_float_prov(query, tableB, schema_map_partial[tname])

                schema_map_partial[tname] = better_mapping

        logging.info("Candidate Tables: ")
        #logging.info(schema_map_partial)

        top_tables = []
        rank_candidate = []
        rank2 = []

        for i in self.real_tables.keys():

            if i == query_name:
                continue

            tname = i[6:]

            tableA = query
            tableB = self.real_tables[i]

            gid = self.table_group[tname]

            if tname not in schema_map_partial:
                continue
            else:
                inital_mapping = schema_map_partial[tname]

            if len(inital_mapping.items()) == 0:
                continue

            if len(candidate_keys) == 0:
                    candidate_keys = list(inital_mapping.keys())

            key_return, col_depend, key_indexA, key_indexB = SM_test.detect_key_constraints(query_name, i, inital_mapping,
                                                                    partial_mapping_cached, candidate_keys, tableA, tableB)

            if len(key_return) == 0:
                continue

            better_mapping = inital_mapping

            try:
                key_selectivity = float(
                    len(np.intersect1d(tableA[key_return].dropna().values, tableB[inital_mapping[key_return]].dropna().values))) / float(
                    len(np.union1d(tableA[key_return].dropna().values, tableB[inital_mapping[key_return]].dropna().values)))
            except:
                key_selectivity = 0

            rank_candidate.append((i, key_return[0], key_selectivity, col_depend, better_mapping, key_indexA, key_indexB))

            upp_new_score = beta * float(len(col_depend))

            rank2.append(upp_new_score)

        rank_candidate = sorted(rank_candidate, key=lambda d: d[2], reverse=True)
        rank2 = sorted(rank2, reverse=True)

        if len(rank_candidate) == 0:
            end_time = timeit.default_timer()
            if tflag:
                return [], end_time - start_time
            else:
                return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableA = query
            tableB = self.real_tables[rank_candidate[i][0]]

            key = rank_candidate[i][1]
            SM_DEP = rank_candidate[i][3]
            SM_real = rank_candidate[i][4]
            #SM_real = SM_test.continue_full_mapping(tableA, tableB, SM_real)

            if key in partial_mapping_cached:
                SM_DEP, key_indexA, key_indexB = SM_test.continue_full_dependency(tableA, tableB, query_name, rank_candidate[i][0], key,
                                                          SM_real, partial_mapping_cached, SM_DEP, 0.9)
            else:
                key_indexA = rank_candidate[i][5]
                key_indexB = rank_candidate[i][6]

            score = float(alpha) * rank_candidate[i][2] + float(beta) * float(len(SM_DEP))

            top_tables.append((rank_candidate[i][0], score))

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)

        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):

            if ks + id >= len(rank_candidate):
                break

            threshold = rank2[ks + id] + float(alpha) * rank_candidate[ks + id][2]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableA = query
                tableB = self.real_tables[rank_candidate[ks + id][0]]
                SM_real = rank_candidate[ks + id][4]
                #SM_real = SM_test.continue_full_mapping(tableA, tableB, SM_real)

                if key in partial_mapping_cached:
                    SM_DEP, key_indexA, key_indexB = SM_test.continue_full_dependency(tableA, tableB, query_name, rank_candidate[i][0], key,
                                                          SM_real, partial_mapping_cached, SM_DEP, 0.9)
                else:
                    key_indexA = rank_candidate[ks + id][5]
                    key_indexB = rank_candidate[ks + id][6]

                new_score = float(alpha) * rank_candidate[ks + id][2] + float(beta) * float(len(SM_DEP))

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        cnt = 0
        rtables = []
        for i, j in top_tables:
            if cnt == k:
                break
            rtables.append(i)
            cnt = cnt + 1

        end_time = timeit.default_timer()
        if tflag:
            running_time = end_time - start_time
            return rtables, running_time
        else:
            return rtables