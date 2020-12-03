import logging
import psycopg2
import sys
import json
import pandas as pd
import networkx as nx
from .search_tables import SearchTables
from .sorted import Sorted_Components
from .random import Random_Components
from .search_lshe import LSHE
from .search_ks import KS
from juneau.config import config
from juneau.db.table_db import pre_vars


class WithProv_Cached(SearchTables):

    special_type = ['np', 'pd']

    def __connect2db_init(self):
        # Define our connection string
        conn_string = "host='localhost' dbname=\'" + config.sql.dbname + "\' user=\'" + config.sql.name + \
                      "\' password=\'" + config.sql.password + "\'"

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
            dependency = pd.read_sql_table('dependen', conn, schema = config.sql.graph)#, schema='graph_model')
            line2cid = pd.read_sql_table('line2cid', conn, schema = config.sql.graph)#, schema='graph_model')
            lastliid = pd.read_sql_table('lastliid', conn, schema = config.sql.graph)
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

    def __init__(self, schema = config.sql.dbs, clear_cache = False):

        super().__init__(schema, read_flag=False)
        self.index()

        self.db_cursor = self.__connect2db_init()
        self.initialize_in_database_search_class()

        self.real_tables = {}
        self.initialize_for_topk()
        self.initialize_memory_states(clear_cache)

        logging.info('Data Search Extension Prepared!')

    def index(self):
        logging.info('Reading Graph of Notebooks.')
        self.Graphs, self.n_l2cid = self.read_graph_of_notebook()

    def initialize_in_database_search_class(self):
        self.search_lshe = LSHE(self.eng)
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
            query1 = "DROP SCHEMA IF EXISTS " + config.topk.schema_ra_states + " CASCADE;"
            query2 = "CREATE SCHEMA IF NOT EXISTS " + config.topk.schema_ra_states + ";"
            query3 = "DROP SCHEMA IF EXISTS " + config.topk.schema_sa_states + " CASCADE;"
            query4 = "CREATE SCHEMA IF NOT EXISTS " + config.topk.schema_sa_states + ";"

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

    def search_tables_from_database(self, query, SM_test):

        tableA = query.value
        mapping = {}
        for col in tableA.columns:
            #print(col, tableA[col].dtype)
            if tableA[col].dtype == 'object':
                c_table = self.search_lshe.query_cols([{'schema':config.sql.dbs, 'table':query.name, 'col':col}])[0]
                if len(c_table['similar_domains']) > 0:
                    if col not in mapping:
                        mapping[col] = []
                    for res in c_table['similar_domains']:
                        mapping[col].append([res['table'], res['column']])
            elif tableA[col].dtype == 'int' or tableA[col].dtype == 'float':
                c_table = self.search_ks.query_cols([{'schema':config.sql.dbs, 'table':query.name, 'col':col}])[0]
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

    def search_additional_training_data(self, SM_test, query, k, candidate_table_list = None, alpha = 0.1, beta = 0.9, theta = 1):

        schema_map_partial, candidate_keys, partial_mapping_cached = self.search_tables_from_database(query, SM_test)
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

    def search_alternative_features(self, SM_test, query, k, candidate_table_list = None, alpha = 0.4 , beta = 0.4, theta = 1):

        schema_map_partial, candidate_keys, partial_mapping_cached = self.search_tables_from_database(query, SM_test)
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

    def search_alternative_data_clean(self, SM_test, query, k, candidate_table_list = None, alpha = 0.4 , beta = 0.4, gamma = 0.1, theta = 1):

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

