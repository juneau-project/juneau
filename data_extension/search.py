from py2neo import NodeMatcher
#from table_db import user_name, password, dbname
from data_extension.table_db import connect2gdb, connect2db
from data_extension.table_db import fetch_all_table_names, fetch_all_views
from data_extension.table_db import SchemaMapping, SchemaMapping_SK
from data_extension.table_db import generate_graph
from data_extension.table_db import parse_code
from data_extension.table_db import pre_vars

import data_extension.config as cfg

import json
import pandas as pd
import numpy as np
from sys import getsizeof
import os
import pickle

import random
import sys
import timeit
import copy
import queue
import logging
import networkx as nx

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

special_type = ['np', 'pd']

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def _getsizeof(x):
    # return the size of variable x. Amended version of sys.getsizeof
    # which also supports ndarray, Series and DataFrame
    if type(x).__name__ in ['ndarray', 'Series']:
        return x.nbytes
    elif type(x).__name__ == 'DataFrame':
        return x.memory_usage().sum()
    else:
        return getsizeof(x)

def last_line_var(varname, code):
    code = code.split('\n')
    ret = 0
    for id, i in enumerate(code):
        if '=' not in i:
            continue
        j = i.split('=')
        if varname in j[0]:
            ret = id + 1
    return ret

class WithProv:

    def __line2cid(self, dir):
        nb_l2c = {}
        files = os.listdir(dir)
        for f in files:
            ind = pickle.load(open(dir + "/" + f, "rb"))
            nb_l2c[f[:-2]] = ind
        return nb_l2c

    def __row_similarity(self, colA, colB):

        colA_value = colA[~pd.isnull(colA)].values
        colB_value = colB[~pd.isnull(colB)].values

        row_sim_upper = len(np.intersect1d(colA_value, colB_value))
        row_sim_lower = len(np.union1d(colA_value, colB_value))
        row_sim = float(row_sim_upper) / float(row_sim_lower)
        return row_sim

    def __col_similarity(self, tableA, tableB, SM, key_factor):

        col_sim_upper = 1 + float(len(SM.keys()) - 1) * float(key_factor)
        tableA_not_in_tableB = []
        for kyA in tableA.columns.tolist():
            if kyA not in SM:
                tableA_not_in_tableB.append(kyA)
        col_sim_lower = len(tableB.columns.values) + len(tableA_not_in_tableB)
        col_sim = float(col_sim_upper) / float(col_sim_lower)
        return col_sim

    def __remove_dup(self, ranked_list, ks):
        res = []
        for i,j,l in ranked_list:
            flg = True
            for k,m in res:
                if self.real_tables[i].equals(self.real_tables[k]):
                    flg = False
                    break
            if flg == True:
                res.append((i,l))

            if len(res) == ks:
                break
        return res

    def __dfs(self, snode):

        cell_rel = self.geng.match_one((snode,), r_type = "Containedby")
        cell_node = cell_rel.end_node
        names = []
        return_names = []
        stack = [cell_node]
        while(True):
            if len(stack) != 0:
                node = stack.pop()
                names.append(node['name'])
                for rel in self.geng.match((node, ), r_type = "Contains"):
                    return_names.append(rel.end_node['name'])

                for rel in self.geng.match((node, ), r_type="Successor"):
                    if rel.end_node['name'] in names:
                        continue
                    else:
                        stack.append(rel.end_node)

                for rel in self.geng.match((node, ), r_type = "Parent"):
                    if rel.end_node['name'] in names:
                        continue
                    else:
                        stack.append(rel.end_node)
            else:
                break

        return list(set(return_names))

    def init_schema_mapping(self):

        matcher = NodeMatcher(self.geng)

        tables_touched = []
        tables_connected = []
        for i in self.real_tables.keys():
            if i[6:] not in set(tables_touched):
                current_node = matcher.match("Var", name = i[6:]).first()
                connected_tables = self.__dfs(current_node)
                tables_touched = tables_touched + connected_tables
                tables_connected.append(connected_tables)

        self.schema_linking = {}
        self.schema_element = {}
        self.schema_element_count = {}
        self.schema_element_dtype = {}

        self.table_group = {}

        # assign each table a group id
        for idi, i in enumerate(tables_connected):
            for j in i:
                self.table_group[j] = idi

        for idi, i in enumerate(tables_connected):
            self.schema_linking[idi] = {}
            self.schema_element[idi] = {}
            self.schema_element_dtype[idi] = {}
            self.schema_element_count[idi] = {}

            for j in i:
                tname = 'rtable' + j
                if tname not in self.real_tables:
                    continue
                for col in self.real_tables[tname].columns:
                    if col not in self.schema_linking[idi]:
                        if len(self.schema_linking[idi].keys()) == 0:
                            sid = 0
                        else:
                            sid = max(list(self.schema_linking[idi].values())) + 1

                        self.schema_linking[idi][col] = sid
                        self.schema_element_dtype[idi][col] = self.real_tables[tname][col].dtype
                        self.schema_element_count[idi][col] = 1
                        self.schema_element[idi][col] = []
                        self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()
                        self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
                    else:
                        self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()
                        self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
                        self.schema_element_count[idi][col] += 1

        logging.info('There are %s groups of tables.'%len(tables_connected))

    def sketch_meta_mapping(self, sz = 10):
        self.schema_element_sample = {}
        for i in self.schema_element.keys():
            self.schema_element_sample[i] = {}
            if len(self.schema_element[i].keys()) <= sz:
                for sc in self.schema_element[i].keys():
                    self.schema_element_sample[i][sc] = self.schema_element[i][sc]
            else:
                sc_choice = []
                for sc in self.schema_element[i].keys():
                    if sc == "Unnamed: 0" or "index" in sc:
                        continue
                    if (self.schema_element_dtype[i][sc] is np.dtype(float)):
                        continue
                    sc_value = list(self.schema_element[i][sc])
                    sc_choice.append((sc, float(len(set(sc_value)))/float(len(sc_value))))
                sc_choice = sorted(sc_choice, key = lambda d:d[1], reverse=True)

                count = 0
                for sc,v in sc_choice:
                    if count == sz:
                        break
                    self.schema_element_sample[i][sc] = self.schema_element[i][sc]
                    count += 1

    def sketch_query_cols(self, query, sz = 10):
        if query.shape[1] <= sz:
            return query.columns.tolist()
        else:
            q_cols = query.columns.tolist()
            c_scores = []
            for i in q_cols:
                if i == "Unnamed: 0" or "index" in i:
                    continue
                if query[i].dtype is np.dtype(float):
                    continue
                cs_v = query[i].tolist()
                c_scores.append((i, float(len(set(cs_v)))/float(len(cs_v))))
            c_scores = sorted(c_scores, key = lambda d:d[1], reverse=True)

            q_cols_chosen = []
            c_count = 0
            for i, j in c_scores:
                if c_count == sz:
                    break
                q_cols_chosen.append(i)
                c_count += 1
            return q_cols_chosen

    def __init__(self, dbname, schema = None):
        self.query = None
        self.eng = connect2db(dbname)
        self.geng = connect2gdb()

        self.real_tables = {}

        if schema != None:
            logging.info('Indexing existing tables from data lake')
            self.tables = fetch_all_table_names(schema, self.eng)
            for i in self.tables:
                try:
                    tableR = pd.read_sql_table(i, self.eng, schema = schema)
                    if 'Unnamed: 0' in tableR.columns:
                        tableR.drop(['Unnamed: 0'], axis=1, inplace=True)
                    self.real_tables[i] = tableR
                except:
                    continue
        else:
            logging.info('Indexing views  from data lake')
            self.tables = fetch_all_views(self.eng)
            for i in self.tables:
                try:
                    tableR = pd.read_sql_table(i, self.eng)
                    self.real_tables[i] = tableR
                except:
                    continue

        logging.info('%s tables detected in the database.')

        self.init_schema_mapping()
        self.sketch_meta_mapping()

        logging.info('Data Search Extension Started!')

        self.n_l2cid = self.__line2cid('~/similar_table_lcid')

    def app_common_key(self,tableA, tableB, SM, key, thres_prune): # thres_prune = 0.2

        kyA = key
        kyB = SM[key]
        key_value_A = tableA[kyA].tolist()
        key_value_B = tableB[kyB].tolist()

        key_estimateA = float(len(set(key_value_A))) / float(len(key_value_A))
        key_estimateB = float(len(set(key_value_B))) / float(len(key_value_B))
        if min(key_estimateA, key_estimateB) <= thres_prune:
            return 0

#        other_key = list(SM.keys())
#        other_key.remove(key)
        mapped_keyA = list(SM.keys())

        if kyA not in self.query_fd:
            self.query_fd[kyA] = {}
            for idv, kv in enumerate(key_value_A):
                if kv not in self.query_fd[kyA]:
                    self.query_fd[kyA][kv] = []
                self.query_fd[kyA][kv].append(','.join(map(str, tableA[mapped_keyA].iloc[idv].tolist())))
            fd = copy.deepcopy(self.query_fd[key])
        else:
            fd = copy.deepcopy(self.query_fd[key])

        mapped_keyB = list(SM.values())
        for idv, kv in enumerate(key_value_B):
            if kv not in fd:
                fd[kv] = []
            fd[kv].append(','.join(map(str, tableB[mapped_keyB].iloc[idv].tolist())))

        key_score = 0
        for fdk in fd.keys():
            key_score = key_score + float(len(set(fd[fdk]))) / float(tableA.shape[0] + tableB.shape[0])

        return key_score

    def app_join_key(self, tableA, tableB, SM, key, thres_prune):

        kyA = key
        key_value_A = tableA[key].tolist()
        scoreA = float(len(set(key_value_A)))/float(len(key_value_A))
        if scoreA == 1:
            return 1

        kyB = SM[key]
        key_value_B = tableB[kyB].tolist()
        scoreB = float(len(set(key_value_B)))/float(len(key_value_B))
        if scoreB == 1:
            return 1

        if min(scoreA, scoreB) < thres_prune:
            return 0


        mapped_keyA = list(SM.keys())

        if kyA not in self.query_fd:
            self.query_fd[kyA] = {}
            for idv, kv in enumerate(key_value_A):
                if kv not in self.query_fd[kyA]:
                    self.query_fd[kyA][kv] = []
                self.query_fd[kyA][kv].append(','.join(map(str, tableA[mapped_keyA].iloc[idv].tolist())))
            fd = copy.deepcopy(self.query_fd[key])
        else:
            fd = copy.deepcopy(self.query_fd[key])

        mapped_keyB = list(SM.values())
        for idv, kv in enumerate(key_value_B):
            if kv in fd:
                fd[kv].append(','.join(map(str, tableB[mapped_keyB].iloc[idv].tolist())))

        key_scoreAB = 0
        for fdk in fd.keys():
            key_scoreAB += float(len(set(fd[fdk]))) / float(tableA.shape[0] + tableB.shape[0])

        temp_fd = {}
        for idv, kv in enumerate(key_value_B):
            if kv not in temp_fd:
                temp_fd[kv] = []
            temp_fd[kv].append(','.join(map(str, tableB[mapped_keyB].iloc[idv].tolist())))

        for idv, kv in enumerate(key_value_A):
            if kv in temp_fd:
                temp_fd[kv].append(','.join(map(str, tableA[mapped_keyA].iloc[idv].tolist())))

        key_scoreBA = 0
        for fdk in temp_fd.keys():
            key_scoreBA += float(len(set(temp_fd[fdk]))) / float(tableA.shape[0] + tableB.shape[0])

        return max(key_scoreAB, key_scoreBA)

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
            max_valueL.append(self.__row_similarity(tableA[i], tableB[j]))

        if len(max_valueL) > 0:
            mv = max(max_valueL)
        else:
            mv = 0

        return s_mapping, mv

    def comp_table_similarity_row(self, tableA, tableB, key, SM, sample_size):

        try:
            value_setb = set(tableB[SM[key]].tolist())
        except:
            print(tableB.columns)
            print(SM[key])

        sim_sum = 0
        sim_count = 0
        for idi, i in enumerate(tableA[key].tolist()):
            if sim_count == sample_size:
                break
            if i in value_setb:
                dfB = tableB.loc[tableB[SM[key]] == i]
                sim_array = []
                for index, row in dfB.iterrows():
                    a = np.array(list(map(str, tableA.iloc[idi].tolist())))
                    b = np.array(list(map(str, row.tolist())))
                    #a = a[~pd.isnull(a)].values
                    #b = b[~pd.isnull(b)].values
                    try:
                        sim_array.append(float(len(np.intersect1d(a,b)))/float(len(np.union1d(a,b))))
                    except:
                        print(a)
                        print(b)
                sim_array = np.array(sim_array)
                sim_sum = sim_sum + np.mean(sim_array)
                sim_count += 1
        if sim_count == 0:
            return 0
        else:
            return float(sim_sum)/float(sim_count)

    def comp_table_similarity(self, tableA, tableB, beta, SM, gid, thres_key_prune, thres_key_cache):

        key_choice = []
        for kyA in SM.keys():
            flg = False
            if kyA in self.already_map[gid]:
                determined = self.already_map[gid][kyA]
                check_set = set(list(SM.values()))
                for ds in determined:
                    if ds.issubset(check_set):
                        flg = True
                        break
                if flg:
                    key_choice.append((kyA, self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)))
                    break

                else:
                    key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
                    key_choice.append((kyA, key_score))
                    if key_score == 1:
                        break
            else:
                key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
                key_choice.append((kyA, key_score))

        if len(key_choice) == 0:
            return 0
        else:
            key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
            key_chosen = key_choice[0][0]
            key_factor = key_choice[0][1]

            if key_factor >= thres_key_cache:
                if key_chosen not in self.already_map[gid]:
                    self.already_map[gid][key_chosen] = []
                self.already_map[gid][key_chosen].append(set(list(SM.values())))

            row_sim = self.__row_similarity(tableA[key_chosen], tableB[SM[key_chosen]])
            col_sim = self.__col_similarity(tableA, tableB, SM, key_factor)

            return beta * col_sim + float(1 - beta) * row_sim

    def comp_table_similarity_key(self, SM_test, tableA, tableB, beta, SM, gid, meta_mapping, schema_linking, thres_key_prune, thres_key_cache, unmatched):

        key_choice = []
        for kyA in SM.keys():
            flg = False
            if kyA in self.already_map[gid]:
                determined = self.already_map[gid][kyA]
                check_set = set(list(SM.values()))
                for ds in determined:
                    if ds.issubset(check_set):
                        flg = True
                        break
                if flg:
                    key_choice.append((kyA, self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)))
                    break

                else:
                    key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
                    key_choice.append((kyA, key_score))
                    if key_score == 1:
                        break
            else:
                key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
                key_choice.append((kyA, key_score))

        if len(key_choice) == 0:
            #for i in tableA.columns.tolist():
            #    for j in tableB.columns.tolist():
            #        unmatched[gid][i][schema_linking[gid][j]] = ''
            return 0, meta_mapping, unmatched, 0, None
        else:
            key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
            key_chosen = key_choice[0][0]
            key_factor = key_choice[0][1]

            if key_factor >= thres_key_cache:
                if key_chosen not in self.already_map[gid]:
                    self.already_map[gid][key_chosen] = []
                self.already_map[gid][key_chosen].append(set(list(SM.values())))

            SM_real, meta_mapping, unmatched, sm_time = SM_test.mapping_naive_incremental(tableA, tableB, gid, meta_mapping, schema_linking, unmatched, mapped=SM) #SM_test.mapping_naive(tableA, tableB, SM)

            row_sim = self.__row_similarity(tableA[key_chosen], tableB[SM_real[key_chosen]])
            col_sim = self.__col_similarity(tableA, tableB, SM_real, key_factor)

            return beta * col_sim + float(1 - beta) * row_sim, meta_mapping, unmatched, sm_time, key_chosen

    def comp_table_joinable(self, tableA, tableB, beta, SM, gid, thres_key_prune, thres_key_cache):

        key_choice = []
        for kyA in SM.keys():
            flg = False
            if kyA in self.already_map[gid]:
                determined = self.already_map[gid][kyA]
                check_set = set(list(SM.values()))
                for ds in determined:
                    if ds.issubset(check_set):
                        flg = True
                        break
                if flg:
                    key_choice.append((kyA, self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)))
                    break

                else:
                    key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
                    key_choice.append((kyA, key_score))
                    if key_score == 1:
                        break
            else:
                key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
                key_choice.append((kyA, key_score))

        key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
        key_chosen = key_choice[0][0]
        key_factor = key_choice[0][1]

        if key_factor >= thres_key_cache:
            if key_chosen not in self.already_map[gid]:
                self.already_map[gid][key_chosen] = []
            self.already_map[gid][key_chosen].append(set(list(SM.values())))

        row_sim = self.__row_similarity(tableA[key_chosen], tableB[SM[key_chosen]])

        col_sim_upper = 1 + float(len(tableA.columns.values) - 1) * float(key_factor)
        tableA_not_in_tableB = []
        for kyA in tableA.columns.tolist():
            if kyA not in SM:
                tableA_not_in_tableB.append(kyA)
        col_sim_lower = len(tableB.columns.values) + len(tableA_not_in_tableB)
        col_sim = float(col_sim_upper) / float(col_sim_lower)

        col_sim_upper2 = 1 + float(len(tableB.columns.values) - 1) * float(key_factor)
        col_sim2 = float(col_sim_upper2) / float(col_sim_lower)

        score1 = beta * col_sim + float(1 - beta) * row_sim
        score2 = beta * col_sim2 + float(1 - beta) * row_sim

        return max(score1, score2)

    def comp_table_joinable_key(self, SM_test, tableA, tableB, beta, SM, gid, meta_mapping, schema_linking, thres_key_prune, unmatched):

        key_choice = []
        for kyA in SM.keys():
            key_score = self.app_join_key(tableA, tableB, SM, kyA, thres_key_prune)
            key_choice.append((kyA, key_score))

        if len(key_choice) == 0:
            return 0, meta_mapping, unmatched, 0, None
        else:
            key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
            key_chosen = key_choice[0][0]
            key_factor = key_choice[0][1]

            SM_real, meta_mapping, unmatched, sm_time = SM_test.mapping_naive_incremental(tableA, tableB, gid,
                                                                                          meta_mapping, schema_linking,
                                                                                          unmatched,
                                                                                          mapped=SM)  # SM_test.mapping_naive(tableA, tableB, SM)

            col_sim_upper = 1 + float(len(tableA.columns.values) - 1) * float(key_factor)
            tableA_not_in_tableB = []
            for kyA in tableA.columns.tolist():
                if kyA not in SM_real:
                    tableA_not_in_tableB.append(kyA)
            col_sim_lower = len(tableB.columns.values) + len(tableA_not_in_tableB) - 1
            col_sim = float(col_sim_upper) / float(col_sim_lower)

            col_sim_upper2 = 1 + float(len(tableB.columns.values) - 1) * float(key_factor)
            col_sim2 = float(col_sim_upper2) / float(col_sim_lower)

            row_sim = self.__row_similarity(tableA[key_chosen], tableB[SM_real[key_chosen]])

            score1 = beta * col_sim + float(1 - beta) * row_sim
            score2 = beta * col_sim2 + float(1 - beta) * row_sim

            return max(score1, score2), meta_mapping, unmatched, sm_time, key_chosen

    # search_similar_tables(query, beta, k, 0.9, 0.2, True)
    def search_similar_tables(self, query, beta, k, thres_key_cache, thres_key_prune, tflag = False):

        self.query = query

        topk_tables = []
        SM_test = SchemaMapping()
        #groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
        #self.query_fd = {}

        start_time1 = timeit.default_timer()
        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_tables(self.query, self.schema_element, groups_possibly_matched)
        end_time= timeit.default_timer()
        time1 += end_time - start_time

        time2 = 0
        time3 = 0

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            start_time = timeit.default_timer()
            SM = self.schema_mapping(self.query, self.real_tables[i], meta_mapping, gid, False)
            end_time = timeit.default_timer()
            time2 += end_time - start_time

            if len(SM.keys()) == 0:
                continue

            start_time = timeit.default_timer()
            table_sim = self.comp_table_similarity(self.query, self.real_tables[i], beta, SM, gid, thres_key_prune, thres_key_cache)
            end_time = timeit.default_timer()
            time3 += end_time - start_time
            topk_tables.append((i, table_sim))

        topk_tables = sorted(topk_tables, key = lambda d:d[1], reverse=True)
        end_time1 = timeit.default_timer()
        time4 = end_time1 - start_time1

        if len(topk_tables) < k:
            k = len(topk_tables)

        rtables_names = self.__remove_dup(topk_tables, k)

        if tflag == True:
            print('Schema Mapping Cost: ', time1 + time2)
            print('Similarity Computation Cost: ', time3)
            print('Totally Cost: ', time4)

        rtables = []
        for i in rtables_names:
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_similar_tables_threshold1(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag = False):

        self.query = query
        query_col = self.sketch_query_cols(query)

        self.already_map = {}
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        SM_test = SchemaMapping()
        #groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
        self.query_fd = {}

        start_time1 = timeit.default_timer()
        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_tables(self.query, query_col, self.schema_element, groups_possibly_matched)
        end_time= timeit.default_timer()
        time1 += end_time - start_time

        top_tables = []
        Cache_MaxSim = {}

        rank2 = []
        rank_candidate = []

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            tableS = self.query
            tableR = self.real_tables[i]

            start_time = timeit.default_timer()
            SM, ms = self.schema_mapping(tableS, tableR, meta_mapping, gid, True)
            end_time = timeit.default_timer()
            time1 = time1 + end_time - start_time

            Cache_MaxSim[tname] = ms

            if len(SM.items()) == 0:
                continue

            tableSnotintableR = []
            for sk in tableS.columns.tolist():
                if sk not in SM:
                    tableSnotintableR.append(sk)

            vname_score =  float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))
            vname_score2 = float(len(SM.keys()) - 1) / float(len(tableR.columns.values) + len(tableSnotintableR) - 1)
            ubound = beta * vname_score2 + float(1 - beta) * Cache_MaxSim[tname]

            rank2.append(ubound)
            rank_candidate.append((tname, vname_score, SM))

        rank2 = sorted(rank2, reverse=True)
        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableR = self.real_tables[rank_candidate[i][0]]
            SM = rank_candidate[i][2]
            gid = self.table_group[rank_candidate[i][0][6:]]
            top_tables.append((rank_candidate[i][0], self.comp_table_similarity(self.query, tableR, beta, SM, gid, thres_key_prune, thres_key_cache)))

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):
            if ks + id >= len(rank_candidate):
                break

            threshold = beta * rank_candidate[ks + id][1] + rank2[0]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableR = self.real_tables[rank_candidate[ks + id][0]]
                SM = rank_candidate[ks + id][2]
                gid = self.table_group[rank_candidate[ks + id][0][6:]]
                new_score = self.comp_table_similarity(self.query, tableR, beta, SM, gid, thres_key_cache, thres_key_cache)
                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        end_time1 = timeit.default_timer()
        time3 = end_time1 - start_time1

        if tflag == True:
            print('Schema Mapping Cost: ', time1)
            print('Totally Cost: ', time3)

        rtables_names = self.__remove_dup(top_tables, ks)

        rtables = []
        for i in rtables_names:
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_similar_tables_threshold2(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag = False):

        self.query = query

        self.query_fd = {}

        self.already_map = {}
        SM_test = SchemaMapping()
        start_time1 = timeit.default_timer()

        # store the mapping computed at the very beginning
        # Initialize
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        # Choose the most possible keys from the query table
        query_col = self.sketch_query_cols(query)
        #print(query_col)

        time1 = 0
        start_time = timeit.default_timer()
        # Do mapping
        meta_mapping = SM_test.mapping_naive_tables(self.query, query_col, self.schema_element_sample, \
                                                               self.schema_element_dtype)

        end_time= timeit.default_timer()
        time1 += end_time - start_time

        logging.info(str(meta_mapping))

        # Compute unmatched pairs
        unmatched = {}
        for i in meta_mapping.keys():
            unmatched[i] = {}
            for j in query.columns.tolist():
                unmatched[i][j] = {}
                if (j in query_col) and (j not in meta_mapping[i]):
                    for l in self.schema_element_sample[i].keys():
                        unmatched[i][j][l] = ""


        top_tables = []
        Cache_MaxSim = {}

        rank2 = []
        rank_candidate = []

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            tableS = self.query
            tableR = self.real_tables[i]

            start_time = timeit.default_timer()
            SM, ms = self.schema_mapping(tableS, tableR, meta_mapping, gid)
            end_time = timeit.default_timer()
            time1 = time1 + end_time - start_time
            Cache_MaxSim[tname] = ms

            if len(SM.items()) == 0:
                continue

            tableSnotintableR = []
            for sk in tableS.columns.tolist():
                if sk not in SM:
                    tableSnotintableR.append(sk)

            vname_score =  float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))

            vname_score2 = float(min(len(tableS.columns.tolist()), len(tableR.columns.tolist())) - 1) \
                           / float(len(tableR.columns.values) + len(tableSnotintableR) - 1)

            ubound = beta * vname_score2 + float(1 - beta) * Cache_MaxSim[tname]

            rank2.append(ubound)
            rank_candidate.append((tname, vname_score, SM))

        rank2 = sorted(rank2, reverse=True)
        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableR = self.real_tables[rank_candidate[i][0]]
            gid = self.table_group[rank_candidate[i][0][6:]]
            SM_real = rank_candidate[i][2]
            score, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_similarity_key(SM_test, self.query, tableR, beta, SM_real, gid, meta_mapping, self.schema_linking, thres_key_prune, thres_key_cache, unmatched)
            top_tables.append((rank_candidate[i][0], score, key_chosen))
            time1 += sm_time

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):
            if ks + id >= len(rank_candidate):
                break

            threshold = beta * rank_candidate[ks + id][1] + float(1 - beta) * rank2[0]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableR = self.real_tables[rank_candidate[ks + id][0]]
                gid = self.table_group[rank_candidate[ks + id][0][6:]]
                SM_real = rank_candidate[ks + id][2]
                rs, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_similarity_key(SM_test, self.query, tableR, beta, SM_real, gid,
                                                                     meta_mapping, self.schema_linking, thres_key_prune,
                                                                     thres_key_cache, unmatched)
                time1 += sm_time
                new_score = rs

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score, key_chosen))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        end_time1 = timeit.default_timer()
        time3 = end_time1 - start_time1

        if tflag == True:
            print('Schema Mapping Cost: ', time1)
            print('Totally Cost: ', time3)

        rtables_names = self.__remove_dup(top_tables, ks)

        rtables = []
        for i,j in rtables_names:
            print(i,j)
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_joinable_tables(self, query, beta, k, thres_key_cache, thres_key_prune, tflag):

        self.query = query
        self.already_map = {}
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        topk_tables = []
        SM_test = SchemaMapping()
        groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
        self.query_fd = {}

        start_time1 = timeit.default_timer()
        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_tables(self.query, self.schema_element, groups_possibly_matched)
        end_time= timeit.default_timer()
        time1 += end_time - start_time
        time2 = 0
        time3 = 0

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            start_time = timeit.default_timer()
            SM = self.schema_mapping(self.query, self.real_tables[i], meta_mapping, gid, False)
            end_time = timeit.default_timer()
            time2 += end_time - start_time

            if len(SM.keys()) == 0:
                continue

            start_time = timeit.default_timer()
            table_sim = self.comp_table_joinable(self.query, self.real_tables[i], beta, SM, gid, thres_key_prune, thres_key_cache)
            end_time = timeit.default_timer()
            time3 += end_time - start_time
            topk_tables.append((i, table_sim))

        topk_tables = sorted(topk_tables, key = lambda d:d[1], reverse=True)
        end_time1 = timeit.default_timer()
        time4 = end_time1 - start_time1

        if len(topk_tables) < k:
            k = len(topk_tables)

        rtables_names = self.__remove_dup(topk_tables, k)

        if tflag == True:
            print('Schema Mapping Cost: ', time1 + time2)
            print('Joinability Computation Cost: ', time3)
            print('Totally Cost: ', time4)

        rtables = []
        for i in rtables_names:
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_joinable_tables_threshold1(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag):

        self.query = query
        self.already_map = {}
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        SM_test = SchemaMapping()
        groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
        self.query_fd = {}

        start_time1 = timeit.default_timer()

        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_tables(self.query, self.schema_element, groups_possibly_matched)
        end_time= timeit.default_timer()
        time1 += end_time - start_time

        top_tables = []
        Cache_MaxSim = {}

        rank2 = []
        rank_candidate = []

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            tableS = self.query
            tableR = self.real_tables[i]

            start_time = timeit.default_timer()
            SM, ms = self.schema_mapping(tableS, tableR, meta_mapping, gid, True)
            end_time = timeit.default_timer()
            time1 = time1 + end_time - start_time

            Cache_MaxSim[tname] = ms

            if len(SM.items()) == 0:
                continue

            tableSnotintableR = []
            for sk in tableS.columns.tolist():
                if sk not in SM:
                    tableSnotintableR.append(sk)

            vname_score =  float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))
            vname_score2 = float(max(len(tableR.columns.values), len(tableS.columns.values)) - 1) / float(len(tableR.columns.values) + len(tableSnotintableR))
            ubound = beta * vname_score2 + float(1 - beta) * Cache_MaxSim[tname]

            rank2.append(ubound)
            rank_candidate.append((tname, vname_score, SM))

        rank2 = sorted(rank2, reverse=True)
        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableR = self.real_tables[rank_candidate[i][0]]
            SM = rank_candidate[i][2]
            gid = self.table_group[rank_candidate[i][0][6:]]
            top_tables.append((rank_candidate[i][0], self.comp_table_joinable(self.query, tableR, beta, SM, gid, thres_key_prune, thres_key_cache)))

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):
            if ks + id >= len(rank_candidate):
                break

            threshold = beta * rank_candidate[ks + id][1] + rank2[0]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableR = self.real_tables[rank_candidate[ks + id][0]]
                SM = rank_candidate[ks + id][2]
                gid = self.table_group[rank_candidate[ks + id][0][6:]]
                new_score = self.comp_table_joinable(self.query, tableR, beta, SM, gid, thres_key_cache, thres_key_cache)
                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        end_time1 = timeit.default_timer()
        time3 = end_time1 - start_time1

        rtables_names = self.__remove_dup(top_tables, k)

        if tflag == True:
            print('Schema Mapping Cost: ', time1)
            print('Totally Cost: ', time3)


        rtables = []
        for i in rtables_names:
            rtables.append((i, self.real_tables[i]))


        return rtables

    def search_joinable_tables_threshold2(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag):

        self.query = query
        self.query_fd = {}
        self.already_map = {}

        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        query_col = self.sketch_query_cols(query)

        SM_test = SchemaMapping()
        #groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)

        unmatched = {}
        for i in self.schema_linking.keys():
            unmatched[i] = {}
            for j in query.columns.tolist():
                unmatched[i][j] = {}

        start_time1 = timeit.default_timer()

        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping, unmatched = SM_test.mapping_naive_tables_join(self.query, query_col, \
                                                                    self.schema_element_sample, self.schema_element, \
                                                                    unmatched, tflag, self.schema_element_dtype)
        end_time= timeit.default_timer()
        time1 += end_time - start_time


        top_tables = []
        Cache_MaxSim = {}

        rank2 = []
        rank_candidate = []

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            tableS = self.query
            tableR = self.real_tables[i]

            start_time = timeit.default_timer()
            SM,ms = self.schema_mapping(tableS, tableR, meta_mapping, gid)
            end_time = timeit.default_timer()
            time1 = time1 + end_time - start_time

            Cache_MaxSim[tname] = ms

            if len(SM.items()) == 0:
                continue

            tableSnotintableR = []
            for sk in tableS.columns.tolist():
                if sk not in SM:
                    tableSnotintableR.append(sk)

            vname_score =  float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))
            vname_score2 = float(max(len(tableR.columns.values), len(tableS.columns.values)) - 1) / float(len(tableR.columns.values) + len(tableSnotintableR))
            ubound = beta * vname_score2 + float(1 - beta) * Cache_MaxSim[tname]

            rank2.append(ubound)
            rank_candidate.append((tname, vname_score, SM))

        rank2 = sorted(rank2, reverse=True)
        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableR = self.real_tables[rank_candidate[i][0]]
            SM_real = rank_candidate[i][2]
            gid = self.table_group[rank_candidate[i][0][6:]]
            score, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_joinable_key(SM_test, self.query, tableR, beta, SM_real, gid, meta_mapping, self.schema_linking, thres_key_prune, unmatched)
            top_tables.append((rank_candidate[i][0], score, key_chosen))
            time1 += sm_time

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):
            if ks + id >= len(rank_candidate):
                break

            threshold = beta * rank_candidate[ks + id][1] + rank2[0]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableR = self.real_tables[rank_candidate[ks + id][0]]
                SM_real = rank_candidate[ks + id][2]
                gid = self.table_group[rank_candidate[ks + id][0][6:]]
                new_score, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_joinable_key(SM_test, self.query, tableR,
                                                                                         beta, SM_real, gid,
                                                                                         meta_mapping,
                                                                                         self.schema_linking,
                                                                                         thres_key_prune, unmatched)
                time1 += sm_time

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score, key_chosen))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        end_time1 = timeit.default_timer()
        time3 = end_time1 - start_time1

        rtables_names = self.__remove_dup(top_tables, k)

        if tflag == True:
            print('Schema Mapping Cost: ', time1)
            print('Totally Cost: ', time3)


        rtables = []
        for i,j in rtables_names:
            print(i,j)
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_role_sim_tables(self, query, k):
        test_class = SearchProv()
        table_names = test_class.search_topk(query,k)
        table_names = [t.split('_') for t in table_names]
        db_name = {}
        for t in table_names:
            nid = t[-1]
            vname = '_'.join(t[1:-2])
            cid = str(self.n_l2cid[nid][int(t[-2])])
            db_name[nid + ";" + vname + ";" + cid] = ""
        print(db_name)
        db_return = []
        for t in self.real_tables.keys():
            temp = t[6:].split('_')
            cid = str(temp[0])
            nid = str(temp[-1])
            vname = str('_'.join(temp[1:-2]))
            sname = nid + ";" + vname + ";" + cid
            #print(sname)
            if sname in db_name:
                db_return.append((t, self.real_tables[t]))
        return db_return

class WithProv_Optimized:

    def read_graph_of_notebook(self):
        Graphs = {}
        dependency = pd.read_sql_table('dependen', self.eng, cfg.sql_graph)#, schema='graph_model')
        line2cid = pd.read_sql_table('line2cid', self.eng, cfg.sql_graph)#, schema='graph_model')
        #print(dependency)
        #print(line2cid)

        dependency_store = {}
        line2cid_store = {}

        for index, row in dependency.iterrows():
            dependency_store[row['view_id']] = json.loads(row['view_cmd'])
        for index, row in line2cid.iterrows():
            line2cid_store[row['view_id']] = json.loads(row['view_cmd'])
        #print(dependency_store)
        #print(line2cid_store)

        for nid in dependency_store.keys():
            Graph = self.__generate_graph(nid, dependency_store[nid], line2cid_store[nid])
            #print(Graph)
            Graphs[nid] = Graph
        #print(Graphs)
        #print(line2cid_store)
        return Graphs, line2cid_store


    def __line2cid(self, dir):
        nb_l2c = {}
        files = os.listdir(dir)
        for f in files:
            ind = pickle.load(open(dir + "/" + f, "rb"))
            nb_l2c[f[:-2]] = ind
        return nb_l2c

    def __row_similarity(self, colA, colB):

        colA_value = colA[~pd.isnull(colA)].values
        colB_value = colB[~pd.isnull(colB)].values

        row_sim_upper = len(np.intersect1d(colA_value, colB_value))
        row_sim_lower = len(np.union1d(colA_value, colB_value))
        row_sim = float(row_sim_upper) / float(row_sim_lower)
        return row_sim

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
                left_node.append('var_' + ele + '_' + str(i) + '_' + str(nid))
            # print('left',left_node)

            for ele in left:
                if type(ele) is tuple or type(ele) is list:
                    ele = ele[0]

                new_node = 'var_' + ele + '_' + str(i) + '_' + str(nid)
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
                        if dep not in special_type:
                            candidate_node = 'var_' + dep + '_' + str(1) + '_' + str(nid)
                            G.add_node(candidate_node, cell_id=0, line_id=1, var=dep)
                        else:
                            candidate_node = dep + str(nid)
                            G.add_node(candidate_node, cell_id=0, line_id=1, var=dep)

                    else:
                        candidate_node = rankbyline[0][0]

                    G.add_edge(new_node, candidate_node, label=ename)

        return G

    def __col_similarity(self, tableA, tableB, SM, key_factor):

        col_sim_upper = 1 + float(len(SM.keys()) - 1) * float(key_factor)
        tableA_not_in_tableB = []
        for kyA in tableA.columns.tolist():
            if kyA not in SM:
                tableA_not_in_tableB.append(kyA)
        col_sim_lower = len(tableB.columns.values) + len(tableA_not_in_tableB)
        col_sim = float(col_sim_upper) / float(col_sim_lower)
        return col_sim

    def __remove_dup(self, ranked_list, ks):
        res = []
        for i,j,l in ranked_list:
            flg = True
            for k,m in res:
                if self.real_tables[i].equals(self.real_tables[k]):
                    flg = False
                    break
            if flg == True:
                res.append((i,l))

            if len(res) == ks:
                break
        return res

    def __dfs(self, snode):

        cell_rel = self.geng.match_one((snode,), r_type = "Containedby")
        cell_node = cell_rel.end_node
        names = []
        return_names = []
        stack = [cell_node]
        while(True):
            if len(stack) != 0:
                node = stack.pop()
                names.append(node['name'])
                for rel in self.geng.match((node, ), r_type = "Contains"):
                    return_names.append(rel.end_node['name'])

                for rel in self.geng.match((node, ), r_type="Successor"):
                    if rel.end_node['name'] in names:
                        continue
                    else:
                        stack.append(rel.end_node)

                for rel in self.geng.match((node, ), r_type = "Parent"):
                    if rel.end_node['name'] in names:
                        continue
                    else:
                        stack.append(rel.end_node)
            else:
                break

        return list(set(return_names))

    def init_schema_mapping(self):

        matcher = NodeMatcher(self.geng)

        tables_touched = []
        tables_connected = []
        for i in self.real_tables.keys():
            if i[6:] not in set(tables_touched):
                current_node = matcher.match("Var", name = i[6:]).first()
                connected_tables = self.__dfs(current_node)
                tables_touched = tables_touched + connected_tables
                tables_connected.append(connected_tables)

        self.schema_linking = {}
        self.schema_element = {}
        self.schema_element_count = {}
        self.schema_element_dtype = {}

        self.table_group = {}

        # assign each table a group id
        for idi, i in enumerate(tables_connected):
            for j in i:
                self.table_group[j] = idi

        for idi, i in enumerate(tables_connected):
            self.schema_linking[idi] = {}
            self.schema_element[idi] = {}
            self.schema_element_dtype[idi] = {}
            self.schema_element_count[idi] = {}

            for j in i:
                tname = 'rtable' + j

                if tname not in self.real_tables:
                    continue
                for col in self.real_tables[tname].columns:
                    if col not in self.schema_linking[idi]:
                        if len(self.schema_linking[idi].keys()) == 0:
                            sid = 0
                        else:
                            sid = max(list(self.schema_linking[idi].values())) + 1

                        self.schema_linking[idi][col] = sid
                        self.schema_element_dtype[idi][col] = self.real_tables[tname][col].dtype
                        self.schema_element_count[idi][col] = 1
                        self.schema_element[idi][col] = []
                        self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()
                        self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
                    else:
                        self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()
                        self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
                        self.schema_element_count[idi][col] += 1

        logging.info('There are %s groups of tables.'%len(tables_connected))

    def sample_schema(self, col_size = 100):
        self.schema_element_sample_row = {}
        for i in self.schema_element.keys():
            self.schema_element_sample_row[i] = {}
            for sc in self.schema_element[i].keys():
                if len(self.schema_element[i][sc]) < col_size:
                    self.schema_element_sample_row[i][sc] = self.schema_element[i][sc]
                else:
                    self.schema_element_sample_row[i][sc] = random.sample(self.schema_element[i][sc],col_size)

    def sketch_meta_mapping(self, sz = 10, col_size =100):
        self.schema_element_sample_col = {}
        for i in self.schema_element.keys():
            self.schema_element_sample_col[i] = {}
            if len(self.schema_element[i].keys()) <= sz:
                for sc in self.schema_element[i].keys():
                    if len(self.schema_element[i][sc]) < col_size:
                        self.schema_element_sample_col[i][sc] = self.schema_element[i][sc]
                    else:
                        self.schema_element_sample_col[i][sc] = random.sample(self.schema_element[i][sc], col_size)
            else:
                sc_choice = []
                for sc in self.schema_element[i].keys():
                    if sc == "Unnamed: 0" or "index" in sc:
                        continue
                    if (self.schema_element_dtype[i][sc] is np.dtype(float)):
                        continue
                    sc_value = list(self.schema_element[i][sc])
                    sc_choice.append((sc, float(len(set(sc_value)))/float(len(sc_value))))
                sc_choice = sorted(sc_choice, key = lambda d:d[1], reverse=True)

                count = 0
                for sc,v in sc_choice:
                    if count == sz:
                        break
                    if len(self.schema_element[i][sc]) < col_size:
                        self.schema_element_sample_col[i][sc] = self.schema_element[i][sc]
                    else:
                        self.schema_element_sample_col[i][sc] = random.sample(self.schema_element[i][sc], col_size)

                    count += 1

    def sketch_query_cols(self, query, sz = 10):
        if query.shape[1] <= sz:
            return query.columns.tolist()
        else:
            q_cols = query.columns.tolist()
            c_scores = []
            for i in q_cols:
                if i == "Unnamed: 0" or "index" in i:
                    continue
                if query[i].dtype is np.dtype(float):
                    continue
                cs_v = query[i].tolist()
                c_scores.append((i, float(len(set(cs_v)))/float(len(cs_v))))
            c_scores = sorted(c_scores, key = lambda d:d[1], reverse=True)

            q_cols_chosen = []
            c_count = 0
            for i, j in c_scores:
                if c_count == sz:
                    break
                q_cols_chosen.append(i)
                c_count += 1
            return q_cols_chosen

    def __init__(self, dbname, schema = None):
        self.query = None
        self.eng = connect2db(dbname)
        self.geng = connect2gdb()

        self.real_tables = {}

        if schema != None:
            logging.info('Indexing existing tables from data lake')
            self.tables = fetch_all_table_names(schema, self.eng)
            for i in self.tables:
                try:
                    tableR = pd.read_sql_table(i, self.eng, schema = schema)
                    if 'Unnamed: 0' in tableR.columns:
                        tableR.drop(['Unnamed: 0'], axis=1, inplace=True)
                    self.real_tables[i] = tableR
                except:
                    continue
        else:
            logging.info('Indexing views')
            self.tables = fetch_all_views(self.eng)
            for i in self.tables:
                try:
                    tableR = pd.read_sql_table(i, self.eng)
                    self.real_tables[i] = tableR
                except:
                    continue

        logging.info('%s tables detected in the database.'%len(self.real_tables.keys()))

        self.init_schema_mapping()
        self.sample_schema()
        self.sketch_meta_mapping()
        logging.info('Reading Graph of Notebooks.')
        self.Graphs, self.n_l2cid = self.read_graph_of_notebook()
        #self.n_l2cid = self.__line2cid('/Users/yizhang/PycharmProjects/sig_demo/nb_data_extension/notebook_data_extension/data_extension/related_table_lcid')

        logging.info('Data Search Extension Prepared!')

    def app_common_key(self,tableA, tableB, SM, key, thres_prune): # thres_prune = 0.2

        kyA = key
        kyB = SM[key]
        key_value_A = tableA[kyA].tolist()
        key_value_B = tableB[kyB].tolist()

        key_estimateA = float(len(set(key_value_A))) / float(len(key_value_A))
        key_estimateB = float(len(set(key_value_B))) / float(len(key_value_B))
        if min(key_estimateA, key_estimateB) <= thres_prune:
            return 0

#        other_key = list(SM.keys())
#        other_key.remove(key)
        mapped_keyA = list(SM.keys())

        if kyA not in self.query_fd:
            self.query_fd[kyA] = {}
            for idv, kv in enumerate(key_value_A):
                if kv not in self.query_fd[kyA]:
                    self.query_fd[kyA][kv] = []
                self.query_fd[kyA][kv].append(','.join(map(str, tableA[mapped_keyA].iloc[idv].tolist())))
            fd = copy.deepcopy(self.query_fd[key])
        else:
            fd = copy.deepcopy(self.query_fd[key])

        mapped_keyB = list(SM.values())
        for idv, kv in enumerate(key_value_B):
            if kv not in fd:
                fd[kv] = []
            fd[kv].append(','.join(map(str, tableB[mapped_keyB].iloc[idv].tolist())))

        key_score = 0
        for fdk in fd.keys():
            key_score = key_score + float(len(set(fd[fdk]))) / float(tableA.shape[0] + tableB.shape[0])

        return key_score

    def app_join_key(self, tableA, tableB, SM, key, thres_prune):

        kyA = key
        key_value_A = tableA[key].tolist()
        scoreA = float(len(set(key_value_A)))/float(len(key_value_A))
        if scoreA == 1:
            return 1

        kyB = SM[key]
        key_value_B = tableB[kyB].tolist()
        scoreB = float(len(set(key_value_B)))/float(len(key_value_B))
        if scoreB == 1:
            return 1

        if min(scoreA, scoreB) < thres_prune:
            return 0


        mapped_keyA = list(SM.keys())

        if kyA not in self.query_fd:
            self.query_fd[kyA] = {}
            for idv, kv in enumerate(key_value_A):
                if kv not in self.query_fd[kyA]:
                    self.query_fd[kyA][kv] = []
                self.query_fd[kyA][kv].append(','.join(map(str, tableA[mapped_keyA].iloc[idv].tolist())))
            fd = copy.deepcopy(self.query_fd[key])
        else:
            fd = copy.deepcopy(self.query_fd[key])

        mapped_keyB = list(SM.values())
        for idv, kv in enumerate(key_value_B):
            if kv in fd:
                fd[kv].append(','.join(map(str, tableB[mapped_keyB].iloc[idv].tolist())))

        key_scoreAB = 0
        for fdk in fd.keys():
            key_scoreAB += float(len(set(fd[fdk]))) / float(tableA.shape[0] + tableB.shape[0])

        temp_fd = {}
        for idv, kv in enumerate(key_value_B):
            if kv not in temp_fd:
                temp_fd[kv] = []
            temp_fd[kv].append(','.join(map(str, tableB[mapped_keyB].iloc[idv].tolist())))

        for idv, kv in enumerate(key_value_A):
            if kv in temp_fd:
                temp_fd[kv].append(','.join(map(str, tableA[mapped_keyA].iloc[idv].tolist())))

        key_scoreBA = 0
        for fdk in temp_fd.keys():
            key_scoreBA += float(len(set(temp_fd[fdk]))) / float(tableA.shape[0] + tableB.shape[0])

        return max(key_scoreAB, key_scoreBA)

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
            max_valueL.append(self.__row_similarity(tableA[i], tableB[j]))

        if len(max_valueL) > 0:
            mv = max(max_valueL)
        else:
            mv = 0

        return s_mapping, mv

    def comp_table_similarity_key(self, SM_test, tableA, tableB, beta, SM, gid, meta_mapping, schema_linking, thres_key_prune, thres_key_cache, unmatched):

        key_choice = []
        for kyA in SM.keys():
            flg = False
            if kyA in self.already_map[gid]:
                determined = self.already_map[gid][kyA]
                check_set = set(list(SM.values()))
                for ds in determined:
                    if ds.issubset(check_set):
                        flg = True
                        break
                if flg:
                    key_choice.append((kyA, self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)))
                    break

                else:
                    key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
                    key_choice.append((kyA, key_score))
                    if key_score == 1:
                        break
            else:
                key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
                key_choice.append((kyA, key_score))

        if len(key_choice) == 0:
            #for i in tableA.columns.tolist():
            #    for j in tableB.columns.tolist():
            #        unmatched[gid][i][schema_linking[gid][j]] = ''
            return 0, meta_mapping, unmatched, 0, None
        else:
            key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
            key_chosen = key_choice[0][0]
            key_factor = key_choice[0][1]

            if key_factor >= thres_key_cache:
                if key_chosen not in self.already_map[gid]:
                    self.already_map[gid][key_chosen] = []
                self.already_map[gid][key_chosen].append(set(list(SM.values())))

            SM_real, meta_mapping, unmatched, sm_time = SM_test.mapping_naive_incremental(tableA, tableB, gid, meta_mapping, schema_linking, unmatched, mapped=SM) #SM_test.mapping_naive(tableA, tableB, SM)

            row_sim = self.__row_similarity(tableA[key_chosen], tableB[SM_real[key_chosen]])
            col_sim = self.__col_similarity(tableA, tableB, SM_real, key_factor)

            return beta * col_sim + float(1 - beta) * row_sim, meta_mapping, unmatched, sm_time, key_chosen

    def comp_table_joinable_key(self, SM_test, tableA, tableB, beta, SM, gid, meta_mapping, schema_linking, thres_key_prune, unmatched):

        key_choice = []
        for kyA in SM.keys():
            key_score = self.app_join_key(tableA, tableB, SM, kyA, thres_key_prune)
            key_choice.append((kyA, key_score))

        if len(key_choice) == 0:
            return 0, meta_mapping, unmatched, 0, None
        else:
            key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
            key_chosen = key_choice[0][0]
            key_factor = key_choice[0][1]

            SM_real, meta_mapping, unmatched, sm_time = SM_test.mapping_naive_incremental(tableA, tableB, gid,
                                                                                          meta_mapping, schema_linking,
                                                                                          unmatched,
                                                                                          mapped=SM)  # SM_test.mapping_naive(tableA, tableB, SM)

            col_sim_upper = 1 + float(len(tableA.columns.values) - 1) * float(key_factor)
            tableA_not_in_tableB = []
            for kyA in tableA.columns.tolist():
                if kyA not in SM_real:
                    tableA_not_in_tableB.append(kyA)
            col_sim_lower = len(tableB.columns.values) + len(tableA_not_in_tableB) - 1
            col_sim = float(col_sim_upper) / float(col_sim_lower)

            col_sim_upper2 = 1 + float(len(tableB.columns.values) - 1) * float(key_factor)
            col_sim2 = float(col_sim_upper2) / float(col_sim_lower)

            row_sim = self.__row_similarity(tableA[key_chosen], tableB[SM_real[key_chosen]])

            score1 = beta * col_sim + float(1 - beta) * row_sim
            score2 = beta * col_sim2 + float(1 - beta) * row_sim

            return max(score1, score2), meta_mapping, unmatched, sm_time, key_chosen

    def search_similar_tables_threshold2(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag = False):

        self.query = query

        self.query_fd = {}

        self.already_map = {}
        SM_test = SchemaMapping()
        start_time1 = timeit.default_timer()

        # store the mapping computed at the very beginning
        # Initialize
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        # Choose the most possible keys from the query table
        query_col = self.sketch_query_cols(query)
        #print(query_col)

        time1 = 0
        start_time = timeit.default_timer()
        # Do mapping
        meta_mapping = SM_test.mapping_naive_tables(self.query, query_col, self.schema_element_sample_col, \
                                                               self.schema_element_dtype)

        end_time= timeit.default_timer()
        time1 += end_time - start_time

        #logging.info(str(meta_mapping))

        # Compute unmatched pairs
        unmatched = {}
        for i in meta_mapping.keys():
            unmatched[i] = {}
            for j in query.columns.tolist():
                unmatched[i][j] = {}
                if (j in query_col) and (j not in meta_mapping[i]):
                    for l in self.schema_element_sample_row[i].keys():
                        unmatched[i][j][l] = ""


        top_tables = []
        Cache_MaxSim = {}

        rank2 = []
        rank_candidate = []

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            tableS = self.query
            tableR = self.real_tables[i]

            start_time = timeit.default_timer()
            SM, ms = self.schema_mapping(tableS, tableR, meta_mapping, gid)
            end_time = timeit.default_timer()
            time1 = time1 + end_time - start_time
            Cache_MaxSim[tname] = ms

            if len(SM.items()) == 0:
                continue

            tableSnotintableR = []
            for sk in tableS.columns.tolist():
                if sk not in SM:
                    tableSnotintableR.append(sk)

            vname_score =  float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))

            vname_score2 = float(min(len(tableS.columns.tolist()), len(tableR.columns.tolist())) - 1) \
                           / float(len(tableR.columns.values) + len(tableSnotintableR) - 1)

            ubound = beta * vname_score2 + float(1 - beta) * Cache_MaxSim[tname]

            rank2.append(ubound)
            rank_candidate.append((tname, vname_score, SM))

        rank2 = sorted(rank2, reverse=True)
        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableR = self.real_tables[rank_candidate[i][0]]
            gid = self.table_group[rank_candidate[i][0][6:]]
            SM_real = rank_candidate[i][2]
            score, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_similarity_key(SM_test, self.query, tableR, beta, SM_real, gid, meta_mapping, self.schema_linking, thres_key_prune, thres_key_cache, unmatched)
            top_tables.append((rank_candidate[i][0], score, key_chosen))
            time1 += sm_time

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):
            if ks + id >= len(rank_candidate):
                break

            threshold = beta * rank_candidate[ks + id][1] + float(1 - beta) * rank2[0]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableR = self.real_tables[rank_candidate[ks + id][0]]
                gid = self.table_group[rank_candidate[ks + id][0][6:]]
                SM_real = rank_candidate[ks + id][2]
                rs, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_similarity_key(SM_test, self.query, tableR, beta, SM_real, gid,
                                                                     meta_mapping, self.schema_linking, thres_key_prune,
                                                                     thres_key_cache, unmatched)
                time1 += sm_time
                new_score = rs

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score, key_chosen))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        end_time1 = timeit.default_timer()
        time3 = end_time1 - start_time1

        logging.info("Schema Mapping Costs: %s Seconds"%time1)
        logging.info("Full Search Costs: %s Seconds"%time3)

        rtables_names = self.__remove_dup(top_tables, ks)

        rtables = []
        for i,j in rtables_names:
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_joinable_tables_threshold2(self, query, beta, k, theta, thres_key_cache, thres_key_prune):

        self.query = query
        self.query_fd = {}
        self.already_map = {}

        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        query_col = self.sketch_query_cols(query)

        SM_test = SchemaMapping()

        unmatched = {}
        for i in self.schema_linking.keys():
            unmatched[i] = {}
            for j in query.columns.tolist():
                unmatched[i][j] = {}

        start_time1 = timeit.default_timer()

        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping, unmatched = SM_test.mapping_naive_tables_join(self.query, query_col, \
                                                                    self.schema_element_sample_col, \
                                                                    self.schema_element_sample_row, \
                                                                    self.schema_element_dtype, unmatched)
        end_time= timeit.default_timer()
        time1 += end_time - start_time
        #logging.info(str(meta_mapping))
        logging.info("Initial Schema Mapping Costs: %s Seconds."%(end_time - start_time))

        top_tables = []
        Cache_MaxSim = {}

        rank2 = []
        rank_candidate = []

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            tableS = self.query
            tableR = self.real_tables[i]

            start_time = timeit.default_timer()
            SM,ms = self.schema_mapping(tableS, tableR, meta_mapping, gid)
            end_time = timeit.default_timer()
            time1 = time1 + end_time - start_time

            Cache_MaxSim[tname] = ms

            if len(SM.items()) == 0:
                continue

            tableSnotintableR = []
            for sk in tableS.columns.tolist():
                if sk not in SM:
                    tableSnotintableR.append(sk)

            vname_score =  float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))
            vname_score2 = float(max(len(tableR.columns.values), len(tableS.columns.values)) - 1) / float(len(tableR.columns.values) + len(tableSnotintableR))
            ubound = beta * vname_score2 + float(1 - beta) * Cache_MaxSim[tname]

            rank2.append(ubound)
            rank_candidate.append((tname, vname_score, SM))

        rank2 = sorted(rank2, reverse=True)
        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableR = self.real_tables[rank_candidate[i][0]]
            SM_real = rank_candidate[i][2]
            gid = self.table_group[rank_candidate[i][0][6:]]
            score, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_joinable_key(SM_test, self.query, tableR, beta, SM_real, gid, meta_mapping, self.schema_linking, thres_key_prune, unmatched)
            top_tables.append((rank_candidate[i][0], score, key_chosen))
            time1 += sm_time

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):
            if ks + id >= len(rank_candidate):
                break

            threshold = beta * rank_candidate[ks + id][1] + rank2[0]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableR = self.real_tables[rank_candidate[ks + id][0]]
                SM_real = rank_candidate[ks + id][2]
                gid = self.table_group[rank_candidate[ks + id][0][6:]]
                new_score, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_joinable_key(SM_test, self.query, tableR,
                                                                                         beta, SM_real, gid,
                                                                                         meta_mapping,
                                                                                         self.schema_linking,
                                                                                         thres_key_prune, unmatched)
                time1 += sm_time

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score, key_chosen))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        end_time1 = timeit.default_timer()
        time3 = end_time1 - start_time1

        rtables_names = self.__remove_dup(top_tables, k)

        logging.info('Schema Mapping Costs: %s Seconds'%time1)
        logging.info('Full Search Costs:%s Seconds'%time3)

        rtables = []
        for i,j in rtables_names:
            #print(i,j)
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_role_sim_tables(self, query, k):
        test_class = SearchProv(self.Graphs)
        table_names = test_class.search_topk(query,k)
        table_names = [t.split('_') for t in table_names]
        db_name = {}
        for t in table_names:
            nid = t[-1]
            vname = '_'.join(t[1:-2])
            cid = str(self.n_l2cid[nid][t[-2]])
            db_name[nid + ";" + vname + ";" + cid] = ""
        db_return = []
        for t in self.real_tables.keys():
            temp = t[6:].split('_')
            cid = str(temp[0])
            nid = str(temp[-1])
            vname = str('_'.join(temp[1:-2]))
            sname = nid + ";" + vname + ";" + cid
            if sname in db_name:
                db_return.append((t, self.real_tables[t]))
        return db_return

class SearchProv:

    def __star_dis(self, listA, listB):
        listA = sorted(listA)
        listB = sorted(listB)
        #print(listA, listB)
        lenA = len(listA)
        lenB = len(listB)
        i = 0
        j = 0
        interset = []
        while(i < lenA and j < lenB):
            #print(listA[i], listB[j], listA[i] == listB[j])
            if listA[i] == listB[j]:
                interset.append(listA[i])
                i += 1
                j += 1
            elif listA[i] < listB[j]:
                i += 1
            elif listA[i] > listB[j]:
                j += 1
            else:
                print(i,j)
        dist = abs(lenA - lenB) + max(lenA, lenB) - len(interset)
        return dist

    def Star_Mapping_Dis(self, query, root):
        mapo = {}
        mapr = {}
        distance_pair = []
        for i in query.keys():
            stara = query[i].values()
            for j in root.keys():
                starb = root[j].values()
                simAB = self.__star_dis(stara, starb)
                distance_pair.append((i, j, simAB))
        distance_pair = sorted(distance_pair, key=lambda d: d[2])
        #    print(distance_pair)

        distance_ret = 0
        for i, j, k in distance_pair:
            if i not in mapo and j not in mapr:
                mapo[i] = j
                mapr[j] = i
                distance_ret = distance_ret + k

        return distance_ret

    def Graph_Edit_Dis(self, query, k):
        distance_rank = []
        for i in self.Graphs_Dependencies.keys():
            for j in self.Graphs_Dependencies[i].keys():
                dist = self.Star_Mapping_Dis(query, self.Graphs_Dependencies[i][j])
                distance_rank.append((j, dist))
        distance_rank = sorted(distance_rank, key=lambda d: d[1])
        if k > len(distance_rank):
            k = len(distance_rank)
        res = []
        for i, j in distance_rank[:k]:
            res.append(i)
        return res

    def __get_pre_depedencies4node(self, graph, qnode):
        node_list = {}
        q = queue.Queue()
        q.put(qnode)
        while(not q.empty()):
            temp_node = q.get()
            if temp_node not in node_list:
                node_list[temp_node] = {}
            predecessors = graph.successors(temp_node)
            for n in predecessors:
                q.put(n)
                node_list[temp_node][n] = '+' + graph[temp_node][n]['label']
                successors = graph.predecessors(n)
                for s in successors:
                    if s in node_list:
                        if n not in node_list:
                            node_list[n] = {}
                        node_list[n][s] = '-' + graph[s][n]['label']
        return node_list

    def __get_all_node_dependencies(self):
        gnodes_subgraph = {}
        for g in self.Graphs.keys():
            graph = self.Graphs[g]
            gnodes_subgraph[g] = {}
            gnodes = list(graph.nodes)
            for n in gnodes:
                gnodes_subgraph[g][n] = self.__get_pre_depedencies4node(graph, n)
        return gnodes_subgraph

    def __read_models(self):
        files = os.listdir(self.model_address)
        Graphs = {}
        for i in files:
            G = pickle.load(open(self.model_address + "/" + i, "rb"))
            Graphs[i] = G
        return Graphs

    def __init__(self, graphs):
        self.Graphs = graphs
        self.Graphs_Dependencies = self.__get_all_node_dependencies()

    def search_topk(self, query, k):
        return self.Graph_Edit_Dis(query, k)

# class WithProv_Sk:
#
#     def __row_similarity(self, colA, colB):
#
#         colA_value = colA[~pd.isnull(colA)].values
#         colB_value = colB[~pd.isnull(colB)].values
#
#         row_sim_upper = len(np.intersect1d(colA_value, colB_value))
#         row_sim_lower = len(np.union1d(colA_value, colB_value))
#         row_sim = float(row_sim_upper) / float(row_sim_lower)
#         return row_sim
#
#     def __col_similarity(self, tableA, tableB, SM, key_factor):
#
#         col_sim_upper = 1 + float(len(SM.keys()) - 1) * float(key_factor)
#         tableA_not_in_tableB = []
#         for kyA in tableA.columns.tolist():
#             if kyA not in SM:
#                 tableA_not_in_tableB.append(kyA)
#         col_sim_lower = len(tableB.columns.values) + len(tableA_not_in_tableB)
#         col_sim = float(col_sim_upper) / float(col_sim_lower)
#         return col_sim
#
#     def __remove_dup(self, ranked_list, ks):
#         res = []
#         for i,j in ranked_list:
#             flg = True
#             for k in res:
#                 if self.real_tables[i].equals(self.real_tables[k]):
#                     flg = False
#                     break
#             if flg == True:
#                 res.append(i)
#
#             if len(res) == ks:
#                 break
#         return res
#
#     def dfs(self, snode):
#
#         cell_rel = self.geng.match_one((snode,), r_type = "Containedby")
#         cell_node = cell_rel.end_node
#         names = []
#         return_names = []
#         stack = [cell_node]
#         while(True):
#             if len(stack) != 0:
#                 node = stack.pop()
#                 names.append(node['name'])
#                 for rel in self.geng.match((node, ), r_type = "Contains"):
#                     return_names.append(rel.end_node['name'])
#
#                 for rel in self.geng.match((node, ), r_type="Successor"):
#                     if rel.end_node['name'] in names:
#                         continue
#                     else:
#                         stack.append(rel.end_node)
#                 for rel in self.geng.match((node, ), r_type = "Parent"):
#                     if rel.end_node['name'] in names:
#                         continue
#                     else:
#                         stack.append(rel.end_node)
#             else:
#                 break
#
#         return list(set(return_names))
#
#     def init_schema_mapping(self):
#
#         matcher = NodeMatcher(self.geng)
#
#         tables_touched = []
#         tables_connected = []
#         for i in self.real_tables.keys():
#             if i[6:] not in set(tables_touched):
#                 current_node = matcher.match("Var", name = i[6:]).first()
#                 connected_tables = self.dfs(current_node)
#                 tables_touched = tables_touched + connected_tables
#                 tables_connected.append(connected_tables)
#
#         self.schema_linking = {}
#         self.schema_element = {}
#         self.schema_element_count = {}
#
#         self.table_group = {}
#
#         # assign each table a group id
#         for idi, i in enumerate(tables_connected):
#             for j in i:
#                 self.table_group[j] = idi
#
#         for idi, i in enumerate(tables_connected):
#             self.schema_linking[idi] = {}
#             self.schema_element[idi] = {}
#             self.schema_element_count[idi] = {}
#
#             for j in i:
#                 tname = 'rtable' + j
#                 if tname not in self.real_tables:
#                     continue
#                 for col in self.TableB[tname].keys():
#                     if col not in self.schema_linking[idi]:
#                         if len(self.schema_linking[idi].keys()) == 0:
#                             sid = 0
#                         else:
#                             sid = max(list(self.schema_linking[idi].values())) + 1
#
#                         self.schema_linking[idi][col] = sid
#                         self.schema_element_count[idi][col] = 1
#                         self.schema_element[idi][col] = []
#                         self.schema_element[idi][col] += list(self.TableB[tname][col])
#                         self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
#                     else:
#                         self.schema_element[idi][col] += list(self.TableB[tname][col])
#                         self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
#                         self.schema_element_count[idi][col] += 1
#
#         print("We got ", len(tables_connected), " groups of tables.")
#
#     def sketch_meta_mapping(self, sz = 10):
#         self.schema_element_sample = {}
#         for i in self.schema_element.keys():
#             self.schema_element_sample[i] = {}
#             prob = np.array(list(self.schema_element_count[i].values()))
#             prob = prob/sum(prob)
#             sampled_col = np.random.choice(list(self.schema_element[i].keys()), sz, p = prob)
#             for sc in sampled_col:
#                 self.schema_element_sample[i][sc] = self.schema_element[i][sc]
#
#     def __init__(self, dbname, schema = None):
#         self.query = None
#         self.eng = connect2db(dbname)
#         self.geng = connect2gdb()
#
#         self.real_tables = {}
#
#         if schema != None:
#             self.tables = fetch_all_table_names(schema, self.eng)
#             for i in self.tables:
#                 try:
#                     tableR = pd.read_sql_table(i, self.eng, schema = schema)
#                     self.real_tables[i] = tableR
#                 except:
#                     continue
#         else:
#             self.tables = fetch_all_views(self.eng)
#             for i in self.tables:
#                 try:
#                     tableR = pd.read_sql_table(i, self.eng)
#                     self.real_tables[i] = tableR
#                 except:
#                     continue
#
#         print("We got ", len(self.real_tables.keys()), " tables in total.")
#
#         self.__sk_real_tables()
#         self.init_schema_mapping()
#         self.sketch_meta_mapping()
#
#         print("Data Extension Started!")
#
#     def __sk_real_tables(self):
#         self.TableB = {}
#         for i in self.real_tables.keys():
#             self.TableB[i] = {}
#             for j in self.real_tables[i].columns:
#                 if j == "Unnamed: 0" or "index" in j:
#                     continue
#                 else:
#                     self.TableB[i][j] = []
#                     colB = self.real_tables[i][j][~pd.isnull(self.real_tables[i][j])].values
#                     for k in colB:
#                         self.TableB[i][j].append(str(k))
#                     self.TableB[i][j] = np.array(list(set(self.TableB[i][j])))
#
#     def __sk_query_table(self, query):
#         self.query = {}
#         for i in query.columns:
#             if i == "Unnamed: 0" or "index" in i:
#                 continue
#             else:
#                 self.query[i] = []
#                 colA = query[i][~pd.isnull(query[i])].values
#                 for k in colA:
#                     self.query[i].append(str(k))
#                 self.query[i] = np.array(list(set(self.query[i])))
#
#     def app_common_key(self,tableA, tableB, SM, key, thres_prune): # thres_prune = 0.2
#
#         kyA = key
#         kyB = SM[key]
#         key_value_A = tableA[kyA].tolist()
#         key_value_B = tableB[kyB].tolist()
#
#         key_estimateA = float(len(set(key_value_A))) / float(len(key_value_A))
#         key_estimateB = float(len(set(key_value_B))) / float(len(key_value_B))
#         if min(key_estimateA, key_estimateB) <= thres_prune:
#             return 0
#
# #        other_key = list(SM.keys())
# #        other_key.remove(key)
#         mapped_keyA = list(SM.keys())
#
#         if kyA not in self.query_fd:
#             self.query_fd[kyA] = {}
#             for idv, kv in enumerate(key_value_A):
#                 if kv not in self.query_fd[kyA]:
#                     self.query_fd[kyA][kv] = []
#                 self.query_fd[kyA][kv].append(','.join(map(str, tableA[mapped_keyA].iloc[idv].tolist())))
#             fd = copy.deepcopy(self.query_fd[key])
#         else:
#             fd = copy.deepcopy(self.query_fd[key])
#
#         mapped_keyB = list(SM.values())
#         for idv, kv in enumerate(key_value_B):
#             if kv not in fd:
#                 fd[kv] = []
#             fd[kv].append(','.join(map(str, tableB[mapped_keyB].iloc[idv].tolist())))
#
#         key_score = 0
#         for fdk in fd.keys():
#             key_score = key_score + float(len(set(fd[fdk]))) / float(tableA.shape[0] + tableB.shape[0])
#
#         return key_score
#
#     def schema_mapping(self, tableA_s, tableB_s, meta_mapping, gid, tflag, tableA, tableB):
#         s_mapping = {}
#         t_mapping = {}
#         for i in tableA_s.keys():
#             if i not in meta_mapping[gid]:
#                 continue
#             t_mapping[self.schema_linking[gid][meta_mapping[gid][i]]] = i
#
#         for i in tableB_s.keys():
#             if self.schema_linking[gid][i] in t_mapping:
#                 if tableB[i].dtype != tableA[t_mapping[self.schema_linking[gid][i]]].dtype:
#                     continue
#                 s_mapping[t_mapping[self.schema_linking[gid][i]]] = i
#
#         if tflag == True:
#             max_valueL = []
#             for i in s_mapping.keys():
#                 j = s_mapping[i]
#                 colA = tableA_s[i]
#                 colB = tableB_s[j]
#                 max_valueL.append(float(len(np.intersect1d(colA, colB)))/float(len(np.union1d(colA, colB))))
#
#             if len(max_valueL) > 0:
#                 mv = max(max_valueL)
#             else:
#                 mv = 0
#
#             return s_mapping, mv
#         else:
#             return s_mapping
#
#     def comp_table_similarity_row(self, tableA, tableB, key, SM, sample_size):
#
#         value_setb = set(tableB[SM[key]].tolist())
#
#         sim_sum = 0
#         sim_count = 0
#         for idi, i in enumerate(tableA[key].tolist()):
#             if sim_count == sample_size:
#                 break
#             if i in value_setb:
#                 dfB = tableB.loc[tableB[SM[key]] == i]
#                 sim_array = []
#                 for index, row in dfB.iterrows():
#                     a = np.array(list(map(str, tableA.iloc[idi].tolist())))
#                     b = np.array(list(map(str, row.tolist())))
#                     #a = a[~pd.isnull(a)].values
#                     #b = b[~pd.isnull(b)].values
#                     try:
#                         sim_array.append(float(len(np.intersect1d(a,b)))/float(len(np.union1d(a,b))))
#                     except:
#                         print(a)
#                         print(b)
#                 sim_array = np.array(sim_array)
#                 sim_sum = sim_sum + np.mean(sim_array)
#                 sim_count += 1
#         if sim_count == 0:
#             return 0
#         else:
#             return float(sim_sum)/float(sim_count)
#
#     def comp_table_similarity(self, tableA, tableB, beta, SM, gid, thres_key_prune, thres_key_cache):
#
#         key_choice = []
#         for kyA in SM.keys():
#             flg = False
#             if kyA in self.already_map[gid]:
#                 determined = self.already_map[gid][kyA]
#                 check_set = set(list(SM.values()))
#                 for ds in determined:
#                     if ds.issubset(check_set):
#                         flg = True
#                         break
#                 if flg:
#                     key_choice.append((kyA, self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)))
#                     break
#
#                 else:
#                     key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
#                     key_choice.append((kyA, key_score))
#                     if key_score == 1:
#                         break
#             else:
#                 key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
#                 key_choice.append((kyA, key_score))
#
#         if len(key_choice) == 0:
#             return 0
#         else:
#             key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
#             key_chosen = key_choice[0][0]
#             key_factor = key_choice[0][1]
#
#             if key_factor >= thres_key_cache:
#                 if key_chosen not in self.already_map[gid]:
#                     self.already_map[gid][key_chosen] = []
#                 self.already_map[gid][key_chosen].append(set(list(SM.values())))
#
#             row_sim = self.__row_similarity(tableA[key_chosen], tableB[SM[key_chosen]])
#             col_sim = self.__col_similarity(tableA, tableB, SM, key_factor)
#
#             return beta * col_sim + float(1 - beta) * row_sim
#
#     def comp_table_joinable(self, tableA, tableB, beta, SM, gid, thres_key_prune, thres_key_cache):
#
#         key_choice = []
#         for kyA in SM.keys():
#             flg = False
#             if kyA in self.already_map[gid]:
#                 determined = self.already_map[gid][kyA]
#                 check_set = set(list(SM.values()))
#                 for ds in determined:
#                     if ds.issubset(check_set):
#                         flg = True
#                         break
#                 if flg:
#                     key_choice.append((kyA, self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)))
#                     break
#
#                 else:
#                     key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
#                     key_choice.append((kyA, key_score))
#                     if key_score == 1:
#                         break
#             else:
#                 key_score = self.app_common_key(tableA, tableB, SM, kyA, thres_key_prune)
#                 key_choice.append((kyA, key_score))
#
#         key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
#         key_chosen = key_choice[0][0]
#         key_factor = key_choice[0][1]
#
#         if key_factor >= thres_key_cache:
#             if key_chosen not in self.already_map[gid]:
#                 self.already_map[gid][key_chosen] = []
#             self.already_map[gid][key_chosen].append(set(list(SM.values())))
#
#         row_sim = self.__row_similarity(tableA[key_chosen], tableB[SM[key_chosen]])
#
#         col_sim_upper = 1 + float(len(tableA.columns.values) - 1) * float(key_factor)
#         tableA_not_in_tableB = []
#         for kyA in tableA.columns.tolist():
#             if kyA not in SM:
#                 tableA_not_in_tableB.append(kyA)
#         col_sim_lower = len(tableB.columns.values) + len(tableA_not_in_tableB)
#         col_sim = float(col_sim_upper) / float(col_sim_lower)
#
#         col_sim_upper2 = 1 + float(len(tableB.columns.values) - 1) * float(key_factor)
#         col_sim2 = float(col_sim_upper2) / float(col_sim_lower)
#
#         score1 = beta * col_sim + float(1 - beta) * row_sim
#         score2 = beta * col_sim2 + float(1 - beta) * row_sim
#
#         return max(score1, score2)
#
#     # search_similar_tables(query, beta, k, 0.9, 0.2, True)
#     def search_similar_tables(self, query, beta, k, thres_key_cache, thres_key_prune, tflag):
#
#         self.query = query
#         self.already_map = {}
#         for i in self.schema_linking.keys():
#             self.already_map[i] = {}
#
#         topk_tables = []
#         SM_test = SchemaMapping()
#         groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
#         self.query_fd = {}
#
#         start_time1 = timeit.default_timer()
#         time1 = 0
#         start_time = timeit.default_timer()
#         meta_mapping = SM_test.mapping_naive_tables(self.query, self.schema_element, groups_possibly_matched)
#         end_time= timeit.default_timer()
#         time1 += end_time - start_time
#         time2 = 0
#         time3 = 0
#
#         for i in self.real_tables.keys():
#
#             tname = i
#             gid = self.table_group[tname[6:]]
#             if gid not in meta_mapping:
#                 continue
#
#             start_time = timeit.default_timer()
#             SM = self.schema_mapping(self.query, self.real_tables[i], meta_mapping, gid, False)
#             end_time = timeit.default_timer()
#             time2 += end_time - start_time
#
#             if len(SM.keys()) == 0:
#                 continue
#
#             start_time = timeit.default_timer()
#             table_sim = self.comp_table_similarity(self.query, self.real_tables[i], beta, SM, gid, thres_key_prune, thres_key_cache)
#             end_time = timeit.default_timer()
#             time3 += end_time - start_time
#             topk_tables.append((i, table_sim))
#
#         topk_tables = sorted(topk_tables, key = lambda d:d[1], reverse=True)
#         end_time1 = timeit.default_timer()
#         time4 = end_time1 - start_time1
#
#         if len(topk_tables) < k:
#             k = len(topk_tables)
#
#         rtables_names = self.__remove_dup(topk_tables, k)
#
#         if tflag == True:
#             print('Schema Mapping Cost: ', time1 + time2)
#             print('Similarity Computation Cost: ', time3)
#             print('Totally Cost: ', time4)
#
#         rtables = []
#         for i in rtables_names:
#             rtables.append((i, self.real_tables[i]))
#
#         return rtables
#
#     def search_similar_tables_threshold1(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag):
#
#         self.query = query
#         self.already_map = {}
#         for i in self.schema_linking.keys():
#             self.already_map[i] = {}
#
#         SM_test = SchemaMapping()
#         groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
#         self.query_fd = {}
#
#         start_time1 = timeit.default_timer()
#
#         time1 = 0
#         start_time = timeit.default_timer()
#         meta_mapping = SM_test.mapping_naive_tables(self.query, self.schema_element, groups_possibly_matched)
#         end_time= timeit.default_timer()
#         time1 += end_time - start_time
#
#         top_tables = []
#         Cache_MaxSim = {}
#
#         rank2 = []
#         rank_candidate = []
#
#         for i in self.real_tables.keys():
#
#             tname = i
#             gid = self.table_group[tname[6:]]
#             if gid not in meta_mapping:
#                 continue
#
#             tableS = self.query
#             tableR = self.real_tables[i]
#
#             start_time = timeit.default_timer()
#             SM, ms = self.schema_mapping(tableS, tableR, meta_mapping, gid, True)
#             end_time = timeit.default_timer()
#             time1 = time1 + end_time - start_time
#
#             Cache_MaxSim[tname] = ms
#
#             if len(SM.items()) == 0:
#                 continue
#
#             tableSnotintableR = []
#             for sk in tableS.columns.tolist():
#                 if sk not in SM:
#                     tableSnotintableR.append(sk)
#
#             vname_score =  float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))
#             vname_score2 = float(len(SM.keys()) - 1) / float(len(tableR.columns.values) + len(tableSnotintableR) - 1)
#             ubound = beta * vname_score2 + float(1 - beta) * Cache_MaxSim[tname]
#
#             rank2.append(ubound)
#             rank_candidate.append((tname, vname_score, SM))
#
#         rank2 = sorted(rank2, reverse=True)
#         rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)
#
#         if len(rank_candidate) == 0:
#             return []
#
#         if len(rank_candidate) > k:
#             ks = k
#         else:
#             ks = len(rank_candidate)
#
#         for i in range(ks):
#             tableR = self.real_tables[rank_candidate[i][0]]
#             SM = rank_candidate[i][2]
#             gid = self.table_group[rank_candidate[i][0][6:]]
#             top_tables.append((rank_candidate[i][0], self.comp_table_similarity(self.query, tableR, beta, SM, gid, thres_key_prune, thres_key_cache)))
#
#         top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
#         min_value = top_tables[-1][1]
#
#         ks = ks - 1
#         id = 0
#         while (True):
#             if ks + id >= len(rank_candidate):
#                 break
#
#             threshold = beta * rank_candidate[ks + id][1] + rank2[0]
#
#             if threshold <= min_value * theta:
#                 break
#             else:
#                 id = id + 1
#                 if ks + id >= len(rank_candidate):
#                     break
#
#                 tableR = self.real_tables[rank_candidate[ks + id][0]]
#                 SM = rank_candidate[ks + id][2]
#                 gid = self.table_group[rank_candidate[ks + id][0][6:]]
#                 new_score = self.comp_table_similarity(self.query, tableR, beta, SM, gid, thres_key_cache, thres_key_cache)
#                 if new_score <= min_value:
#                     continue
#                 else:
#                     top_tables.append((rank_candidate[ks + id][0], new_score))
#                     top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
#                     min_value = top_tables[ks][1]
#
#         end_time1 = timeit.default_timer()
#         time3 = end_time1 - start_time1
#
#         if tflag == True:
#             print('Schema Mapping Cost: ', time1)
#             print('Totally Cost: ', time3)
#
#         rtables_names = self.__remove_dup(top_tables, ks)
#
#         rtables = []
#         for i in rtables_names:
#             rtables.append((i, self.real_tables[i]))
#
#         return rtables
#
#     def search_similar_tables_threshold2(self, query, nlimit, beta, k, theta, thres_key_cache, thres_key_prune, tflag, rsz):
#
#         self.__sk_query_table(query)
#
#         if len(self.query.keys()) <= nlimit:
#             key_map = list(self.query.keys())
#         else:
#             key_map = []
#             key_prob = {}
#             for i in self.query.keys():
#                 key_prob[i] = len(self.query[i])
#             key_prob = sorted(key_prob.items(), key=lambda d:d[1], reverse=True)
#             key_prob = key_prob[:nlimit]
#             for i, j in key_prob:
#                 key_map.append(i)
#
#         #print(key_map)
#         self.already_map = {}
#         groups_possibly_matched = []
#         for i in self.schema_linking.keys():
#             self.already_map[i] = {}
#             groups_possibly_matched.append(i)
#
#         SM_test = SchemaMapping_SK()
#         #groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
#         self.query_fd = {}
#
#         start_time1 = timeit.default_timer()
#
#         time1 = 0
#         start_time = timeit.default_timer()
#         meta_mapping = SM_test.mapping_naive_tables(self.query, key_map, self.schema_element, groups_possibly_matched)
#         end_time= timeit.default_timer()
#         time1 += end_time - start_time
#
#         #print("meta mapping")
#         #print(meta_mapping)
#
#         top_tables = []
#
#         rank2 = []
#         rank_candidate = []
#
#         for i in self.TableB.keys():
#
#             tname = i
#             gid = self.table_group[tname[6:]]
#             if gid not in meta_mapping:
#                 continue
#
#             tableS_s = self.query
#             tableR_s = self.TableB[i]
#             tableS = query
#             tableR = self.real_tables[i]
#
#             start_time = timeit.default_timer()
#             SM, ms = self.schema_mapping(tableS_s, tableR_s, meta_mapping, gid, True, tableS, tableR)
#             end_time = timeit.default_timer()
#             time1 = time1 + end_time - start_time
#
#             if len(SM.items()) == 0:
#                 continue
#             #else:
#             #    print(SM)
#
#             key_choice = []
#             for kyA in SM.keys():
#                 flg = False
#                 if kyA in self.already_map[gid]:
#                     determined = self.already_map[gid][kyA]
#                     check_set = set(list(SM.values()))
#                     for ds in determined:
#                         if ds.issubset(check_set):
#                             flg = True
#                             break
#                     if flg:
#                         key_choice.append((kyA, self.app_common_key(tableS, tableR, SM, kyA, thres_key_prune)))
#                         break
#
#                     else:
#                         key_score = self.app_common_key(tableS, tableR, SM, kyA, thres_key_prune)
#                         key_choice.append((kyA, key_score))
#                         if key_score == 1:
#                             break
#                 else:
#                     key_score = self.app_common_key(tableS, tableR, SM, kyA, thres_key_prune)
#                     key_choice.append((kyA, key_score))
#
#             key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
#             key_chosen = key_choice[0][0]
#             key_factor = key_choice[0][1]
#
#             if key_factor >= thres_key_cache:
#                 if key_chosen not in self.already_map[gid]:
#                     self.already_map[gid][key_chosen] = []
#                 self.already_map[gid][key_chosen].append(set(list(SM.values())))
#
#             colA_value = tableS_s[key_chosen]
#             colB_value = tableR_s[SM[key_chosen]]
#             #colA_value = tableS[key_chosen][~pd.isnull(tableS[key_chosen])].values
#             #colB_value = tableR[SM[key_chosen]][~pd.isnull(tableR[SM[key_chosen]])].values
#             key_sim_col = SM_test.jaccard_similarity(colA_value, colB_value)
#
#             vname_score = key_sim_col
#             colb_upper = float(1 + float(max(len(tableS.columns), len(tableR.columns)) - 1) * float(key_factor))
#             colb_lower = float(len(tableR.columns) + len(tableS.columns) - max(len(tableS.columns), len(tableR.columns)))
#             ubound = float(colb_upper) / float(colb_lower)
#
#             rank2.append(ubound)
#             rank_candidate.append((tname, vname_score, SM, key_chosen))
#
#         rank2 = sorted(rank2, reverse=True)
#         rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)
#         #print('rank candiate: ')
#         #print(rank_candidate)
#
#         if len(rank_candidate) == 0:
#             return []
#
#         if len(rank_candidate) > k:
#             ks = k
#         else:
#             ks = len(rank_candidate)
#
#         for i in range(ks):
#             tableR = self.real_tables[rank_candidate[i][0]]
#             #gid = self.table_group[rank_candidate[i][0][6:]]
#             rs = self.comp_table_similarity_row(query, tableR, rank_candidate[i][3], rank_candidate[i][2], rsz)
#             cs = rank_candidate[i][1]
#
# #            start_time = timeit.default_timer()
# #            SM_real, z = SM_test.mapping_naive(self.query, tableR)
# #            end_time = timeit.default_timer()
# #            time1 = time1 + end_time - start_time
#
#             top_tables.append((rank_candidate[i][0], float(beta * cs) + float((1 - beta) * rs)))
#             #top_tables.append((rank_candidate[i][0], self.comp_table_similarity(self.query, tableR, beta, SM_real, gid, thres_key_prune, thres_key_cache)))
#
#         top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
#         min_value = top_tables[-1][1]
#
#         ks = ks - 1
#         id = 0
#         while (True):
#             if ks + id >= len(rank_candidate):
#                 break
#
#             threshold = beta * rank_candidate[ks + id][1] + float(1 - beta) * rank2[0]
#
#             if threshold <= min_value * theta:
#                 break
#             else:
#                 id = id + 1
#                 if ks + id >= len(rank_candidate):
#                     break
#
#                 tableR = self.real_tables[rank_candidate[ks + id][0]]
#                 rs = self.comp_table_similarity_row(query, tableR, rank_candidate[ks + id][3], rank_candidate[ks + id][2], rsz)
#                 cs = rank_candidate[ks + id][1]
#                 #start_time = timeit.default_timer()
#                 #SM_real, z = SM_test.mapping_naive(self.query, tableR)
#                 #end_time = timeit.default_timer()
#                 #time1 = time1 + end_time - start_time
#
# #                gid = self.table_group[rank_candidate[ks + id][0][6:]]
#                 new_score = float(beta * cs) + float((1 - beta) * rs)
#                 #new_score = self.comp_table_similarity(self.query, tableR, beta, SM_real, gid, thres_key_cache, thres_key_cache)
#                 if new_score <= min_value:
#                     continue
#                 else:
#                     top_tables.append((rank_candidate[ks + id][0], new_score))
#                     top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
#                     min_value = top_tables[ks][1]
#
#         end_time1 = timeit.default_timer()
#         time3 = end_time1 - start_time1
#
#         if tflag == True:
#             print('Schema Mapping Cost: ', time1)
#             print('Totally Cost: ', time3)
#
#         rtables_names = self.__remove_dup(top_tables, ks)
#
#         rtables = []
#         for i in rtables_names:
#             rtables.append((i, self.real_tables[i]))
#
#         return rtables
#
#     def search_joinable_tables(self, query, beta, k, thres_key_cache, thres_key_prune, tflag):
#
#         self.query = query
#         self.already_map = {}
#         for i in self.schema_linking.keys():
#             self.already_map[i] = {}
#
#         topk_tables = []
#         SM_test = SchemaMapping()
#         groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
#         self.query_fd = {}
#
#         start_time1 = timeit.default_timer()
#         time1 = 0
#         start_time = timeit.default_timer()
#         meta_mapping = SM_test.mapping_naive_tables(self.query, self.schema_element, groups_possibly_matched)
#         end_time= timeit.default_timer()
#         time1 += end_time - start_time
#         time2 = 0
#         time3 = 0
#
#         for i in self.real_tables.keys():
#
#             tname = i
#             gid = self.table_group[tname[6:]]
#             if gid not in meta_mapping:
#                 continue
#
#             start_time = timeit.default_timer()
#             SM = self.schema_mapping(self.query, self.real_tables[i], meta_mapping, gid, False)
#             end_time = timeit.default_timer()
#             time2 += end_time - start_time
#
#             if len(SM.keys()) == 0:
#                 continue
#
#             start_time = timeit.default_timer()
#             table_sim = self.comp_table_joinable(self.query, self.real_tables[i], beta, SM, gid, thres_key_prune, thres_key_cache)
#             end_time = timeit.default_timer()
#             time3 += end_time - start_time
#             topk_tables.append((i, table_sim))
#
#         topk_tables = sorted(topk_tables, key = lambda d:d[1], reverse=True)
#         end_time1 = timeit.default_timer()
#         time4 = end_time1 - start_time1
#
#         if len(topk_tables) < k:
#             k = len(topk_tables)
#
#         rtables_names = self.__remove_dup(topk_tables, k)
#
#         if tflag == True:
#             print('Schema Mapping Cost: ', time1 + time2)
#             print('Joinability Computation Cost: ', time3)
#             print('Totally Cost: ', time4)
#
#         rtables = []
#         for i in rtables_names:
#             rtables.append((i, self.real_tables[i]))
#
#         return rtables
#
#     def search_joinable_tables_threshold(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag):
#
#         self.query = query
#         self.already_map = {}
#         for i in self.schema_linking.keys():
#             self.already_map[i] = {}
#
#         SM_test = SchemaMapping()
#         groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
#         self.query_fd = {}
#
#         start_time1 = timeit.default_timer()
#
#         time1 = 0
#         start_time = timeit.default_timer()
#         meta_mapping = SM_test.mapping_naive_tables(self.query, self.schema_element, groups_possibly_matched)
#         end_time= timeit.default_timer()
#         time1 += end_time - start_time
#
#         top_tables = []
#         Cache_MaxSim = {}
#
#         rank2 = []
#         rank_candidate = []
#
#         for i in self.real_tables.keys():
#
#             tname = i
#             gid = self.table_group[tname[6:]]
#             if gid not in meta_mapping:
#                 continue
#
#             tableS = self.query
#             tableR = self.real_tables[i]
#
#             start_time = timeit.default_timer()
#             SM, ms = self.schema_mapping(tableS, tableR, meta_mapping, gid, True)
#             end_time = timeit.default_timer()
#             time1 = time1 + end_time - start_time
#
#             Cache_MaxSim[tname] = ms
#
#             if len(SM.items()) == 0:
#                 continue
#
#             tableSnotintableR = []
#             for sk in tableS.columns.tolist():
#                 if sk not in SM:
#                     tableSnotintableR.append(sk)
#
#             vname_score =  float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))
#             vname_score2 = float(max(len(tableR.columns.values), len(tableS.columns.values)) - 1) / float(len(tableR.columns.values) + len(tableSnotintableR))
#             ubound = beta * vname_score2 + float(1 - beta) * Cache_MaxSim[tname]
#
#             rank2.append(ubound)
#             rank_candidate.append((tname, vname_score, SM))
#
#         rank2 = sorted(rank2, reverse=True)
#         rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)
#
#         if len(rank_candidate) == 0:
#             return []
#
#         if len(rank_candidate) > k:
#             ks = k
#         else:
#             ks = len(rank_candidate)
#
#         for i in range(ks):
#             tableR = self.real_tables[rank_candidate[i][0]]
#             SM = rank_candidate[i][2]
#             gid = self.table_group[rank_candidate[i][0][6:]]
#             top_tables.append((rank_candidate[i][0], self.comp_table_joinable(self.query, tableR, beta, SM, gid, thres_key_prune, thres_key_cache)))
#
#         top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
#         min_value = top_tables[-1][1]
#
#         ks = ks - 1
#         id = 0
#         while (True):
#             if ks + id >= len(rank_candidate):
#                 break
#
#             threshold = beta * rank_candidate[ks + id][1] + rank2[0]
#
#             if threshold <= min_value * theta:
#                 break
#             else:
#                 id = id + 1
#                 if ks + id >= len(rank_candidate):
#                     break
#
#                 tableR = self.real_tables[rank_candidate[ks + id][0]]
#                 SM = rank_candidate[ks + id][2]
#                 gid = self.table_group[rank_candidate[ks + id][0][6:]]
#                 new_score = self.comp_table_joinable(self.query, tableR, beta, SM, gid, thres_key_cache, thres_key_cache)
#                 if new_score <= min_value:
#                     continue
#                 else:
#                     top_tables.append((rank_candidate[ks + id][0], new_score))
#                     top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
#                     min_value = top_tables[ks][1]
#
#         end_time1 = timeit.default_timer()
#         time3 = end_time1 - start_time1
#
#         rtables_names = self.__remove_dup(top_tables, k)
#
#         if tflag == True:
#             print('Schema Mapping Cost: ', time1)
#             print('Totally Cost: ', time3)
#
#
#         rtables = []
#         for i in rtables_names:
#             rtables.append((i, self.real_tables[i]))
#
#         return rtables
#
#     def search_role_sim_tables(self, query, k):
#         test_class = SearchProv()
#         test_class.search_topk(query,k)


def search_tables(search_test, var_df, mode, code, var_name):

    #if mode == 0:
    #    table = pd.read_sql_table(var_df, search_test.eng, schema = 'rowstore')
    #    return table.to_html()
    #else:
    query_table = var_df #dataframe
    query_name = var_name #name of the table

    if mode == 1:
        logging.info("Search for Similar Tables!")
        tables = search_test.search_similar_tables_threshold2(query_table, 0.5, 10, 1.5, 0.9, 0.2)
        logging.info("%s Similar Tables are returned!"%len(tables))
        #tables = search_test.search_similar_tables_threshold2(query_table, 10, 0.5, 5, 1, 0.9, 0.2, True, 10)
    elif mode == 2:
        logging.info("Search for Joinable Tables!")
        tables = search_test.search_joinable_tables_threshold2(query_table, 0.1, 10, 1.5, 0.9, 0.2)
        logging.info("%s Joinable Tables are returned!"%len(tables))
    elif mode == 3:
        logging.info("Search for Provenance Similar Tables!")
        code = '\n'.join([t for t in code.split('\\n') if len(t)> 0 and t[0]!='%'])
        code = '\''.join(code.split('\\\''))
        line_id = last_line_var(var_name, code)
        dependency = parse_code(code)
        graph = generate_graph(dependency)
        query_name = 'var_' + var_name + '_' + str(line_id)
        query_node = pre_vars(query_name, graph)
        tables = search_test.search_role_sim_tables(query_node, 10)
        logging.info("%s Provenance Similar Tables are returned!"%len(tables))



    if len(tables) == 0:
        return ""
    else:
        vardic = [{'varName': v[0], 'varType': type(v[1]).__name__, 'varSize': str(v[1].size), 'varContent': v[1].to_html(index_names = True, justify = 'center', max_rows = 10, max_cols = 5, header = True)} for v in tables] # noqa
        return json.dumps(vardic)
    #return search_test

#print(search_tables())