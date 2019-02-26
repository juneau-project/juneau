from py2neo import NodeMatcher
#from table_db import user_name, password, dbname
from table_db import connect2gdb, connect2db
from table_db import fetch_all_table_names, fetch_all_views
from table_db import SchemaMapping

import json
import pandas as pd
import numpy as np
from sys import getsizeof

import random
import sys
import timeit
import copy

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO




def _getsizeof(x):
    # return the size of variable x. Amended version of sys.getsizeof
    # which also supports ndarray, Series and DataFrame
    if type(x).__name__ in ['ndarray', 'Series']:
        return x.nbytes
    elif type(x).__name__ == 'DataFrame':
        return x.memory_usage().sum()
    else:
        return getsizeof(x)

class WithProv:

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
        for i,j in ranked_list:
            flg = True
            for k in res:
                if self.real_tables[i].equals(self.real_tables[k]):
                    flg = False
                    break
            if flg == True:
                res.append(i)

            if len(res) == ks:
                break
        return res

    def dfs(self, snode):

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
                connected_tables = self.dfs(current_node)
                tables_touched = tables_touched + connected_tables
                tables_connected.append(connected_tables)

        self.schema_linking = {}
        self.schema_element = {}
        self.table_group = {}

        # assign each table a group id
        for idi, i in enumerate(tables_connected):
            for j in i:
                self.table_group[j] = idi

        for idi, i in enumerate(tables_connected):
            self.schema_linking[idi] = {}
            self.schema_element[idi] = {}

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
                        self.schema_element[idi][col] = []
                        self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()
                    else:
                        self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()

        print("We got ", len(tables_connected), " groups of tables.")

    def __init__(self, dbname, schema = None):
        self.query = None
        self.eng = connect2db(dbname)
        self.geng = connect2gdb()

        self.real_tables = {}

        if schema != None:
            self.tables = fetch_all_table_names(schema, self.eng)
            for i in self.tables:
                try:
                    tableR = pd.read_sql_table(i, self.eng, schema = schema)
                    self.real_tables[i] = tableR
                except:
                    continue
        else:
            self.tables = fetch_all_views(self.eng)
            for i in self.tables:
                try:
                    tableR = pd.read_sql_table(i, self.eng)
                    self.real_tables[i] = tableR
                except:
                    continue

        print("We got ", len(self.real_tables.keys()), " tables in total.")

        self.init_schema_mapping()

        print("Data Extension Started!")

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

    def schema_mapping(self, tableA, tableB, meta_mapping, gid, tflag):
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

        if tflag == True:
            max_valueL = []
            for i in s_mapping.keys():
                j = s_mapping[i]
                colA = tableA[i][~pd.isnull(tableA[i])].values
                colB = tableB[j][~pd.isnull(tableB[j])].values
                max_valueL.append(float(len(np.intersect1d(colA, colB)))/float(len(np.union1d(colA, colB))))

            if len(max_valueL) > 0:
                mv = max(max_valueL)
            else:
                mv = 0

            return s_mapping, mv
        else:
            return s_mapping

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

    # search_similar_tables(query, beta, k, 0.9, 0.2, True)
    def search_similar_tables(self, query, beta, k, thres_key_cache, thres_key_prune, tflag):

        self.query = query
        self.already_map = {}
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        topk_tables = []
        SM_test = SchemaMapping()
        self.query_fd = {}

        start_time1 = timeit.default_timer()
        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_groups(self.query, self.schema_element)
        end_time= timeit.default_timer()
        time1 += end_time - start_time
        time2 = 0
        time3 = 0

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]

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

    def search_similar_tables_threshold(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag):

        self.query = query
        self.already_map = {}
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        SM_test = SchemaMapping()
        self.query_fd = {}

        start_time1 = timeit.default_timer()

        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_groups(self.query, self.schema_element)
        end_time= timeit.default_timer()
        time1 += end_time - start_time

        top_tables = []
        Cache_MaxSim = {}

        rank2 = []
        rank_candidate = []

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]

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

    def search_joinable_tables(self, query, beta, k, thres_key_cache, thres_key_prune, tflag):

        self.query = query
        self.already_map = {}
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        topk_tables = []
        SM_test = SchemaMapping()
        self.query_fd = {}

        start_time1 = timeit.default_timer()
        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_groups(self.query, self.schema_element)
        end_time= timeit.default_timer()
        time1 += end_time - start_time
        time2 = 0
        time3 = 0

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]

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

    def search_joinable_tables_threshold(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag):

        self.query = query
        self.already_map = {}
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        SM_test = SchemaMapping()
        self.query_fd = {}

        start_time1 = timeit.default_timer()

        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_groups(self.query, self.schema_element)
        end_time= timeit.default_timer()
        time1 += end_time - start_time

        top_tables = []
        Cache_MaxSim = {}

        rank2 = []
        rank_candidate = []

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]

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


def search_tables(search_test, varname, mode):

    if mode == 0:
        table = pd.read_sql_table(varname, search_test.eng, schema = 'rowstore')
        return table.to_html()
    else:
        query_table = varname
        #test = #Strawman(eng, query_table, dbinfo['schema'], 10, 50)
        print(mode)
        if mode == 1:
            tables = search_test.search_similar_tables(query_table, 0.5, 5, 0.9, 0.2, True)
        elif mode == 2:
            tables = search_test.search_joinable_tables(query_table, 0.1, 5, 0.9, 0.2, True)
        print(len(tables), "tables return!")
        if len(tables) == 0:
            return ""
        else:
            vardic = [{'varName': v[0], 'varType': type(v[1]).__name__, 'varSize': str(v[1].size), 'varContent': v[1].to_html(index_names = True, justify = 'center', max_rows = 10, max_cols = 5, header = True)} for v in tables] # noqa
            return json.dumps(vardic)
    #return search_test

#print(search_tables())