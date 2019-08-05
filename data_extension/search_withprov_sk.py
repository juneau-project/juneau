from py2neo import NodeMatcher
import os
import pandas as pd
import numpy as np
import pickle
import copy
import timeit

from data_extension.table_db import connect2gdb, connect2db
from data_extension.table_db import fetch_all_table_names, fetch_all_views
from data_extension.schemamapping import SchemaMapping
from data_extension.schemamapping_sk import SchemaMapping_SK
from data_extension.search import SearchProv
from data_extension.search_tables import SearchTables
import data_extension.config as cfg

import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class WithProv_Sk(SearchTables):

    # def __row_similarity(self, colA, colB):
    #
    #     colA_value = colA[~pd.isnull(colA)].values
    #     colB_value = colB[~pd.isnull(colB)].values
    #
    #     row_sim_upper = len(np.intersect1d(colA_value, colB_value))
    #     row_sim_lower = len(np.union1d(colA_value, colB_value))
    #     row_sim = float(row_sim_upper) / float(row_sim_lower)
    #     return row_sim
    #
    # def __col_similarity(self, tableA, tableB, SM, key_factor):
    #
    #     col_sim_upper = 1 + float(len(SM.keys()) - 1) * float(key_factor)
    #     tableA_not_in_tableB = []
    #     for kyA in tableA.columns.tolist():
    #         if kyA not in SM:
    #             tableA_not_in_tableB.append(kyA)
    #     col_sim_lower = len(tableB.columns.values) + len(tableA_not_in_tableB)
    #     col_sim = float(col_sim_upper) / float(col_sim_lower)
    #     return col_sim
    #
    # def remove_dup(self, ranked_list, ks):
    #     res = []
    #     for i, j in ranked_list:
    #         flg = True
    #         for k in res:
    #             if self.real_tables[i].equals(self.real_tables[k]):
    #                 flg = False
    #                 break
    #         if flg == True:
    #             res.append(i)
    #
    #         if len(res) == ks:
    #             break
    #     return res
    #
    # def dfs(self, snode):
    #
    #     cell_rel = self.geng.match_one((snode,), r_type="Containedby")
    #     cell_node = cell_rel.end_node
    #     names = []
    #     return_names = []
    #     stack = [cell_node]
    #     while (True):
    #         if len(stack) != 0:
    #             node = stack.pop()
    #             names.append(node['name'])
    #             for rel in self.geng.match((node,), r_type="Contains"):
    #                 return_names.append(rel.end_node['name'])
    #
    #             for rel in self.geng.match((node,), r_type="Successor"):
    #                 if rel.end_node['name'] in names:
    #                     continue
    #                 else:
    #                     stack.append(rel.end_node)
    #             for rel in self.geng.match((node,), r_type="Parent"):
    #                 if rel.end_node['name'] in names:
    #                     continue
    #                 else:
    #                     stack.append(rel.end_node)
    #         else:
    #             break
    #
    #     return list(set(return_names))

    def init_schema_mapping(self):

        matcher = NodeMatcher(self.geng)

        tables_touched = []
        tables_connected = []
        for i in self.real_tables.keys():
            if i[6:] not in set(tables_touched):
                current_node = matcher.match("Var", name=i[6:]).first()
                connected_tables = self.dfs(current_node)
                tables_touched = tables_touched + connected_tables
                tables_connected.append(connected_tables)

        self.schema_linking = {}
        self.schema_element = {}
        self.schema_element_count = {}

        self.table_group = {}

        # assign each table a group id
        for idi, i in enumerate(tables_connected):
            for j in i:
                self.table_group[j] = idi

        for idi, i in enumerate(tables_connected):
            self.schema_linking[idi] = {}
            self.schema_element[idi] = {}
            self.schema_element_count[idi] = {}

            for j in i:
                tname = 'rtable' + j
                if tname not in self.real_tables:
                    continue
                for col in self.TableB[tname].keys():
                    if col not in self.schema_linking[idi]:
                        if len(self.schema_linking[idi].keys()) == 0:
                            sid = 0
                        else:
                            sid = max(list(self.schema_linking[idi].values())) + 1

                        self.schema_linking[idi][col] = sid
                        self.schema_element_count[idi][col] = 1
                        self.schema_element[idi][col] = []
                        self.schema_element[idi][col] += list(self.TableB[tname][col])
                        self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
                    else:
                        self.schema_element[idi][col] += list(self.TableB[tname][col])
                        self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
                        self.schema_element_count[idi][col] += 1

        print("We got ", len(tables_connected), " groups of tables.")

    def sketch_meta_mapping(self, sz=10):
        self.schema_element_sample = {}
        for i in self.schema_element.keys():
            self.schema_element_sample[i] = {}
            prob = np.array(list(self.schema_element_count[i].values()))
            prob = prob / sum(prob)
            sampled_col = np.random.choice(list(self.schema_element[i].keys()), sz, p=prob)
            for sc in sampled_col:
                self.schema_element_sample[i][sc] = self.schema_element[i][sc]

    def __init__(self, dbname, schema=None):
        # self.query = None
        # self.eng = connect2db(dbname)
        # self.geng = connect2gdb()
        #
        # self.real_tables = {}
        #
        # if schema != None:
        #     self.tables = fetch_all_table_names(schema, self.eng)
        #     for i in self.tables:
        #         try:
        #             tableR = pd.read_sql_table(i, self.eng, schema=schema)
        #             self.real_tables[i] = tableR
        #         except:
        #             continue
        # else:
        #     self.tables = fetch_all_views(self.eng)
        #     for i in self.tables:
        #         try:
        #             tableR = pd.read_sql_table(i, self.eng)
        #             self.real_tables[i] = tableR
        #         except:
        #             continue
        #
        # print("We got ", len(self.real_tables.keys()), " tables in total.")

        super().__init__(dbname, schema)

        self.__sk_real_tables()
        # self.init_schema_mapping()
        self.sketch_meta_mapping()

        print("Data Extension Started!")

    def __sk_real_tables(self):
        self.TableB = {}
        for i in self.real_tables.keys():
            self.TableB[i] = {}
            for j in self.real_tables[i].columns:
                if j == "Unnamed: 0" or "index" in j:
                    continue
                else:
                    self.TableB[i][j] = []
                    colB = self.real_tables[i][j][~pd.isnull(self.real_tables[i][j])].values
                    for k in colB:
                        self.TableB[i][j].append(str(k))
                    self.TableB[i][j] = np.array(list(set(self.TableB[i][j])))

    def __sk_query_table(self, query):
        self.query = {}
        for i in query.columns:
            if i == "Unnamed: 0" or "index" in i:
                continue
            else:
                self.query[i] = []
                colA = query[i][~pd.isnull(query[i])].values
                for k in colA:
                    self.query[i].append(str(k))
                self.query[i] = np.array(list(set(self.query[i])))

    def schema_mapping(self, tableA_s, tableB_s, meta_mapping, gid, tflag, tableA, tableB):
        s_mapping = {}
        t_mapping = {}
        for i in tableA_s.keys():
            if i not in meta_mapping[gid]:
                continue
            t_mapping[self.schema_linking[gid][meta_mapping[gid][i]]] = i

        for i in tableB_s.keys():
            if self.schema_linking[gid][i] in t_mapping:
                if tableB[i].dtype != tableA[t_mapping[self.schema_linking[gid][i]]].dtype:
                    continue
                s_mapping[t_mapping[self.schema_linking[gid][i]]] = i

        if tflag == True:
            max_valueL = []
            for i in s_mapping.keys():
                j = s_mapping[i]
                colA = tableA_s[i]
                colB = tableB_s[j]
                max_valueL.append(float(len(np.intersect1d(colA, colB))) / float(len(np.union1d(colA, colB))))

            if len(max_valueL) > 0:
                mv = max(max_valueL)
            else:
                mv = 0

            return s_mapping, mv
        else:
            return s_mapping

    # search_similar_tables(query, beta, k, 0.9, 0.2, True)
    def search_similar_tables(self, query, beta, k, thres_key_cache, thres_key_prune, tflag):

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
        end_time = timeit.default_timer()
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
            table_sim = self.comp_table_similarity(self.query, self.real_tables[i], beta, SM, gid, thres_key_prune,
                                                   thres_key_cache)
            end_time = timeit.default_timer()
            time3 += end_time - start_time
            topk_tables.append((i, table_sim))

        topk_tables = sorted(topk_tables, key=lambda d: d[1], reverse=True)
        end_time1 = timeit.default_timer()
        time4 = end_time1 - start_time1

        if len(topk_tables) < k:
            k = len(topk_tables)

        rtables_names = self.remove_dup(topk_tables, k)

        if tflag == True:
            print('Schema Mapping Cost: ', time1 + time2)
            print('Similarity Computation Cost: ', time3)
            print('Totally Cost: ', time4)

        rtables = []
        for i in rtables_names:
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_similar_tables_threshold1(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag):

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
        end_time = timeit.default_timer()
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

            vname_score = float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))
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
            top_tables.append((rank_candidate[i][0],
                               self.comp_table_similarity(self.query, tableR, beta, SM, gid, thres_key_prune,
                                                          thres_key_cache)))

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
                new_score = self.comp_table_similarity(self.query, tableR, beta, SM, gid, thres_key_cache,
                                                       thres_key_cache)
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

        rtables_names = self.remove_dup(top_tables, ks)

        rtables = []
        for i in rtables_names:
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_similar_tables_threshold2(self, query, nlimit, beta, k, theta, thres_key_cache, thres_key_prune, tflag,
                                         rsz):

        self.__sk_query_table(query)

        if len(self.query.keys()) <= nlimit:
            key_map = list(self.query.keys())
        else:
            key_map = []
            key_prob = {}
            for i in self.query.keys():
                key_prob[i] = len(self.query[i])
            key_prob = sorted(key_prob.items(), key=lambda d: d[1], reverse=True)
            key_prob = key_prob[:nlimit]
            for i, j in key_prob:
                key_map.append(i)

        # print(key_map)
        self.already_map = {}
        groups_possibly_matched = []
        for i in self.schema_linking.keys():
            self.already_map[i] = {}
            groups_possibly_matched.append(i)

        SM_test = SchemaMapping_SK()
        # groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
        self.query_fd = {}

        start_time1 = timeit.default_timer()

        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_tables(self.query, key_map, self.schema_element, groups_possibly_matched)
        end_time = timeit.default_timer()
        time1 += end_time - start_time

        # print("meta mapping")
        # print(meta_mapping)

        top_tables = []

        rank2 = []
        rank_candidate = []

        for i in self.TableB.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            tableS_s = self.query
            tableR_s = self.TableB[i]
            tableS = query
            tableR = self.real_tables[i]

            start_time = timeit.default_timer()
            SM, ms = self.schema_mapping(tableS_s, tableR_s, meta_mapping, gid, True, tableS, tableR)
            end_time = timeit.default_timer()
            time1 = time1 + end_time - start_time

            if len(SM.items()) == 0:
                continue
            # else:
            #    print(SM)

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
                        key_choice.append((kyA, self.app_common_key(tableS, tableR, SM, kyA, thres_key_prune)))
                        break

                    else:
                        key_score = self.app_common_key(tableS, tableR, SM, kyA, thres_key_prune)
                        key_choice.append((kyA, key_score))
                        if key_score == 1:
                            break
                else:
                    key_score = self.app_common_key(tableS, tableR, SM, kyA, thres_key_prune)
                    key_choice.append((kyA, key_score))

            key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
            key_chosen = key_choice[0][0]
            key_factor = key_choice[0][1]

            if key_factor >= thres_key_cache:
                if key_chosen not in self.already_map[gid]:
                    self.already_map[gid][key_chosen] = []
                self.already_map[gid][key_chosen].append(set(list(SM.values())))

            colA_value = tableS_s[key_chosen]
            colB_value = tableR_s[SM[key_chosen]]
            # colA_value = tableS[key_chosen][~pd.isnull(tableS[key_chosen])].values
            # colB_value = tableR[SM[key_chosen]][~pd.isnull(tableR[SM[key_chosen]])].values
            key_sim_col = SM_test.jaccard_similarity(colA_value, colB_value)

            vname_score = key_sim_col
            colb_upper = float(1 + float(max(len(tableS.columns), len(tableR.columns)) - 1) * float(key_factor))
            colb_lower = float(
                len(tableR.columns) + len(tableS.columns) - max(len(tableS.columns), len(tableR.columns)))
            ubound = float(colb_upper) / float(colb_lower)

            rank2.append(ubound)
            rank_candidate.append((tname, vname_score, SM, key_chosen))

        rank2 = sorted(rank2, reverse=True)
        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)
        # print('rank candiate: ')
        # print(rank_candidate)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):
            tableR = self.real_tables[rank_candidate[i][0]]
            # gid = self.table_group[rank_candidate[i][0][6:]]
            rs = self.comp_table_similarity_row(query, tableR, rank_candidate[i][3], rank_candidate[i][2], rsz)
            cs = rank_candidate[i][1]

            #            start_time = timeit.default_timer()
            #            SM_real, z = SM_test.mapping_naive(self.query, tableR)
            #            end_time = timeit.default_timer()
            #            time1 = time1 + end_time - start_time

            top_tables.append((rank_candidate[i][0], float(beta * cs) + float((1 - beta) * rs)))
            # top_tables.append((rank_candidate[i][0], self.comp_table_similarity(self.query, tableR, beta, SM_real, gid, thres_key_prune, thres_key_cache)))

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
                rs = self.comp_table_similarity_row(query, tableR, rank_candidate[ks + id][3],
                                                    rank_candidate[ks + id][2], rsz)
                cs = rank_candidate[ks + id][1]
                # start_time = timeit.default_timer()
                # SM_real, z = SM_test.mapping_naive(self.query, tableR)
                # end_time = timeit.default_timer()
                # time1 = time1 + end_time - start_time

                #                gid = self.table_group[rank_candidate[ks + id][0][6:]]
                new_score = float(beta * cs) + float((1 - beta) * rs)
                # new_score = self.comp_table_similarity(self.query, tableR, beta, SM_real, gid, thres_key_cache, thres_key_cache)
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

        rtables_names = self.remove_dup(top_tables, ks)

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
        groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
        self.query_fd = {}

        start_time1 = timeit.default_timer()
        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_tables(self.query, self.schema_element, groups_possibly_matched)
        end_time = timeit.default_timer()
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
            table_sim = self.comp_table_joinable(self.query, self.real_tables[i], beta, SM, gid, thres_key_prune,
                                                 thres_key_cache)
            end_time = timeit.default_timer()
            time3 += end_time - start_time
            topk_tables.append((i, table_sim))

        topk_tables = sorted(topk_tables, key=lambda d: d[1], reverse=True)
        end_time1 = timeit.default_timer()
        time4 = end_time1 - start_time1

        if len(topk_tables) < k:
            k = len(topk_tables)

        rtables_names = self.remove_dup(topk_tables, k)

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
        groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
        self.query_fd = {}

        start_time1 = timeit.default_timer()

        time1 = 0
        start_time = timeit.default_timer()
        meta_mapping = SM_test.mapping_naive_tables(self.query, self.schema_element, groups_possibly_matched)
        end_time = timeit.default_timer()
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

            vname_score = float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))
            vname_score2 = float(max(len(tableR.columns.values), len(tableS.columns.values)) - 1) / float(
                len(tableR.columns.values) + len(tableSnotintableR))
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
            top_tables.append((rank_candidate[i][0],
                               self.comp_table_joinable(self.query, tableR, beta, SM, gid, thres_key_prune,
                                                        thres_key_cache)))

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
                new_score = self.comp_table_joinable(self.query, tableR, beta, SM, gid, thres_key_cache,
                                                     thres_key_cache)
                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        end_time1 = timeit.default_timer()
        time3 = end_time1 - start_time1

        rtables_names = self.remove_dup(top_tables, k)

        if tflag == True:
            print('Schema Mapping Cost: ', time1)
            print('Totally Cost: ', time3)

        rtables = []
        for i in rtables_names:
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_role_sim_tables(self, query, k):
        test_class = SearchProv()
        test_class.search_topk(query, k)
