import logging
import timeit

import numpy as np
from py2neo import NodeMatcher

from data_extension.schema_mapping.schemamapping import SchemaMapping
from data_extension.search.search_prov_code import SearchProv
from data_extension.search.search_tables import SearchTables

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

class WithProv(SearchTables):
    schema_linking = {}
    schema_element = {}
    schema_element_count = {}
    schema_element_dtype = {}
    query_fd = {}
    table_group = {}

    def init_schema_mapping(self):

        logging.info("Start Reading From Neo4j!")

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
        super().__init__(dbname, schema)

        self.index()

        logging.info('Data Search Extension Started!')


    def index(self):
        self.sketch_meta_mapping()
        try:
            self.n_l2cid = self.line2cid('~/similar_table_lcid')
        except FileNotFoundError:
            logging.info('Unable to find list of similar table LCIDs')

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

    # search_similar_tables(query, beta, k, 0.9, 0.2, True)
    def search_similar_tables(self, query, beta, k, thres_key_cache, thres_key_prune, tflag = False):

        self.query = query

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
            table_sim = self.comp_table_similarity(self.query, self.real_tables[i], beta, SM, gid, thres_key_prune, thres_key_cache)
            end_time = timeit.default_timer()
            time3 += end_time - start_time
            topk_tables.append((i, table_sim))

        topk_tables = sorted(topk_tables, key = lambda d:d[1], reverse=True)
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

    def search_similar_tables_threshold1(self, query, beta, k, theta, thres_key_cache, thres_key_prune, tflag = False):

        self.query = query
        query_col = self.sketch_query_cols(query)

        self.already_map = {}
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        SM_test = SchemaMapping()
        groups_possibly_matched = SM_test.mapping_naive_groups(self.query, self.schema_element_sample)
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

        rtables_names = self.remove_dup(top_tables, ks)

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

        rtables_names = self.remove_dup(top_tables, ks)

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

        rtables_names = self.remove_dup(topk_tables, k)

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

        rtables_names = self.remove_dup(top_tables, k)

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

        rtables_names = self.remove_dup(top_tables, k)

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

