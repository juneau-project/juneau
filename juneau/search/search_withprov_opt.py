import ast
import json
import logging
import random
import sys
import timeit

import networkx as nx
import numpy as np
import pandas as pd

import data_extension.config as cfg
from data_extension.funclister import FuncLister
from data_extension.schema_mapping.schemamapping import SchemaMapping
from data_extension.search import special_type
from data_extension.search.search_prov_code import SearchProv
from data_extension.search.search_withprov import WithProv
from data_extension.table_db import generate_graph
from data_extension.table_db import pre_vars

#logging.basicConfig(level=logging.DEBUG)

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)



class WithProv_Optimized(WithProv):

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
        dependency = pd.read_sql_table('dependen', self.eng, schema = cfg.sql_graph)#, schema='graph_model')
        line2cid = pd.read_sql_table('line2cid', self.eng, schema = cfg.sql_graph)#, schema='graph_model')
        lastliid = pd.read_sql_table('lastliid', self.eng, schema = cfg.sql_graph)

        dependency_store = {}
        line2cid_store = {}
        lastliid_store = {}

        for index, row in dependency.iterrows():
            dependency_store[row['view_id']] = json.loads(row['view_cmd'])

        for index, row in line2cid.iterrows():
            line2cid_store[row['view_id']] = json.loads(row['view_cmd'])

        for index, row in lastliid.iterrows():
            lastliid_store[row['view_id']] = json.loads(row['view_cmd'])

        for id, nid in enumerate(dependency_store.keys()):
            #Graph = self.__generate_graph(nid, dependency_store[nid], line2cid_store[nid])
            #print(Graph)
            try:
                line_id = lastliid_store[nid]

                if line_id == 0:
                    continue

                nid_name = nid.split("_")[-1]
                Graph = self.__generate_graph(nid_name, dependency_store[nid], line2cid_store[nid])

                if len(list(Graph.nodes)) == 0:
                    continue

                var_name = "_".join(nid.split("_")[1:-1])
                query_name = 'var_' + var_name + '_' + str(line2cid_store[nid][str(line_id)]) + "_" + str(nid_name)

                if Graph.has_node(query_name):
                    query_node = pre_vars(query_name, Graph)
                    Graphs[nid] = query_node
            except:
                logging.error("Can not generate the graph  " + str(id) + " " + str(sys.exc_info()[0]) + " !")

        return Graphs, line2cid_store

    def __generate_query_node_from_code(self, var_name, code):

        code = '\n'.join([t for t in code.split('\\n') if len(t)> 0 and t[0]!='%' and t[0] != '#'])
        code = '\''.join(code.split('\\\''))
        code = code.split('\n')
        dependency, _, all_code = self.__parse_code(code)
        logging.info(all_code)
        logging.info(dependency)
        line_id = self.__last_line_var(var_name, all_code)
        logging.info(line_id)
        #dependency = parse_code(code)
        graph = generate_graph(dependency)
        logging.info("Output Graph")
        logging.info(list(graph.nodes))

        query_name = 'var_' + var_name + '_' + str(line_id)

        query_node = pre_vars(query_name, graph)
        return query_node

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
            # print('left',left_node)

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

    def __parse_code(self, code_list):

        test = FuncLister()
        all_code = ""
        line2cid = {}

        lid = 1
        fflg = False
        for cid, cell in enumerate(code_list):
            #logging.info(cid)
            #logging.info(cell)
            codes = cell.split("\\n")
            new_codes = []
            for code in codes:
                if code[:3].lower() == 'def':
                    fflg = True
                    continue

                temp_code = code.strip(" ")
                temp_code = temp_code.strip("\t")

                if temp_code[:6].lower() == 'return':
                    fflg = False
                    continue


                code = code.strip("\n")
                code = code.strip(" ")
                code = code.split("\"")
                code = "'".join(code)
                code = code.split("\\")
                code = "".join(code)


                line2cid[lid] = cid
                lid = lid + 1
                if len(code) == 0:
                    continue
                if code[0] == '%':
                    continue
                if code[0] == '#':
                    continue

                try:
                    ast.parse(code)
                    if fflg == False:
                        new_codes.append(code)
                except:
                    logging.info(code)

            all_code = all_code + '\n'.join(new_codes) + '\n'

        all_code = all_code.strip("\n")

        tree = ast.parse(all_code)
        test.visit(tree)
        return test.dependency, line2cid, all_code

    def __last_line_var(self, varname, code):
        ret = 0
        code = code.split("\n")
        for id, i in enumerate(code):
            if '=' not in i:
                continue
            j = i.split('=')
            j = [t.strip(" ") for t in j]

            if varname in j[0]:
                if varname == j[0][-len(varname):]:
                    ret = id + 1
        return ret

    # def init_schema_mapping(self):
    #
    #     matcher = NodeMatcher(self.geng)
    #
    #     tables_touched = []
    #     tables_connected = []
    #     for i in self.real_tables.keys():
    #         if i[6:] not in set(tables_touched):
    #             current_node = matcher.match("Var", name = i[6:]).first()
    #             connected_tables = super().dfs(current_node)
    #             tables_touched = tables_touched + connected_tables
    #             tables_connected.append(connected_tables)
    #
    #     self.schema_linking = {}
    #     self.schema_element = {}
    #     self.schema_element_count = {}
    #     self.schema_element_dtype = {}
    #
    #     self.table_group = {}
    #
    #     # assign each table a group id
    #     for idi, i in enumerate(tables_connected):
    #         for j in i:
    #             self.table_group[j] = idi
    #
    #     for idi, i in enumerate(tables_connected):
    #         self.schema_linking[idi] = {}
    #         self.schema_element[idi] = {}
    #         self.schema_element_dtype[idi] = {}
    #         self.schema_element_count[idi] = {}
    #
    #         for j in i:
    #             tname = 'rtable' + j
    #
    #             if tname not in self.real_tables:
    #                 continue
    #             for col in self.real_tables[tname].columns:
    #                 if col not in self.schema_linking[idi]:
    #                     if len(self.schema_linking[idi].keys()) == 0:
    #                         sid = 0
    #                     else:
    #                         sid = max(list(self.schema_linking[idi].values())) + 1
    #
    #                     self.schema_linking[idi][col] = sid
    #                     self.schema_element_dtype[idi][col] = self.real_tables[tname][col].dtype
    #                     self.schema_element_count[idi][col] = 1
    #                     self.schema_element[idi][col] = []
    #                     self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()
    #                     self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
    #                 else:
    #                     self.schema_element[idi][col] += self.real_tables[tname][col][self.real_tables[tname][col].notnull()].tolist()
    #                     self.schema_element[idi][col] = list(set(self.schema_element[idi][col]))
    #                     self.schema_element_count[idi][col] += 1
    #
    #     logging.info('There are %s groups of tables.'%len(tables_connected))

    def sample_rows_for_each_column(self, row_size = 1000):
        self.schema_element_sample_row = {}
        for i in self.schema_element.keys():
            self.schema_element_sample_row[i] = {}
            for sc in self.schema_element[i].keys():
                if len(self.schema_element[i][sc]) < row_size:
                    self.schema_element_sample_row[i][sc] = self.schema_element[i][sc]
                else:
                    self.schema_element_sample_row[i][sc] = random.sample(self.schema_element[i][sc],row_size)

    def sketch_column_and_row_for_meta_mapping(self, sz = 5, row_size =1000):
        self.schema_element_sample_col = {}
        for i in self.schema_element.keys():
            self.schema_element_sample_col[i] = {}
            if len(self.schema_element[i].keys()) <= sz:
                for sc in self.schema_element[i].keys():
                    if len(self.schema_element[i][sc]) < row_size:
                        self.schema_element_sample_col[i][sc] = self.schema_element[i][sc]
                    else:
                        self.schema_element_sample_col[i][sc] = random.sample(self.schema_element[i][sc], row_size)
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
                    if len(self.schema_element[i][sc]) < row_size:
                        self.schema_element_sample_col[i][sc] = self.schema_element[i][sc]
                    else:
                        self.schema_element_sample_col[i][sc] = random.sample(self.schema_element[i][sc], row_size)

                    count += 1

    def sketch_query_cols(self, query, sz = 5):
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

        logging.info('Data Search Extension Prepared!')

    def index(self):
        self.sample_rows_for_each_column()
        self.sketch_column_and_row_for_meta_mapping()
        logging.info('Reading Graph of Notebooks.')
        self.Graphs, self.n_l2cid = self.read_graph_of_notebook()

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

    def search_additional_training_data(self, query, k, code, var_name, beta, theta):

        #introduce the schema mapping class
        self.index()

        SM_test = SchemaMapping()

        #choose only top possible key columns
        query_col_valid = self.sketch_query_cols(query)

        #do partial schema mapping
        partial_mapping = SM_test.mapping_naive_tables(query, query_col_valid, self.schema_element_sample_col, self.schema_element_dtype)

        unmatched = {}
        for i in partial_mapping.keys():
            unmatched[i] = {}
            for j in query.columns.tolist():
                unmatched[i][j] = {}
                if (j in query_col_valid) and (j not in partial_mapping[i]):
                    for l in self.schema_element[i].keys():
                        unmatched[i][j][l] = ""

        prov_class = SearchProv(self.Graphs)
        query_node = self.__generate_query_node_from_code(var_name, code)
        table_prov_rank = prov_class.search_score_rank(query_node)
        table_prov_score = {}


        for i, j in table_prov_rank:
            table_prov_score["rtable" + i] = j
        logging.info(table_prov_score)
            # if i[:3] != "var":
            #     continue
            #
            # t = i.split("_")
            #
            # #nid = t[-1]
            # nid = "_".join(t[1:])
            # logging.info(i)
            # logging.info(nid)
            #
            # if(nid not in self.n_l2cid):
            #     print("notebook " + str(nid) + " does not exist in " + str(self.n_l2cid.keys()))
            #
            # vname = '_'.join(t[1:-2])
            #
            # if(t[-2] not in self.n_l2cid[nid]):
            #     continue
            #
            # cid = int(self.n_l2cid[nid][t[-2]])
            #
            # #table_prov_score["rtable" + str(cid) + "_" + vname.lower() + "_" + str(nid)] = j
            # table_prov_score["rtable" + str(cid) + "_" + str(nid)] = j


        top_tables = []
        rank_candidate = []
        rank2 = []

        for i in self.real_tables.keys():
            tname = i
            if tname not in table_prov_score:
                logging.info(tname)
                continue
            else:
                gid = self.table_group[tname[6:]]
                if gid not in partial_mapping:
                    continue

                tableS = query
                tableR = self.real_tables[i]
                SM, ms = self.schema_mapping(tableS, tableR, partial_mapping, gid)
                rank_candidate.append((tname, float(1)/float(table_prov_score[tname] + 1), SM))

                upp_col_sim = float(min(tableS.shape[1], tableR.shape[1])) / float(max(tableS.shape[1], tableR.shape[1]))
                rank2.append(upp_col_sim)

        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)
        rank2 = sorted(rank2, reverse=True)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)

        for i in range(ks):

            tableS = query
            tableR = self.real_tables[rank_candidate[i][0]]
            gid = self.table_group[rank_candidate[i][0][6:]]
            SM_real = rank_candidate[i][2]
            SM_real, meta_mapping, unmatched, sm_time = SM_test.mapping_naive_incremental(query, tableR, gid, partial_mapping, self.schema_linking, unmatched, mapped=SM_real)
            score = float(beta) * self.col_similarity(tableS, tableR, SM_real, 1) + float(1 - beta) * rank_candidate[i][1]
            top_tables.append((rank_candidate[i][0], score))

        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)

        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):
            if ks + id >= len(rank_candidate):
                break

            threshold = float(beta) * rank2[ks + id] + float( 1 - beta) * rank_candidate[ks + id][1]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableR = self.real_tables[rank_candidate[ks + id][0]]
                gid = self.table_group[rank_candidate[ks + id][0][6:]]
                SM_real = rank_candidate[ks + id][2]
                SM_real, meta_mapping, unmatched, sm_time = SM_test.mapping_naive_incremental(query, tableR, gid,
                                                                                              partial_mapping,
                                                                                              self.schema_linking,
                                                                                              unmatched, mapped=SM_real)

                new_score = float(beta) * self.col_similarity(query, tableR, SM_real, 1) + float(1-beta)*rank_candidate[i][1]

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]


        #logging.info("Schema Mapping Costs: %s Seconds" % time1)
        #logging.info("Full Search Costs: %s Seconds" % time3)

        rtables_names = self.remove_dup2(top_tables, ks)

        rtables = []
        for i in rtables_names:
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_alternative_features(self, query, k, code, var_name, alpha, beta, gamma, theta, thres_key_prune, thres_key_cache):

        # choose only top possible key columns
        query_col = self.sketch_query_cols(query)

        #introduce the schema mapping class
        SM_test = SchemaMapping()
        #do partial schema mapping
        partial_mapping = SM_test.mapping_naive_tables(query, query_col, self.schema_element_sample_col, self.schema_element_dtype)

        unmatched = {}
        for i in partial_mapping.keys():
            unmatched[i] = {}
            for j in query.columns.tolist():
                unmatched[i][j] = {}
                if (j in query_col) and (j not in partial_mapping[i]):
                    for l in self.schema_element[i].keys():
                        unmatched[i][j][l] = ""

        self.query_fd = {}
        self.already_map = {}
        for i in self.schema_linking.keys():
            self.already_map[i] = {}

        prov_class = SearchProv(self.Graphs)
        query_node = self.__generate_query_node_from_code(var_name, code)

        table_prov_rank = prov_class.search_score_rank(query_node)
        table_prov_score = {}
        for i, j in table_prov_rank:
            table_prov_score["rtable" + i.lower()] = j

        logging.info(table_prov_score)
            # if i[:3] != "var":
            #     continue
            #
            # t = i.split("_")
            #
            # nid = t[-1]
            #
            # if (nid not in self.n_l2cid):
            #     print("notebook" + str(nid) + " does not exist")
            #
            # vname = '_'.join(t[1:-2])
            #
            # if (t[-2] not in self.n_l2cid[nid]):
            #     continue
            #
            # cid = int(self.n_l2cid[nid][t[-2]])
            #
            # table_prov_score["rtable" + str(cid) + "_" + vname.lower() + "_" + str(nid)] = j

        top_tables = []
        rank_candidate = []
        rank2 = []

        tableS = query
        for i in self.real_tables.keys():
            tname = i

            if tname not in table_prov_score:
                continue
            else:
                logging.info(tname)
                gid = self.table_group[tname[6:]]
                if gid not in partial_mapping:
                    continue


                tableR = self.real_tables[i]

                SM, ms = self.schema_mapping(tableS, tableR, partial_mapping, gid)

                if len(SM.items()) == 0:
                    continue

                tableSnotintableR = []
                for sk in tableS.columns.tolist():
                    if sk not in SM:
                        tableSnotintableR.append(sk)

                upper_bound_col_score1 = float(1) / float(len(tableR.columns.values) + len(tableSnotintableR))

                upper_bound_col_score =  (upper_bound_col_score1 + float(min(len(tableS.columns.tolist()), len(tableR.columns.tolist())) - 1) \
                               / float(len(tableR.columns.values) + len(tableSnotintableR) - 1))

                upper_bound_row_score = ms / float(abs(tableR.shape[0] - tableS.shape[0]) + 1)

                rank2.append(float(alpha) * upper_bound_col_score + float(beta) * upper_bound_row_score)

                rank_candidate.append((tname, float(table_prov_score[tname]), SM))

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

            col_sim, row_sim, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_similarity_key(SM_test, query,
                                                                                                 tableR, SM_real,
                                                                                                 gid, partial_mapping,
                                                                                                 self.schema_linking,
                                                                                                 thres_key_prune,
                                                                                                 thres_key_cache,
                                                                                                 unmatched)

            score = float(alpha) * (col_sim) + float(beta) * row_sim / float(abs(tableR.shape[0] - tableS.shape[0]) + 1) + float( gamma) * rank_candidate[i][1]

            logging.info(rank_candidate[i][0])
            logging.info(col_sim * alpha)
            logging.info(row_sim * beta / float(abs(tableR.shape[0] - tableS.shape[0]) + 1))
            logging.info(rank_candidate[i][1] * gamma)
            logging.info(score)

            logging.info("\n")
            top_tables.append((rank_candidate[i][0], score, key_chosen))


        top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
        min_value = top_tables[-1][1]

        ks = ks - 1
        id = 0
        while (True):

            if ks + id >= len(rank_candidate):
                break

            threshold = float(gamma) * rank_candidate[ks + id][1] + rank2[ks+id]

            if threshold <= min_value * theta:
                break
            else:
                id = id + 1
                if ks + id >= len(rank_candidate):
                    break

                tableR = self.real_tables[rank_candidate[ks + id][0]]
                gid = self.table_group[rank_candidate[ks + id][0][6:]]
                SM_real = rank_candidate[ks + id][2]
                col_sim, row_sim, meta_mapping, unmatched, sm_time, key_chosen = self.comp_table_similarity_key(SM_test,
                                                                                                                query,
                                                                                                                tableR,
                                                                                                                SM_real,
                                                                                                                gid,
                                                                                                                partial_mapping,
                                                                                                                self.schema_linking,
                                                                                                                thres_key_prune,
                                                                                                                thres_key_cache,
                                                                                                                unmatched)
                new_score = float(alpha) * (col_sim) + float(beta) * row_sim / float(abs(tableR.shape[0] - tableS.shape[0]) + 1)  + float(gamma) * \
                                                                               rank_candidate[ks + id][1]
                logging.info(rank_candidate[ks + id][0])
                logging.info(col_sim * alpha)
                logging.info(row_sim * beta / float(abs(tableR.shape[0] - tableS.shape[0]) + 1))
                logging.info(rank_candidate[ks + id][1] * gamma)
                logging.info(new_score)

                logging.info("\n")

                if new_score <= min_value:
                    continue
                else:
                    top_tables.append((rank_candidate[ks + id][0], new_score, key_chosen))
                    top_tables = sorted(top_tables, key=lambda d: d[1], reverse=True)
                    min_value = top_tables[ks][1]

        #logging.info("Schema Mapping Costs: %s Seconds" % time1)
        #logging.info("Full Search Costs: %s Seconds" % time3)

        rtables_names = self.remove_dup(top_tables, ks)

        rtables = []
        for i, j in rtables_names:
            rtables.append((i, self.real_tables[i]))

        return rtables

    def search_joinable_data(self, query, k, thres_key_prune):

        # choose only top possible key columns
        query_col = self.sketch_query_cols(query)

        #introduce the schema mapping class
        SM_test = SchemaMapping(sim_thres = 0.6)
        #do partial schema mapping
        partial_mapping = SM_test.mapping_naive_tables(query, query_col, self.schema_element_sample_col, self.schema_element_dtype)

        unmatched = {}
        for i in self.schema_linking.keys():
            unmatched[i] = {}
            for j in query.columns.tolist():
                unmatched[i][j] = {}

        meta_mapping, unmatched = SM_test.mapping_naive_tables_join(query, query_col, \
                                                                    self.schema_element_sample_col, \
                                                                    self.schema_element_sample_row, \
                                                                    self.schema_element_dtype, unmatched)
        logging.info("meta mapping finished!")

        rank_candidate = []

        for i in self.real_tables.keys():

            tname = i
            gid = self.table_group[tname[6:]]
            if gid not in meta_mapping:
                continue

            tableS = query
            tableR = self.real_tables[i]

            SM,ms = self.schema_mapping(tableS, tableR, meta_mapping, gid)

            if len(SM.items()) == 0:
                continue

            key_choice = []
            for kyA in SM.keys():
                key_score = self.approximate_join_key(query, tableR, SM, kyA, thres_key_prune)
                key_choice.append((kyA, key_score))

            if len(key_choice) == 0:
                continue
            else:
                key_choice = sorted(key_choice, key=lambda d: d[1], reverse=True)
                key_factor = key_choice[0][1]

            rank_candidate.append((tname, key_factor))

        rank_candidate = sorted(rank_candidate, key=lambda d: d[1], reverse=True)

        if len(rank_candidate) == 0:
            return []

        if len(rank_candidate) > k:
            ks = k
        else:
            ks = len(rank_candidate)


        rtables_names = self.__remove_dup2(rank_candidate, k)

        #logging.info('Schema Mapping Costs: %s Seconds'%time1)
        #logging.info('Full Search Costs:%s Seconds'%time3)

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

        rtables_names = self.remove_dup(top_tables, ks)

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

        rtables_names = self.remove_dup(top_tables, k)

        logging.info('Schema Mapping Costs: %s Seconds'%time1)
        logging.info('Full Search Costs:%s Seconds'%time3)

        rtables = []
        for i,j in rtables_names:
            #print(i,j)
            rtables.append((i, self.real_tables[i]))

        return rtables

