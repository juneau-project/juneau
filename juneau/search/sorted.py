import logging
import numpy as np
import copy
import pandas as pd
from juneau.utils.utils import sigmoid, jaccard_similarity
from juneau.search.search_prov_code import ProvenanceSearch

class Sorted_State:

    def __init__(self, query, tables):
        self.name = query.name # the query name
        self.tables = tables # a list of table names

    def save_a_state(self, state, previous_state, case_id):

        if previous_state == None:
            self.state = state
            return

        if case_id == 0:
            domains = ["col_sim_ub", "new_row_ub", "prov_sim"]
        elif case_id == 1:
            domains = ["row_sim_ub", "new_col_ub", "prov_sim"]
        elif case_id == 2:
            domains = ["col_sim_ub", "row_sim_ub", "nan_diff_ub", "prov_sim"]

        for domain in domains:
            state[domain] = previous_state[domain].append(state[domain])

        self.state = state # a dictionary of feature:dataframe


class Sorted_Components:

    def __init__(self, mappings, all_tables, all_graphs, previous_state = None):
        self.tables = all_tables
        self.comp_tables = []
        self.cache_tables = []

        self.Graphs = all_graphs
        self.mappings = mappings

        self.pre_state = previous_state

        if self.pre_state != None:
            for tn in self.tables.keys():
                if tn in self.pre_state.tables:
                    self.cache_tables.append(tn)
                else:
                    self.comp_tables.append(tn)
        else:
            self.comp_tables = list(self.tables.keys())

    def provenance_score(self, query, alpha):

        prov_class = ProvenanceSearch(self.Graphs)

        # Compute Provenance Similarity
        logging.info("Compute Provenance Similarity!")
        table_prov_rank = prov_class.search_score_rank(query.node, self.comp_tables)
        table_prov_score = {}

        for i, j in table_prov_rank:
            table_prov_score["rtable" + i] = j
        for i in self.cache_tables:
            table_prov_score[i] = self.pre_state.state["prov_sim"]["score"][i]

        rank_candidate = []
        for i in self.tables.keys():

            if i == query.name:
                continue

            if i in self.cache_tables:
                continue

            if i not in table_prov_score:
                prov_score = 0
            else:
                prov_score = 1 - sigmoid(table_prov_score[i])

            tname = i[6:]

            if tname not in self.mappings:
                inital_mapping = {}
            else:
                inital_mapping = self.mappings[tname]

            prov_score = alpha * prov_score
            rank_candidate.append((i, prov_score, inital_mapping))

        return rank_candidate

    def col_similarity_ub(self, query, beta):

        rank_candiate = []
        for i in self.tables.keys():

            if i == query.name:
                continue

            if i in self.cache_tables:
                rank_candiate.append((i, self.pre_state.state["col_sim_ub"]["score"][i]))
                continue


            tname = i[6:]

            tableA = query.value
            tableB = self.tables[i]

            if tname not in self.mappings:
                col_sim_ub = 0
            else:
                col_sim_ub = float(beta) * \
                         float(min(tableA.shape[1], tableB.shape[1]))\
                         /float(tableA.shape[1] + tableB.shape[1] - len(self.mappings[tname]))
            rank_candiate.append((i, col_sim_ub))
        return rank_candiate

    def row_similarity_ub(self, query, beta):

        rank_candidate = []
        for i in self.tables.keys():

            if i == query.name:
                continue

            if i in self.cache_tables:
                rank_candidate.append((i, self.pre_state.state["row_sim_ub"]["score"][i]))
                continue

            tname = i[6:]

            tableA = query.value
            tableB = self.tables[i]

            if tname not in self.mappings:
                row_sim_ub = 0
            else:
                row_sim_ub = 0
                initial_mapping = self.mappings[tname]
                for key in initial_mapping.keys():
                    Avalue = tableA[key].dropna().keys()
                    Bvalue = tableB[initial_mapping[key]].dropna().values
                    try:
                        row_sim = jaccard_similarity(Avalue, Bvalue)
                    except:
                        row_sim = 0
                    if row_sim > row_sim_ub:
                        row_sim_ub = row_sim
            rank_candidate.append((i, beta * row_sim_ub))
        return rank_candidate

    def new_col_rate_ub(self, query, beta):

        rank_candidate = []
        for i in self.tables.keys():

            if i == query.name:
                continue

            tname = i[6:]

            tableA = query.value

            if tname not in self.mappings:
                inital_mapping = {}
                new_data_rate = 1
            else:
                inital_mapping = self.mappings[tname]
                new_data_rate = float(tableA.shape[1] - len(inital_mapping))/float(tableA.shape[1])

            new_data_rate_ub = float(beta) * new_data_rate
            rank_candidate.append((i, new_data_rate_ub))
        return rank_candidate

    def new_row_rate_ub(self, query, beta):

        rank_candidate = []
        for i in self.tables.keys():

            if i == query.name:
                continue

            if i in self.cache_tables:
                rank_candidate.append((i, self.pre_state.state["new_row_ub"]["score"][i]))
                continue

            tname = i[6:]

            tableA = query.value
            tableB = self.tables[i]

            if tname not in self.mappings:
                new_data_rate = 0
            else:
                new_data_rate = 0
                inital_mapping = self.mappings[tname]
                for key in inital_mapping.keys():
                    Alen = tableA[key].dropna().values
                    Blen = tableB[inital_mapping[key]].dropna().values
                    try:
                        new_data_rate_temp = float(1) - float(len(np.intersect1d(Alen, Blen))) / float(len(Alen))
                    except:
                        new_data_rate_temp = 0

                    if new_data_rate_temp > new_data_rate:
                        new_data_rate = new_data_rate_temp

            rank_candidate.append((i, beta * new_data_rate))

        return rank_candidate

    def nan_delta_ub(self, query, beta):

        rank_candidate = []

        for i in self.tables.keys():

            if i == query.name:
                continue

            if i in self.cache_tables:
                rank_candidate.append((i, self.pre_state.state["nan_diff_ub"]["score"][i]))
                continue

            tname = i[6:]

            tableA = query.value

            if tname not in self.mappings:
                nan_ub = 0
            else:

                key_indexA = list(self.mappings[tname].keys())
                ub_zero_diff = tableA[key_indexA].isnull().sum().sum()
                value_num = tableA.shape[0] * tableA.shape[1]
                nan_ub = float(ub_zero_diff)/float(value_num)
            rank_candidate.append((i, beta * nan_ub))
        return rank_candidate

    def merge_additional_training(self, query, alpha, beta):

        ub1 = sorted(self.col_similarity_ub(query, beta), key = lambda d:d[1], reverse=True)
        ub2 = sorted(self.new_row_rate_ub(query, 1 - alpha - beta), key = lambda d:d[1], reverse=True)
        #print(ub1[:5])
        #print(ub2[:5])
        ub = ub1[0][1] + ub2[0][1]

        rank_candidate = self.provenance_score(query, alpha)
        old_rank_candidate = copy.deepcopy(rank_candidate)
        rank_candidate = []
        for i in range(len(old_rank_candidate)):
            rank_candidate.append((old_rank_candidate[i][0], old_rank_candidate[i][1] + ub, old_rank_candidate[i][2]))

        u1_df = pd.DataFrame([pair[1] for pair in ub1], index = [pair[0] for pair in ub1], columns = ["score"])
        u2_df = pd.DataFrame([pair[1] for pair in ub2], index = [pair[0] for pair in ub2], columns = ["score"])
        u3_df = pd.DataFrame([pair[1] for pair in old_rank_candidate], index = [pair[0] for pair in old_rank_candidate], columns = ["score"])

        sa_state = Sorted_State(query, list(self.tables.keys()))
        sa_state_value = {"col_sim_ub":u1_df, "new_row_ub": u2_df, "prov_sim": u3_df}
        sa_state.save_a_state(sa_state_value, self.pre_state, 0)

        return rank_candidate, old_rank_candidate, sa_state

    def merge_feature_engineering(self, query, alpha, beta):

        ub1 = sorted(self.row_similarity_ub(query, beta), key = lambda d:d[1], reverse=True)
        ub2 = sorted(self.new_col_rate_ub(query, 1 - alpha - beta), key = lambda d:d[1], reverse=True)
        print(ub1[:5])
        print(ub2[:5])
        ub = ub1[0][1] + ub2[0][1]

        rank_candidate = self.provenance_score(query, alpha)
        old_rank_candidate = copy.deepcopy(rank_candidate)
        rank_candidate = []
        for i in range(len(old_rank_candidate)):
            rank_candidate.append((old_rank_candidate[i][0], old_rank_candidate[i][1] + ub, old_rank_candidate[i][2]))

        uf1_df = pd.DataFrame([pair[1] for pair in ub1], index = [pair[0] for pair in ub1], columns = ["score"])
        uf2_df = pd.DataFrame([pair[1] for pair in ub2], index = [pair[0] for pair in ub2], columns = ["score"])
        uf3_df = pd.DataFrame([pair[1] for pair in old_rank_candidate], index = [pair[0] for pair in old_rank_candidate], columns = ["score"])

        sa_state = Sorted_State(query, list(self.tables.keys()))
        sa_state_value = {"row_sim_ub":uf1_df, "new_col_ub":uf2_df, "prov_sim":uf3_df}
        sa_state.save_a_state(sa_state_value)

        return rank_candidate, old_rank_candidate, sa_state

    def merge_data_cleaning(self, query, alpha, beta, gamma):

        ub1 = sorted(self.col_similarity_ub(query, beta), key=lambda d:d[1], reverse=True)
        ub2 = sorted(self.row_similarity_ub(query, gamma), key=lambda d:d[1], reverse=True)
        ub3 = sorted(self.nan_delta_ub(query, float(1 - alpha - beta - gamma)), key=lambda d:d[1], reverse=True)

        ub = ub1[0][1] + ub2[0][1] + ub3[0][1]

        rank_candidate = self.provenance_score(query, alpha)
        old_rank_candidate = copy.deepcopy(rank_candidate)
        rank_candidate = []
        for i in range(len(old_rank_candidate)):
            rank_candidate.append((old_rank_candidate[i][0], old_rank_candidate[i][1] + ub, old_rank_candidate[i][2]))

        uf1_df = pd.DataFrame([pair[1] for pair in ub1], index = [pair[0] for pair in ub1], columns = ["score"])
        uf2_df = pd.DataFrame([pair[1] for pair in ub2], index = [pair[0] for pair in ub2], columns = ["score"])
        uf3_df = pd.DataFrame([pair[1] for pair in ub3], index = [pair[0] for pair in ub3], columns = ["score"])
        uf4_df = pd.DataFrame([pair[1] for pair in old_rank_candidate], index = [pair[0] for pair in old_rank_candidate], columns = ["score"])

        sa_state = Sorted_State(query, list(self.tables.keys()))
        sa_state_value = {"col_sim_ub":uf1_df, "row_sim_ub":uf2_df, "nan_diff_ub":uf3_df, "prov_sim":uf4_df}
        sa_state.save_a_state(sa_state_value)

        return rank_candidate, old_rank_candidate, sa_state
