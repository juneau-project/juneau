import numpy as np
import pandas as pd
from data_extension.util import jaccard_similarity

class Random_State:

    def __init__(self, query, tables):
        self.name = query.name # the query name
        self.tables = tables # a list of table names
    def save_a_state(self, state):
        self.state = state # a dictionary of feature:dataframe



class Random_Components:
    def __init__(self, tables, schema_mapping_class, previous_state = None):
        self.tables = tables
        self.sm_class = schema_mapping_class
        self.pre_state = previous_state

    def full_schema_mapping(self, tableA, tableB, initial_sm):
        return self.sm_class.continue_full_mapping(tableA, tableB, initial_sm)

    def common_key_detection(self, query, target_name, mapping, candidate_keys, partial_mapping_cached, key_index_flag = False):
        if len(candidate_keys) == 0:
            key_return, _, key_indexA, key_indexB = self.sm_class.detect_key_constraints(query.name, target_name, mapping,
                                                                      partial_mapping_cached, list(mapping.keys()),
                                                                      query.value, self.tables[target_name])
        else:
            key_return  = candidate_keys

        if key_index_flag:
            return key_return, key_indexA, key_indexB
        else:
            return key_return

    def col_similarity(self, tableA, tableB, SM, key_factor):

        col_sim_upper = 1 + float(len(SM.keys()) - 1) * float(key_factor)
        tableA_not_in_tableB = []
        for kyA in tableA.columns.tolist():
            if kyA not in SM:
                tableA_not_in_tableB.append(kyA)
        col_sim_lower = len(tableB.columns.values) + len(tableA_not_in_tableB)
        col_sim = float(col_sim_upper) / float(col_sim_lower)
        return col_sim

    def new_col_rate(self, tableA, tableB, SM):
        tableA_not_in_tableB = []
        for kyA in tableA.columns.tolist():
            if kyA not in SM:
                tableA_not_in_tableB.append(kyA)
        col_rate = float(len(tableA_not_in_tableB))/float(tableA.shape[1])
        return col_rate

    def row_similarity(self, detected_key, SM, tableA, tableB):
        if len(detected_key) != 0:
            max_row_sim = 0
            for key in detected_key:
                if key in SM:
                    Avalue = tableA[key].values
                    Bvalue = tableB[SM[key]].values
                    try:
                        row_sim = jaccard_similarity(Avalue, Bvalue)
                    except:
                        row_sim = 0
                if row_sim > max_row_sim:
                    max_row_sim = row_sim

        else:
            max_row_sim = 0

        return max_row_sim

    def nan_delta(self, mapping, tableA, tableB, key_indexA, key_indexB):

        if len(mapping.keys()) == 0:
            return 0

        tableA_NAN = tableA.loc[key_indexA][list(mapping.keys())].isnull()
        tableB_NAN = tableB.loc[key_indexB][list(mapping.values())].isnull()

        df_bool = (~tableB_NAN.isnull()) & (tableA_NAN.isnull())
        df_bool_score = float((df_bool == True).sum().sum())

        return df_bool_score

    def new_row_rate(self, detected_key, mapping, tableA, tableB):

        if len(detected_key) != 0:

            new_data_rate = 0
            for key in detected_key:
                if key in mapping:
                    Alen = tableA[key].values
                    Blen = tableB[mapping[key]].values
                    try:
                        new_data_rate_temp = float(1) - float(len(np.intersect1d(Alen, Blen))) / float(len(Alen))
                    except:
                        new_data_rate_temp = 0
                else:
                    new_data_rate_temp = 0

                if new_data_rate_temp > new_data_rate:
                    new_data_rate = new_data_rate_temp
        else:
            new_data_rate = 0

        return new_data_rate

    def merge_additional_training(self, sorted_score, query, tableB_name, initial_mapping, candidate_keys, partial_mapping_cached, alpha, beta):
        if self.pre_state != None:
            if tableB_name in self.pre_state.tables:
                score = alpha * self.pre_state.state["sorted_score"][tableB_name] + beta * self.pre_state.state["col_sim"][tableB_name] + \
                        float(1 - beta - alpha) * self.pre_state.state["new_row_rate"][tableB_name]
                scores = [self.pre_state.state["sorted_score"][tableB_name], self.pre_state.state["col_sim"][tableB_name], \
                          self.pre_state.state["new_row_rate"][tableB_name]]
                return score, scores


        tableB = self.tables[tableB_name]

        sm_full = self.full_schema_mapping(query.value, self.tables[tableB_name], initial_mapping)

        common_key = self.common_key_detection(query, tableB_name, sm_full, candidate_keys, partial_mapping_cached)

        col_sim = self.col_similarity(query.value, tableB, sm_full, 1)
        new_row_rate = self.new_row_rate(common_key, sm_full, query.value, tableB)

        score = alpha * sorted_score + float(beta) * col_sim + \
                float(1 - beta - alpha) * new_row_rate

        scores = [sorted_score, col_sim, new_row_rate]

        return score, scores

    def merge_additional_feature(self, sorted_score, query, tableB_name, initial_mapping, candidate_keys, partial_mapping_cached, alpha, beta):
        if self.pre_state != None:
            if tableB_name in self.pre_state.tables:
                score = alpha * self.pre_state.state["sorted_score"][tableB_name] + beta * self.pre_state.state["row_sim"][tableB_name] + \
                    float(1 - beta - alpha) * self.pre_state.state["new_col_rate"][tableB_name]
                scores = [self.pre_state.state["sorted_score"][tableB_name], self.pre_state.state["row_sim"][tableB_name], \
                          self.pre_state.state["new_col_rate"][tableB_name]]
                return score, scores

        tableB = self.tables[tableB_name]

        sm_full = self.full_schema_mapping(query.value, self.tables[tableB_name], initial_mapping)

        common_key = self.common_key_detection(query, tableB_name, sm_full, candidate_keys, partial_mapping_cached)

        row_sim = self.row_similarity(common_key, sm_full, query.value, tableB)
        new_col_rate = self.new_row_rate(common_key, sm_full, query.value, tableB)

        score = alpha * sorted_score + float(beta) * row_sim + float( 1 - alpha - beta) * new_col_rate

        scores = [sorted_score, row_sim, new_col_rate]
        return score, scores

    def merge_data_cleaning(self, sorted_score, query, tableB_name, initial_mapping, candidate_keys, partial_mapping_cached, alpha, beta, gamma):
        if self.pre_state != None:
            if tableB_name in self.pre_state.tables:
                score = alpha * self.pre_state.state["sorted_score"][tableB_name] + beta * self.pre_state.state["col_sim"][tableB_name] + \
                        gamma * self.pre_state.state["row_sim"][tableB_name] + float( 1 - alpha - beta - gamma) * self.pre_state.state["nan_delta"][tableB_name]
                scores = [self.pre_state.state["sorted_score"][tableB_name], self.pre_state.state["col_sim"][tableB_name], \
                          self.pre_state.state["row_sim"][tableB_name], self.pre_state.state["nan_delta"][tableB_name]]
                return score, scores

        tableB = self.tables[tableB_name]

        sm_full = self.full_schema_mapping(query.value, self.tables[tableB_name], initial_mapping)

        common_key, key_indexA, key_indexB = self.common_key_detection(query, tableB_name, sm_full, candidate_keys, partial_mapping_cached, True)

        row_sim = self.row_similarity(common_key, sm_full, query.value, tableB)
        col_sim = self.col_similarity(query.value, tableB, sm_full, 1)
        nan_delta = self.nan_delta(sm_full, query.value, tableB, key_indexA, key_indexB)

        score = alpha * sorted_score + float(beta) * col_sim + float(gamma) * row_sim + float(1 - alpha - beta - gamma) * nan_delta
        scores = [sorted_score, col_sim, row_sim, nan_delta]

        return score, scores

    def save_state(self, query, random_scores, case_id, engine, cursor):
        table_index = list(random_scores.keys())
        df_value = list(random_scores.values())
        all_cols = ["sorted_score", "col_sim", "row_sim", "new_row_rate", "new_col_rate"]
        if case_id == 0:
            cols = ["sorted_score", "col_sim", "new_row_rate"]
            new_state_df = pd.DataFrame(df_value, index = table_index, columns = cols )
        elif case_id == 1:
            cols = ["sorted_score", "row_sim", "new_col_rate"]
            new_state_df = pd.DataFrame(df_value, index = table_index, columns = cols )
        elif case_id == 2:
            cols = ["sorted_score", "col_sim", "row_sim", "nan_delta"]
            new_state_df = pd.DataFrame(df_value, index = table_index, columns = cols)

        if self.pre_state != None:
            state_df = self.pre_state.append(new_state_df)
        else:
            state_df = new_state_df

        select_schema_string = "select table_name from information_schema.tables " \
                               "where table_schema = \'topk_ra_states\' and table_name = \'" + query.name + "\'"
        cursor.execute(select_schema_string)
        existing_tables = cursor.fetchall()
#        print(existing_tables)
        if len(existing_tables) > 0:
            #pd.set_option('display.max_colwidth', -1)
            existing_state_df = pd.read_sql_table(table_name=query.name, index_col='table_name', con = engine, schema="topk_ra_states")
            e_cols = existing_state_df.columns
            n_cols = new_state_df.columns
            k_cols = np.intersect1d(e_cols, n_cols)
            new_state_df = pd.merge(existing_state_df, new_state_df, how = "outer", on = k_cols.tolist(), left_index=True, right_index=True)

        new_state_df.to_sql(query.name, con=engine, schema="topk_ra_states", if_exists='replace', index_label='table_name')
            # e_cols = existing_state_df.columns
            #
            # new_state_df.to_sql(query.name, con = engine, schema= "topk_ra_states_temp", if_exists='replace', index_label='table_name')
            # join_str = "CREATE TABLE topk_ra_states." + query.name + "_temp AS SELECT "
            # for col in e_cols:
            #     join_str = join_str + "topk_ra_states."  + query.name + "." + col + ", "
            #
            # old_cols = [c for c in all_cols if c not in cols]
            # for col in old_cols:
            #     join_str = join_str + "topk_ra_states_temp." + query.name + "." + col + ", "
            #
            # join_str =  join_str[:-2] + " FROM topk_ra_states_temp." + query.name + " FULL OUTER JOIN topk_ra_states." + query.name + \
            #             " ON topk_ra_states." + query.name + ".table_name = topk_ra_states_temp." + query.name + ".table_name;"
            # cursor.execute(join_str)
            #
            # cursor.execute("DROP TABLE topk_ra_states_temp." + query.name + ";")
            # cursor.execute("ALTER TABLE topk_ra_states." + query.name + "_temp RENAME TO topk_ra_states." + query.name + ";")

        random_state = Random_State(query, table_index)
        random_state.save_a_state(state_df)
        return random_state












