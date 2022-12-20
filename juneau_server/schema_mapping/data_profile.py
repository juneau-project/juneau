from data_extension.table_db import connect2db_engine
from data_extension.table_db import fetch_all_table_names
from data_extension.store.store_sketch import StoreSketch
from data_extension.util import merge_add_dictionary, merge_two_lists
import pandas as pd
import pickle
from data_extension.util import jaccard_similarity
from datasketch import MinHash
import numpy as np
import copy
import json
from data_extension.config import sql_dbs

import logging
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
import sys

def convert(o):
    if isinstance(o, np.int64): return int(o)
    raise TypeError


max_reg_cols = 1000
PATTERN_PASS = 20
col_matching_threshold = 0.1
high_col_matching_threshold = 0.3
max_table_per_check = 100
approx_int_factor = 3
scan_pass = 1000
int_matching_threshold1 = 0.7
int_matching_threshold2 = 0.3

class Data_Profile():

    def __init__(self, psql_eng = None, schema = None, max_pattern_size = 5, min_pattern_support = 10):

        if psql_eng:
            self.psql_eng = psql_eng

        if not self.psql_eng:
            self.psql_eng = connect2db_engine()

        #self.__create_necessary_index_tables(self.psql_eng)

        self.profile_schema = schema

        self.max_registered_cols = max_reg_cols
        self.max_pattern_size = max_pattern_size
        self.pattern_support = min_pattern_support
        self.profile_info = None

        self.profile_info, self.group_info = self.__query_profile_info(self.psql_eng)

        self.new_group_info = {}
        self.temp_profile_info = {}
        self.temp_new_col_info = {}

        self.sketch_class = StoreSketch(self.psql_eng)
        self.mapped_threshold = high_col_matching_threshold
        self.int_mapped_threshold1 = int_matching_threshold1
        self.int_mapped_threshold2 = int_matching_threshold2
        self.candidate_threshold = col_matching_threshold

        self.real_tables = {}
        self.profile_combination_cnt = {}
        self.temp_new_profile_stored = {}

    def __create_necessary_index_tables(self, conn):

        try:
            conn = conn.connect()
        except:
            conn = conn

        try:
            query0 = "DROP SCHEMA IF EXISTS data_profile CASCADE;"
            query1 = "CREATE SCHEMA IF NOT EXISTS data_profile;"

            try:
                conn.execute(query0)
                conn.execute(query1)
            except:
                logging.error("Loading Data Profile: CREATE SCHEMA data_profile ERROR!")

            query2 = "CREATE TABLE IF NOT EXISTS data_profile.profile (col_id VARCHAR(100), \
                        col_name VARCHAR(100), values VARCHAR(10000000), tables VARCHAR(10000000));"
            query3 = "CREATE TABLE IF NOT EXISTS data_profile.group_profile (group_id VARCHAR(100), \
                        member_ids VARCHAR(10000000));"

            try:
                conn.execute(query2)
                conn.execute(query3)

            except:
                logging.error("Loading Index: CREATE Data Profile Tables FAILED\n")

            return True

        except:
            logging.error("Loading Index: DATABASE CONNECTION ERROR!")
            return False

    def __query_profile_info(self, conn):

        profile_info = {}
        profile_df = pd.read_sql_table('profile', conn.connect(), schema=self.profile_schema)
        for iter, row in profile_df.iterrows():
            pid = row['col_id']
            pnames = row['col_name'].split("#sep#")
            ptables = row['tables'].split("#sep#")
            pvalues = json.loads(row['values'].encode())
            profile_info[pid] = {}
            profile_info[pid]['name'] = pnames
            profile_info[pid]['tables'] = ptables
            profile_info[pid]['type'] = row['type']

            if row['type'] == 'str':
                profile_info[pid]['values'] = pvalues
            else:
                int_p_values = {}
                for pkey in pvalues.keys():
                    try:
                        int_p_values[int(pkey)] = pvalues[pkey]
                    except:
                        continue
                profile_info[pid]['values'] = int_p_values

        group_profile_df = pd.read_sql_table('group_profile', conn.connect(), schema=self.profile_schema)
        group_profile_info = {}
        group_info = []
        for iter, row in group_profile_df.iterrows():
            gid = row['group_id']
            members = row['member_ids'].split("#sep#")
            group_profile_info[gid] = members
            group_info.append('#sep#'.join(sorted(members)))

        return profile_info, group_info

    def schema_mapping(self, table_name, table_df):

        self.real_tables[table_name] = pd.DataFrame(data=None, columns=table_df.columns)

        col2profile, new_profile_candidate_map, unmatched_cols = self.query_data_profile_info_from_postgres(table_df)

        new_profiles_to_register = self.determine_new_data_profile_tobe_registered(table_df, col2profile, new_profile_candidate_map)

        logging.info(new_profiles_to_register)

        self.update_data_profile_info(table_df, table_name, col2profile, new_profiles_to_register, new_profile_candidate_map)
        self.new_data_profile_info(table_df, table_name, new_profiles_to_register, unmatched_cols)

    def query_data_profile_info_from_postgres(self, table_df):

        unmatched_cols = []
        matched_cols = []

        mapped_results = self.sketch_class.query_cols(table_df, self.mapped_threshold, self.int_mapped_threshold1)
        
        logging.info("show lshe mapping results:")
        logging.info(mapped_results)    

        col2profile = {}
        # map to the best one
        for col, matched_ids in mapped_results:
            id_match_scores = []
            for mid in matched_ids:
                jacc_sim = jaccard_similarity(table_df[col].dropna().tolist(), list(self.profile_info[mid]['values'].keys()))
                id_match_scores.append((mid, jacc_sim))
            id_match_scores = sorted(id_match_scores, key=lambda d:d[1], reverse=True)
            col2profile[col] = id_match_scores[0][0]
            matched_cols.append(col)

        candidate_results = self.sketch_class.query_cols(table_df, self.candidate_threshold, self.int_mapped_threshold2)
        new_profile_candidate_map = {}
        # map to the best one
        for col, matched_ids in candidate_results:

            if col in col2profile:
                continue

            id_match_scores = []
            for mid in matched_ids:
                jacc_sim = jaccard_similarity(table_df[col].dropna().tolist(), list(self.profile_info[mid]['values'].keys()))
                id_match_scores.append((mid, jacc_sim))
            id_match_scores = sorted(id_match_scores, key = lambda d:d[1], reverse=True)
            new_profile_candidate_map[col] = id_match_scores[0][0]
            matched_cols.append(col)

        for col in table_df.columns:

            if type(col) != 'str':
                continue


            if col.startswith('#self') or col.startswith('#par'):
                continue


            if col in matched_cols:
                continue

            if table_df[col].dtype != object and table_df[col].dtype != int and table_df[col].dtype != float:
                continue

            unmatched_cols.append(col)
        
        #if len(list(new_profile_candidate_map.keys())) > 0 or len(list(col2profile.keys())) > 0:
        #    self.profile_info, self.group_info = self.__query_profile_info(self.psql_eng)
  
        return col2profile, new_profile_candidate_map, unmatched_cols

    def determine_new_data_profile_tobe_registered(self, table_df, col2profile, new_profile_candidate_map, threshold = min(col_matching_threshold, 0.1)):

        table_df = table_df.dropna()

        new_cols_to_register = []

        for col in new_profile_candidate_map.keys():

            if col == 'comments':
                continue

            if table_df[col].dtype == str or table_df[col].dtype == object:
                profile_id = new_profile_candidate_map[col]
                profile_values = list(self.profile_info[profile_id]['values'].keys())

                other_cols = list(new_profile_candidate_map.keys())
                other_cols.remove(col)
                other_cols = other_cols + list(col2profile.keys())

                other_cols_in_profile = list(new_profile_candidate_map.values())
                other_cols_in_profile.remove(new_profile_candidate_map[col])
                other_cols_in_profile = other_cols_in_profile + list(col2profile.values())

                min_score = 1

                check_index = table_df[col].isin(profile_values)

                for oid, o_col in enumerate(other_cols):

                    o_profile_id = other_cols_in_profile[oid]

                    remove_values = table_df[o_col][check_index].values

                    left_values_from_df = np.setdiff1d(table_df[o_col].values, remove_values)

                    left_values_from_profile = np.setdiff1d(np.array(list(self.profile_info[o_profile_id]['values'].keys())), remove_values)

                    if len(set(left_values_from_profile)) == 0:
                        continue

                    diff = jaccard_similarity(left_values_from_df, left_values_from_profile)

                    if diff < min_score:
                        min_score = diff

                if min_score < threshold:
                    new_cols_to_register.append(col)

            elif table_df[col].dtype == int or table_df[col].dtype == float:

                profile_id = new_profile_candidate_map[col]
                avg_digit_len = np.mean([len(str(int(t))) for t in table_df[col].dropna().values.tolist()])
                avg_digit_profile_len = np.mean([ len(str(t)) for t in list(self.profile_info[profile_id]['values'].keys())])
                if abs(avg_digit_len - avg_digit_profile_len) > 1:
                    new_cols_to_register.append(col)


        return new_cols_to_register

    def new_data_profile_info(self, table_df, table_name, registered_cols, unmatched_cols, m_thres = col_matching_threshold, m_thres2 = int_matching_threshold1):

        registered_cols = list(set(registered_cols + unmatched_cols))

        print('cols to be registered: ', registered_cols)

        for cid, col in enumerate(registered_cols):
            
            if col == 'comments':
                continue

            col_type = table_df[col].dtype

            try:
                col_values, col_value_counts = np.unique(table_df[col].dropna().tolist(), return_counts=True)
                col_value_dict = {col_values[i] : 1 for i in range(len(col_values))}
            except:
                continue

            #col_value_dict = {col_values[i] : 1 for i in range(len(col_values))}

            new_temp_id = str(len(self.temp_new_col_info.keys()) + cid)

            self.temp_new_col_info[new_temp_id] = {}
            self.temp_new_col_info[new_temp_id]['tables'] = table_name
            self.temp_new_col_info[new_temp_id]['temp_profile'] = []

            mflg = False
            for temp_pro in self.temp_profile_info.keys():

                temp_pro_type = self.temp_profile_info[temp_pro]['type']

                if temp_pro_type != col_type:
                    continue

                if temp_pro_type == bool:
                    continue

                if col_type == str or col_type == object:

                    match_score = jaccard_similarity(col_values, list(self.temp_profile_info[temp_pro]['values'].keys()))

                    if match_score > m_thres:
                        mflg = True
                        self.temp_new_col_info[new_temp_id]['temp_profile'].append(temp_pro)
                        self.temp_profile_info[temp_pro]['name'].append(col)

                        self.temp_profile_info[temp_pro]['values'] = \
                            merge_add_dictionary(self.temp_profile_info[temp_pro]['values'], col_value_dict)
                        self.temp_profile_info[temp_pro]['tables'].append(table_name)
                        self.temp_profile_info[temp_pro]['type'] = table_df[col].dtype
                    #continue

                elif col_type == int and len(col_values) < 10:

                    match_score = jaccard_similarity(col_values, list(self.temp_profile_info[temp_pro]['values'].keys()))

                    if match_score > m_thres:
                        mflg = True

                        self.temp_new_col_info[new_temp_id]['temp_profile'].append(temp_pro)

                        self.temp_profile_info[temp_pro]['name'].append(col)

                        self.temp_profile_info[temp_pro]['values'] = \
                            merge_add_dictionary(self.temp_profile_info[temp_pro]['values'], col_value_dict)
                        self.temp_profile_info[temp_pro]['tables'].append(table_name)
                        self.temp_profile_info[temp_pro]['type'] = table_df[col].dtype

                elif col_type == int or col_type == float:

                    if len(col_values) == 0:
                        continue

                    col_values = [int(val) for val in col_values]
                    col_approx_values = []
                    col_approx_dict = {}
                    for val in col_values:
                        if val < np.power(10, approx_int_factor):
                            col_approx_values.append(val)
                            new_val = val
                        else:
                            remove_digit = len(str(val)) - approx_int_factor
                            new_val = int (val / np.power(10, remove_digit)) * np.power(10, remove_digit)

                        col_approx_values.append(new_val)
                        if new_val not in col_approx_dict:
                            col_approx_dict[new_val] = 0
                        col_approx_dict[new_val] += 1

                    match_score = jaccard_similarity(col_approx_values, list(self.temp_profile_info[temp_pro]['values'].keys()))
                    if match_score > m_thres2:
                        mflg = True

                    # min_a = min(col_values)
                    # max_a = max(col_values)
                    # if len(list(self.temp_profile_info[temp_pro]['values'].keys())) == 0:
                    #     continue
                    # min_b = min(list(self.temp_profile_info[temp_pro]['values'].keys()))
                    # max_b = max(list(self.temp_profile_info[temp_pro]['values'].keys()))
                    #
                    # if min_b > max_a or min_a > max_b:
                    #     continue
                    # else:
                    #     interval = (min(max_a, max_b) - max(min_a, min_b))/(max(max_a, max_b) - min(min_a,min_b))
                    #     if interval > m_thres:
                    #         mflg = True
                        self.temp_new_col_info[new_temp_id]['temp_profile'].append(temp_pro)
                        self.temp_profile_info[temp_pro]['name'].append(col)
                        self.temp_profile_info[temp_pro]['values'] = \
                            merge_add_dictionary(self.temp_profile_info[temp_pro]['values'], col_approx_dict)
                        self.temp_profile_info[temp_pro]['tables'].append(table_name)
                        self.temp_profile_info[temp_pro]['type'] = table_df[col].dtype

            if not mflg:
                new_temp_profile_id = str(len(self.temp_profile_info.keys()) + 1)
                self.temp_profile_info[new_temp_profile_id] = {}
                self.temp_profile_info[new_temp_profile_id]['name'] = [col]
                self.temp_profile_info[new_temp_profile_id]['tables'] = [table_name]
                self.temp_profile_info[new_temp_profile_id]['type'] = table_df[col].dtype

                if col_type == str or col_type == object:
                    self.temp_profile_info[new_temp_profile_id]['values'] = col_value_dict
                elif col_type == int and len(col_values) < 10:
                    self.temp_profile_info[new_temp_profile_id]['values'] = col_value_dict
                elif col_type == int or col_type == float:
                    col_values = [int(val) for val in col_values]
                    col_approx_values = []
                    col_approx_dict = {}
                    for val in col_values:
                        if val < np.power(10, approx_int_factor):
                            col_approx_values.append(val)
                            new_val = val
                        else:
                            remove_digit = len(str(val)) - approx_int_factor
                            new_val = int (val / np.power(10, remove_digit)) * np.power(10, remove_digit)

                        col_approx_values.append(new_val)
                        if new_val not in col_approx_dict:
                            col_approx_dict[new_val] = 0
                        col_approx_dict[new_val] += 1

                    self.temp_profile_info[new_temp_profile_id]['values'] = col_approx_dict

    def update_data_profile_info(self, table_df, table_name, col2profile, new_profiles_to_register, new_profile_candidate_map):

        profile_to_update = list(col2profile.keys())

        for col in profile_to_update:

            if col in col2profile:
                old_info = self.profile_info[col2profile[col]]
                pid = col2profile[col]
            else:
                old_info = self.profile_info[new_profile_candidate_map[col]]
                pid = new_profile_candidate_map[col]

            old_values = old_info['values']

            col_values = list(set(table_df[col].dropna().tolist()))
            if table_df[col].dtype == str or table_df[col].dtype == object:
                new_values = col_values
            elif table_df[col].dtype == int and len(col_values) < 10:
                new_values = col_values
            else:
                col_values = [int(val) for val in col_values]
                col_approx_values = []
                for val in col_values:
                    if val < np.power(10, approx_int_factor):
                        col_approx_values.append(val)
                        new_val = val
                    else:
                        remove_digit = len(str(val)) - approx_int_factor
                        new_val = int(val / np.power(10, remove_digit)) * np.power(10, remove_digit)

                    col_approx_values.append(new_val)
                new_values = list(set(col_approx_values))

            for val in new_values:
                if val not in old_values:
                    old_values[val] = 0
                old_values[val] += 1

            self.profile_info[pid]['values'] = old_values
            self.profile_info[pid]['tables'].append(table_name)
            self.profile_info[pid]['name'].append(col)

    # these functions will be run periodically
    def __group_profiles(self):

        patterns = []

        temp_patterns = []
        frequent_combination = {}

        for i in range(self.max_pattern_size):

            if i == 0:
                for col in self.temp_profile_info.keys():

                    #if self.temp_profile_info[col]['type'] != object:
                    #    continue

                    col_value_dict = self.temp_profile_info[col]['values']
                    col_value_dict = sorted(col_value_dict.items(), key=lambda d:d[1], reverse=True)
                    for col_value, col_value_cnt in col_value_dict[:PATTERN_PASS]:

                        if col_value_cnt > self.pattern_support:
                            
                            if len(str(col_value)) > 100:
                                continue
                            
                            temp_patterns.append([[col_value], [col]])

                patterns += temp_patterns
            else:
                temp_temp_patterns = copy.deepcopy(temp_patterns)
                temp_patterns = []
                checked_patterns = {}
                checked_patterns_times = {}
                for pid1, pat1 in enumerate(temp_temp_patterns):
                    for pid2, pat2 in enumerate(temp_temp_patterns):

                        if pid2 <= pid1:
                            continue

                        if pat1[1] == pat2[1]:
                            continue

                        if len(list(set(pat1[1] + pat2[1]))) != i + 1:
                            continue

                        all_items, all_cols = merge_two_lists(pat1, pat2)

                        if all_cols is None:
                            continue

                        combination = '#sep#'.join(sorted(all_cols))

                        if combination in frequent_combination:
                            if frequent_combination[combination] >= PATTERN_PASS:
                                continue
                        if combination in checked_patterns_times:
                            if checked_patterns_times[combination] >= scan_pass:
                                continue
                            checked_patterns_times[combination] += 1
                        else:
                            checked_patterns_times[combination] = 1


                        pattern_value_to_check = '#sep#'.join(map(str, all_items))

                        if pattern_value_to_check in checked_patterns:
                            continue

                        if pattern_value_to_check in self.profile_combination_cnt:
                            temp_patterns.append([all_items, all_cols])
                            if '#sep#'.join(sorted(all_cols)) not in frequent_combination:
                                frequent_combination['#sep#'.join(sorted(all_cols))] = 0
                            frequent_combination['#sep#'.join(sorted(all_cols))] += 1
                            continue

                        checked_patterns[pattern_value_to_check] = 0

                        all_tables_to_check = {}
                        for col in all_cols:
                            for tid, temp_table in enumerate(self.temp_profile_info[col]['tables']):
                                if temp_table not in all_tables_to_check:
                                    all_tables_to_check[temp_table] = {}
                                all_tables_to_check[temp_table][col] = self.temp_profile_info[col]['name'][tid]

                        with self.psql_eng.connect() as con:
                            checked_table_num = 0
                            for table_name in all_tables_to_check.keys():
                                
                                if checked_table_num >= max_table_per_check:
                                    break
                                
                                if len(all_tables_to_check[table_name].keys()) != len(all_cols):
                                    continue

                                pflg = False
                                for col in all_tables_to_check[table_name].keys():
                                    if all_tables_to_check[table_name][col] not in self.real_tables[table_name].columns:
                                        pflg = True
                                        break
                                if pflg:
                                    continue
                                
                                query_statement = "select count(*) from " + sql_dbs + ".\"rtable" + table_name + "\" where "
                                for cid, col in enumerate(all_tables_to_check[table_name].keys()):
                                    if self.real_tables[table_name][all_tables_to_check[table_name][col]].dtype == object:
                                        try:
                                            query_statement += '\"rtable' + table_name + "\".\"" + all_tables_to_check[table_name][col] + "\"= E'" + str(all_items[cid]).replace("\'", "\\'") + "' and "
                                        except:
                                            print(self.real_tables[table_name][all_tables_to_check[table_name][col]].dtype)
                                            print(all_items[cid])
                                            print(sys.exc_info())
                                    else:
                                        query_statement += '\"rtable' + table_name + "\".\"" + all_tables_to_check[table_name][
                                            col] + "\"=" + str(all_items[cid]) + " and "

                                query_statement = query_statement.strip(" and ") + ";"
                                query_statement = query_statement.replace('%','%%')

                                #logging.info(query_statement)
                                try:
                                    result = con.execute(query_statement)
                                except:
                                    logging.info("************Error*************")
                                    logging.info(sys.exc_info())
                                    result = []

                                for res in result:
                                    if res[0] > 0:
                                        checked_patterns[pattern_value_to_check] += 1 #pattern_value_count[pattern_value_to_check]
                                
                                checked_table_num += 1
                        con.close()


                        if checked_patterns[pattern_value_to_check] < self.pattern_support:
                            continue

                        if pattern_value_to_check not in self.profile_combination_cnt:
                            self.profile_combination_cnt[pattern_value_to_check] = checked_patterns[pattern_value_to_check]

                        temp_patterns.append([all_items, all_cols])
                        if '#sep#'.join(sorted(all_cols)) not in frequent_combination:
                            frequent_combination['#sep#'.join(sorted(all_cols))] = 0
                        frequent_combination['#sep#'.join(sorted(all_cols))] += 1 #checked_patterns[pattern_value_to_check]

                        print(all_items, all_cols)

                patterns += temp_patterns


        profile_patterns = {}
        for pattern in patterns:
            items = pattern[0]
            cols = pattern[1]

            cols_str = '#sep#'.join(cols)

            #if cols_str not in self.group_info:
            #    self.group_info.append(cols_str)

            if cols_str not in profile_patterns:
                profile_patterns[cols_str] = []

            profile_patterns[cols_str].append('#sep#'.join(map(str, items)))

        #print(profile_patterns)
        return list(profile_patterns.keys())

    def store_new_profiles(self):
        
        if not self.profile_info:   
            self.profile_info, self.group_info = self.__query_profile_info(self.psql_eng)
        
        new_profile_temp_ids = self.__group_profiles()

        new_profile_ids_to_store = []
        new_profile_temp_ids = [t.split("#sep#") for t in new_profile_temp_ids]
        for cols in new_profile_temp_ids:
            for col in cols:
                if col not in self.temp_new_profile_stored:
                    new_profile_ids_to_store.append(col)

        new_profile_ids_to_store = list(set(new_profile_ids_to_store))
        logging.info('new profiles to store: ')
        logging.info(self.temp_profile_info)
        logging.info(new_profile_ids_to_store)

        sketch_to_store = []
        for id, pid in enumerate(new_profile_ids_to_store):

            store_type = ""
            if self.temp_profile_info[pid]['type'] == str or self.temp_profile_info[pid]['type'] == object:
                store_type = 'str'
            elif self.temp_profile_info[pid]['type'] == int:
                store_type = 'int'
            elif self.temp_profile_info[pid]['type'] == float:
                store_type = 'float'
            else:
                continue

            new_pid = str(len(self.profile_info.keys()) + id)

            self.temp_new_profile_stored[pid] = new_pid

            self.profile_info[new_pid] = {}
            self.profile_info[new_pid]['name'] = self.temp_profile_info[pid]['name']
            self.profile_info[new_pid]['tables'] = self.temp_profile_info[pid]['tables']
            self.profile_info[new_pid]['values'] = self.temp_profile_info[pid]['values']
            self.profile_info[new_pid]['type'] = store_type
            if store_type == 'str':
                sketch_to_store.append([new_pid, '#sep#'.join(list(self.temp_profile_info[pid]['values'].keys())), \
                                    len(self.temp_profile_info[pid]['values'].keys()), store_type])
            else:
                logging.info(self.temp_profile_info[pid]['values'].keys())
                sketch_to_store.append([new_pid, ','.join(map(str, list(self.temp_profile_info[pid]['values'].keys()))), \
                                    len(self.temp_profile_info[pid]['values'].keys()), store_type])
        profile_array = []
        for pid in self.profile_info.keys():

            int_dict = {}
            for ikey in self.profile_info[pid]['values'].keys():
                int_dict[str(ikey)] = self.profile_info[pid]['values'][ikey]

            profile_array.append([pid, '#sep#'.join(self.profile_info[pid]['name']), \
                                                    '#sep#'.join(self.profile_info[pid]['tables']), \
                                                    json.dumps(int_dict, default=convert), self.profile_info[pid]['type']])
    
        profile_df = pd.DataFrame(profile_array, columns=['col_id', 'col_name', 'tables', 'values', 'type'])
        profile_df.to_sql('profile', con=self.psql_eng, schema=self.profile_schema, if_exists='replace', index = False)

        group_array = []
        for cols in new_profile_temp_ids:
            new_cols = []
            for col in cols:
                new_cols.append(self.temp_new_profile_stored[col])

            self.group_info.append('#sep#'.join(new_cols))
        self.group_info = list(set(self.group_info))
        for gid, group in enumerate(self.group_info):
            group_array.append([gid, group])
        group_df = pd.DataFrame(group_array, columns= ['group_id','member_ids'])
        group_df.to_sql('group_profile', con = self.psql_eng, schema=self.profile_schema, if_exists='replace', index=False)
        
        if len(sketch_to_store) > 0:    
            self.sketch_class.store_profile_sketches(sketch_to_store)
        
        #self.temp_profile_info = {}
        #self.temp_new_col_info = {}
        #self.real_tables = {}



















