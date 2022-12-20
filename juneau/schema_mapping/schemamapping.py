import json
import logging
import timeit

import numpy as np
import pandas as pd

from data_extension.schema_mapping.mapping import Map
from data_extension.schema_mapping.mapping_index import Map_Profile
from data_extension.util import jaccard_similarity

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)




registered_attribute = ['last_name', 'first_name', 'full_name', 'gender', 'age', 'email', 'ssn', 'address']


class SchemaMapping:

    def __init__(self, dbname, sflg, sim_thres = 0.5):

        self.sim_thres = sim_thres

        self.map_class_naive = Map()

        self.map_class_profile = Map_Profile(dbname)
        if sflg:
            self.map_class_profile.loading_index()


    def load_index(self):
        if self.map_class_profile:
            self.map_class_profile.loading_index()

    def dump_index(self):
        if self.map_class_profile:
            self.map_class_profile.dump_index()

    def mapping_naive(self, tableA, tableB, mapped={}):


        Mpair = mapped
        MpairR = {}
        for i in Mpair.keys():
            MpairR[Mpair[i]] = i

        matching = self.map_class_naive.mapping(tableA, tableB, Mpair, MpairR)

        for i in range(len(matching)):
            if matching[i][2] < self.thres:
                break
            else:
                if matching[i][0] not in Mpair and matching[i][1] not in MpairR:
                    Mpair[matching[i][0]] = matching[i][1]
                    MpairR[matching[i][1]] = matching[i][0]

        if len(matching) == 0:
            rv = 0
        else:
            rv = matching[0][2]


        return Mpair, rv


    # Do full schema mapping
    def mapping_naive_incremental(self, tableA, tableB, gid, meta_mapping, schema_linking, unmatched, mapped = {}):

        start_time = timeit.default_timer()
        time1 = 0

        Mpair = mapped
        MpairR = {}
        for i in Mpair.keys():
            MpairR[Mpair[i]] = i

        matching = []
        t_mapping = {}
        for i in tableA.columns.tolist():
            if i in Mpair:
                continue
            if i not in meta_mapping[gid]:
                continue
            t_mapping[schema_linking[gid][meta_mapping[gid][i]]] = i

        for i in tableB.columns.tolist():
            if i in MpairR:
                continue
            if schema_linking[gid][i] in t_mapping:
                if tableB[i].dtype != tableA[t_mapping[schema_linking[gid][i]]].dtype:
                    continue
                Mpair[t_mapping[schema_linking[gid][i]]] = i
                MpairR[i] = t_mapping[schema_linking[gid][i]]

        scma = tableA.columns.tolist()
        scmb = tableB.columns.tolist()
        shmal = len(scma)
        shmbl = len(scmb)

        acol_set = {}

        for i in range(shmal):

            nameA = scma[i]

            if nameA in Mpair:
                continue

            if nameA == "Unnamed: 0" or "index" in nameA:
                continue

            if nameA not in acol_set:
                colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values
                acol_set[nameA] = list(set(colA))
            else:
                colA = acol_set[nameA]

            for j in range(shmbl):

                nameB = scmb[j]  # .split('_')[0].lower()
                if nameB in MpairR:
                    continue

                if nameB == "Unnamed: 0" or "index" in nameB:
                    continue

                if tableA[nameA].dtype != tableB[nameB].dtype:
                    continue

                if nameB in unmatched[gid][nameA]:
                    continue

                colB = tableB[scmb[j]][~pd.isnull(tableB[scmb[j]])].values

                try:
                    colB = colB[~np.isnan(colB)]
                except:
                    try:
                        colB = colB[colB != np.array(None)]
                    except:
                        colB = colB

                s1 = timeit.default_timer()
                sim_col = jaccard_similarity(colA, colB)
                e1 = timeit.default_timer()
                time1 += e1 - s1

                if sim_col < self.sim_thres:
                    unmatched[gid][nameA][nameB] = ''

                matching.append((nameA, nameB, sim_col))

        matching = sorted(matching, key=lambda d: d[2], reverse=True)

        for i in range(len(matching)):
            if matching[i][2] < self.sim_thres:
                break
            else:
                if matching[i][0] not in Mpair and matching[i][1] not in MpairR:
                    Mpair[matching[i][0]] = matching[i][1]
                    MpairR[matching[i][1]] = matching[i][0]

        for i in tableA.columns.tolist():
            if i in Mpair:
                if i not in meta_mapping[gid]:
                    meta_mapping[gid][i] = Mpair[i]

                for j in tableB.columns.tolist():
                    if j != Mpair[i]:
                        unmatched[gid][i][j] = ''


        end_time = timeit.default_timer()
        time_total = end_time - start_time
        #print('full schema mapping: ', time_total)
        return Mpair, meta_mapping, unmatched, time_total

    # Do schema mapping for tables when looking for similar tables
    def mapping_naive_tables(self, tableA, valid_keys, schema_element, schema_dtype, tflag = False):

        start_time = timeit.default_timer()
        time1 = 0

        Mpair = {}
        MpairR = {}

        scma = tableA.columns.values
        shmal = len(scma)
        acol_set = {}

        for group in schema_element.keys():

            Mpair[group] = {}
            MpairR[group] = {}
            matching = []

            for i in range(shmal):

                nameA = scma[i]
                if nameA == "Unnamed: 0" or "index" in nameA:
                    continue

                if nameA not in valid_keys:
                    continue

                if nameA not in acol_set:
                    colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values
                    acol_set[nameA] = list(set(colA))
                else:
                    colA = acol_set[nameA]

                for j in schema_element[group].keys():

                    nameB = j
                    if nameB == "Unnamed: 0" or "index" in nameB:
                        continue

                    colB = np.array(schema_element[group][nameB])

                    if schema_dtype[group][j] is not tableA[nameA].dtype:
                        continue

                    try:
                        colB = colB[~np.isnan(colB)]
                    except:
                        try:
                            colB = colB[colB != np.array(None)]
                        except:
                            colB = colB

                    s1 = timeit.default_timer()

                    sim_col = jaccard_similarity(colA, colB)

                    e1 = timeit.default_timer()
                    time1 += e1 - s1

                    matching.append((nameA, nameB, sim_col))

            matching = sorted(matching, key=lambda d: d[2], reverse=True)

            for i in range(len(matching)):
                if matching[i][2] < self.sim_thres:
                    break
                else:
                    if matching[i][0] not in Mpair[group] and matching[i][1] not in MpairR[group]:
                        Mpair[group][matching[i][0]] = matching[i][1]
                        MpairR[group][matching[i][1]] = matching[i][0]

        end_time = timeit.default_timer()

        if tflag:
            print('Schema Mapping Before Search: %s Seconds.'%(end_time - start_time))

        return Mpair

    # Do schema mapping for tables when looking for joinable tables
    def mapping_naive_tables_join(self, tableA, valid_keys, schema_element_sample, schema_element, schema_dtype, unmatched, tflag = False):

        start_time = timeit.default_timer()
        time1 = 0

        Mpair = {}
        MpairR = {}

        scma = tableA.columns.values
        shmal = len(scma)
        acol_set = {}

        for group in schema_element.keys():

            Mpair[group] = {}
            MpairR[group] = {}
            matching = []

            for i in range(shmal):

                nameA = scma[i]

                if nameA == "Unnamed: 0" or "index" in nameA:
                    continue
                if nameA not in valid_keys:
                    continue

                if nameA not in acol_set:
                    A_index = ~pd.isnull(tableA[nameA])
                    colA = (tableA[nameA][A_index]).values
                    acol_set[nameA] = list(set(colA))
                else:
                    colA = acol_set[nameA]

                for j in schema_element[group].keys():

                    nameB = j

                    if nameB == "Unnamed: 0" or "index" in nameB:
                        continue

                    if schema_dtype[group][j] is not tableA[nameA].dtype:
                        continue

                    colB = np.array(schema_element[group][nameB])

                    try:
                        colB = colB[~np.isnan(colB)]
                    except:
                        try:
                            colB = colB[colB != np.array(None)]
                        except:
                            colB = colB

                    s1 = timeit.default_timer()

                    try:
                        sim_col = jaccard_similarity(colA, colB)
                    except:
                        print(colA)
                        print(colB)

                    if sim_col < self.sim_thres:
                        unmatched[group][nameA][nameB] = ''

                    e1 = timeit.default_timer()
                    time1 += e1 - s1

                    matching.append((nameA, nameB, sim_col))

            for i in schema_element_sample[group].keys():

                nameB = i

                if nameB == "Unnamed: 0" or "index" in nameB:
                    continue

                colB = np.array(schema_element_sample[group][nameB])

                try:
                    colB = colB[~np.isnan(colB)]
                except:
                    try:
                        colB = colB[colB != np.array(None)]
                    except:
                        colB = colB

                for j in range(shmal):

                    nameA = scma[j]
                    if nameA == "Unnamed: 0" or "index" in nameA:
                        continue

                    if nameB in unmatched[group][nameA]:
                        continue

                    if nameA not in acol_set:
                        colA = tableA[nameA][~pd.isnull(tableA[nameA])].values
                        acol_set[nameA] = list(set(colA))
                    else:
                        colA = acol_set[nameA]

                    if schema_dtype[group][nameB] is not tableA[nameA].dtype:
                        continue

                    s1 = timeit.default_timer()
                    sim_col = jaccard_similarity(colA, colB)
                    e1 = timeit.default_timer()
                    time1 += e1 - s1

                    if sim_col < self.sim_thres:
                        unmatched[group][nameA][nameB] = ''

                    matching.append((nameA, nameB, sim_col))

            matching = sorted(matching, key=lambda d: d[2], reverse=True)

            for i in range(len(matching)):
                if matching[i][2] < self.sim_thres:
                    break
                else:
                    if matching[i][0] not in Mpair[group] and matching[i][1] not in MpairR[group]:
                        Mpair[group][matching[i][0]] = matching[i][1]
                        MpairR[group][matching[i][1]] = matching[i][0]


        end_time = timeit.default_timer()

        if tflag:
            print('raw schema mapping: ', end_time - start_time)
            print('sim schema mapping: ', time1)

        return Mpair, unmatched

    # Do schema mapping on Groups
    def mapping_naive_groups(self, tableA, tableA_valid, schema_element):

        start_time = timeit.default_timer()
        time1 = 0

        Mpair = {}
        MpairR = {}

        scma = tableA.columns.values
        shmal = len(scma)
        acol_set = {}

        group_list = []
        for group in schema_element.keys():
            Mpair[group] = {}
            MpairR[group] = {}
            matching = []

            for i in range(shmal):

                nameA = scma[i]
                if nameA not in tableA_valid:
                    continue

                if nameA == "Unnamed: 0" or "index" in nameA:
                    continue

                colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values
                if nameA not in acol_set:
                    acol_set[nameA] = list(set(colA))

                #try:
                #    colA = colA[~np.isnan(colA)]
                #except:
                #    try:
                #        colA = colA[colA != np.array(None)]
                #    except:
                #        colA = colA

                for j in schema_element[group].keys():

                    nameB = j
                    colB = np.array(schema_element[group][nameB])

                    try:
                        colB = colB[~np.isnan(colB)]
                    except:
                        try:
                            colB = colB[colB != np.array(None)]
                        except:
                            colB = colB

                    s1 = timeit.default_timer()

                    sim_col = jaccard_similarity(acol_set[nameA], colB)
                    e1 = timeit.default_timer()
                    time1 += e1 - s1
                    #c1 += 1
                    matching.append((nameA, nameB, sim_col))

            matching = sorted(matching, key=lambda d: d[2], reverse=True)

            if len(matching) == 0:
                continue

            if matching[0][2] < self.sim_thres:
                continue
            else:
                group_list.append(group)

        end_time = timeit.default_timer()

        return group_list



    def mapping_to_columns_index2(self, tableA, tableAname, dbname, thres):

        tableA = tableA.head(1000)
        logging.info(tableA.head(3))

        mapped_pairs = self.map_class_profile.compute_candidate_pairs_index(tableA, thres)

        logging.info("matching paris: ")
        logging.info(mapped_pairs)

        return_mapp_keys = {}
        rev_return_mapp_keys = {}
        for i, j, k in mapped_pairs:
            if i not in return_mapp_keys and j not in rev_return_mapp_keys:
                return_mapp_keys[i] = j
                rev_return_mapp_keys[j] = i

        for colA in tableA.columns.tolist():
            if colA not in return_mapp_keys:
                if tableA[colA].dtype == object:
                    if len(set(tableA[colA].dropna().values)) > 20:
                        return_mapp_keys[colA] = colA

        #logging.info(self.bigtable.keys())
        self.map_class_profile.update_cached_columns2(return_mapp_keys, tableA, tableAname)
        #logging.info("update cache")
        #logging.info(self.bigtable.keys())
        #logging.info("Update Profile Column Time: " + str(time_updatec_profile))

        #times = timeit.default_timer()
        self.map_class_profile.update_cached_tindex2(return_mapp_keys, tableA, tableAname)
        #logging.info("update dependencies")
        #timee = timeit.default_timer()
        #time_updatei_profile = time_updatei_profile + timee - times
        #logging.info("Update Profile Index Time: " + str(time_updatei_profile))
        #self.psql_eng = None

        #logging.info("Create Profile Time: " + str(time_create_profile))
        #logging.info("Match Profile Time: " + str(time_match_profile))


        self.map_class_profile.table_stored_str[tableA.to_string()] = tableAname

        return mapped_pairs

    def mapping_to_columns_search(self, tableA, thres):

        tableA = tableA.dropna().head(500)

        mapped_pairs = self.map_class_profile.compute_candidate_pairs_index(tableA, thres)

        return_mapp_keys = {}
        rev_return_mapp_keys = {}
        for i, j, k in mapped_pairs:
            if i not in return_mapp_keys and j not in rev_return_mapp_keys:
                return_mapp_keys[i] = j
                rev_return_mapp_keys[j] = i

        return_mapps = {}
        return_mapps_cached = {}

        for mkey in return_mapp_keys.keys():
            return_mapps[mkey] = self.map_class_profile.bigtable_table[return_mapp_keys[mkey]]

        for mkey in return_mapp_keys.keys():
            return_mapps_cached[mkey] = return_mapp_keys[mkey]

        return return_mapps, return_mapps_cached

    def add_mapping_by_workflow(self, query_name, partial_mapping, real_tables, table_group):

        #get groups
        query = real_tables[query_name]

        schema_map_partial = {}
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
        connected_groups.append(table_group[query_name[6:]])

        for ct in connected_tables:
            connected_groups.append(table_group[ct])
        connected_groups = list(set(connected_groups))

        #do search
        for i in real_tables.keys():

            if i == query_name:
                continue

            tname = i[6:]

            tableB = real_tables[i]

            gid = table_group[tname]

            if gid not in connected_groups:
                continue

            if tname not in schema_map_partial:
                better_mapping = self.map_class_profile.schema_mapping_float_prov(query, tableB, {})
            else:
                better_mapping = self.map_class_profile.schema_mapping_float_prov(query, tableB, schema_map_partial[tname])

            schema_map_partial[tname] = better_mapping



        return schema_map_partial


    def mapping_to_columns_keys_search(self, tableA, thres, num = 1):

        mapping, mapping_cached = self.mapping_to_columns_search(tableA, thres)

        mapping_key = {}
        for keyA in mapping.keys():
            mapping_key[keyA] = self.map_class_profile.bigtable_keyscore[mapping_cached[keyA]]

        mapping_key = sorted(mapping_key.items(), key = lambda d:d[1], reverse=True)

        return_key = []
        if len(mapping_key) < num:
            for k, s in mapping_key:
                return_key.append(k)
            return mapping, return_key, mapping_cached
        else:
            for k, s in mapping_key:
                return_key.append(k)
                if len(return_key) == num:
                    break
            return mapping, return_key, mapping_cached

    #def mapping_to_columns_keys_search_incremental(self, tableA, thres, pre_mapping = {}, num = 1):




    def detect_key_constraints(self, tableA_name, tableB_name, mapping, mapping2c, candidate_keys, tableA, tableB,
                               key_thres=0.01, dep_thres=0.01):

        key_rank = []
        key_depen = {}
        key_indexA = {}
        key_indexB = {}

        cached_dep_key = []
        for ck in candidate_keys:

            if ck not in mapping:
                continue

            if ck not in mapping2c:
                continue

            if mapping2c[ck] in self.cached_table_depend:
                cached_dep_key.append(ck)

        if len(cached_dep_key) > 0:
            cached_dep_key_rank = []
            for ck in cached_dep_key:
                return_key = ck
                return_dep = []
                possible_dep = self.cached_table_depend[mapping2c[ck]]

                for ckey in mapping2c.keys():

                    if ckey == ck:
                        continue

                    if ckey not in mapping:
                        continue

                    if mapping2c[ckey] in possible_dep:
                        return_dep.append(ckey)

                cached_dep_key_rank.append((return_key, return_dep, len(return_dep)))

            cached_dep_key_rank = sorted(cached_dep_key_rank, key=lambda d: d[2], reverse=True)
            cdk = mapping2c[cached_dep_key_rank[0][0]]

            if tableA_name in self.cached_group_tables[cdk].columns and tableB_name in self.cached_group_tables[
                cdk].columns:
                join_index = self.cached_group_tables[cdk][[tableA_name, tableB_name]].dropna()
                key_indexC = []
                key_indexD = []
                identical_cnt = 0
                for iter, row in join_index.iterrows():
                    indexA_list = json.loads(row[tableA_name])
                    indexB_list = json.loads(row[tableB_name])
                    if len(indexA_list) != 1 or len(indexB_list) != 1:
                        continue
                    else:
                        key_indexC.append(indexA_list[0])
                        key_indexD.append(indexB_list[0])
                        identical_cnt = identical_cnt + 1

                if float(identical_cnt) / float(join_index.shape[0]) > key_thres:
                    return cached_dep_key_rank[0][0], cached_dep_key_rank[0][1], key_indexC, key_indexD

        for ck in candidate_keys:

            if ck not in mapping:
                continue

            if tableA[ck].dropna().shape[0] == 0:
                continue

            if tableB[mapping[ck]].dropna().shape[0] == 0:
                continue

            pruning_scoreA = float(len(set(tableA[ck].dropna().values))) / float(tableA[ck].dropna().shape[0])
            pruning_scoreB = float(len(set(tableB[mapping[ck]].dropna().values))) / float(
                tableB[mapping[ck]].dropna().shape[0])

            if tableA[ck].dtype == np.float64:
                continue

            if pruning_scoreA < 0.01:
                continue

            if pruning_scoreB < 0.01:
                continue

            # logging.info(pruning_scoreA)
            # logging.info(pruning_scoreB)

            if ck in mapping2c:

                # logging.info("here keys")
                key_depen[ck] = []

                keyA = ck
                keyB = mapping[keyA]
                keyC = mapping2c[keyA]

                key_indexA[keyA] = []
                key_indexB[keyB] = []

                # joinA = None
                # for it in range(self.cached_group_cnt[keyC]):
                #     if tableA_name in self.cached_group_tables[keyC][it].columns:
                #         joinA = self.cached_group_tables[keyC][it][[tableA_name]]
                #         break
                # joinB = None
                # for it in range(self.cached_group_cnt[keyC]):
                #     if tableB_name in self.cached_group_tables[keyC][it].columns:
                #         joinB = self.cached_group_tables[keyC][it][[tableB_name]]
                #         break

                nameC = keyC  # + "_index0"
                tableC = self.cached_group_tables[nameC]
                if tableA_name not in tableC.columns or tableB_name not in tableC.columns:
                    join_index = pd.DataFrame(columns=[tableA_name, tableB_name])
                else:
                    join_index = self.cached_group_tables[nameC][[tableA_name, tableB_name]].dropna()
                # join_index = pd.concat([joinA, joinB], axis=1).dropna()

                index_pair = []
                identical_num = 0
                for iter, row in join_index.iterrows():

                    indexA_list = json.loads(row[tableA_name])
                    indexB_list = json.loads(row[tableB_name])

                    if len(indexA_list) != 1 or len(indexB_list) != 1:
                        continue
                    else:
                        index_pair.append((indexA_list[0], indexB_list[0]))
                        key_indexA[keyA].append(indexA_list[0])
                        key_indexB[keyB].append(indexB_list[0])

                # logging.info("here3")
                # logging.info(float(len(index_pair))/float(join_index.shape[0]))

                if join_index.shape[0] == 0:
                    continue

                if float(len(index_pair)) / float(join_index.shape[0]) < key_thres:
                    continue

                for cck in mapping.keys():

                    if cck == ck:
                        continue

                    cckb = mapping[cck]

                    identical_cnt = 0
                    for a, b in index_pair:
                        if tableA[cck].loc[a] == tableB[cckb].loc[b]:
                            identical_num = identical_num + 1
                            identical_cnt = identical_cnt + 1

                    identical_flt = float(identical_cnt) / float(len(index_pair))
                    logging.info(identical_flt)
                    if identical_flt > dep_thres:
                        key_depen[ck].append(cck)

                key_rank.append((ck, identical_num))

            else:
                key_depen[ck] = []

                keyA = ck
                keyB = mapping[keyA]

                key_indexA[keyA] = []
                key_indexB[keyB] = []

                join_index = {}
                value_all = np.union1d(tableA[keyA].dropna().values, tableB[keyB].dropna().values)

                for v in value_all:
                    join_index[v] = {}
                    join_index[v]['A'] = []
                    join_index[v]['B'] = []

                for iter, row in tableA.iterrows():
                    if row[keyA] in join_index:
                        join_index[row[keyA]]['A'].append(iter)
                for iter, row in tableB.iterrows():
                    if row[keyB] in join_index:
                        join_index[row[keyB]]['B'].append(iter)

                index_pair = []
                identical_num = 0
                for v in join_index.keys():
                    indexA_list = join_index[v]['A']
                    indexB_list = join_index[v]['B']

                    if len(indexA_list) != 1 or len(indexB_list) != 1:
                        continue
                    else:
                        index_pair.append((indexA_list[0], indexB_list[0]))
                        key_indexA[keyA].append(indexA_list[0])
                        key_indexB[keyB].append(indexB_list[0])

                # logging.info("here2")
                # logging.info(float(len(index_pair)) / float(len(join_index.keys())))
                if float(len(index_pair)) / float(len(join_index.keys())) < key_thres:
                    continue

                for cck in mapping.keys():

                    if cck == ck:
                        continue

                    cckb = mapping[cck]

                    identical_cnt = 0
                    for a, b in index_pair:
                        if tableA[cck].loc[a] == tableB[cckb].loc[b]:
                            identical_num = identical_num + 1
                            identical_cnt = identical_cnt + 1

                    identical_flt = float(identical_cnt) / float(len(index_pair))
                    # logging.info(identical_flt)
                    if identical_flt > dep_thres:
                        key_depen[ck].append(cck)

                key_rank.append((ck, identical_num))

        if len(key_rank) == 0:
            return [], {}, [], []

        key_rank = sorted(key_rank, key=lambda d: d[1], reverse=True)
        logging.info(key_rank)

        key_return = key_rank[0][0]
        col_depend = key_depen[key_return]

        if key_return in mapping2c:
            key_return_cached = mapping2c[key_return]
            col_depend_cached = []
            for col in col_depend:
                if col in mapping2c:
                    col_depend_cached.append(mapping2c[col])

            if key_return_cached not in self.cached_table_depend:
                self.cached_table_depend[key_return_cached] = col_depend_cached
            else:
                a_cached = self.cached_table_depend[key_return_cached]
                for col_insert in col_depend_cached:
                    if col_insert not in a_cached:
                        a_cached.append(col_insert)
                self.cached_table_depend[key_return_cached] = a_cached

            store_dep = self.dumps_json(self.cached_table_depend)
            col = []
            col_value = []
            for t_key in store_dep.keys():
                col.append(t_key)
                col_value.append(store_dep[t_key])
            dict_df = {'col': col, 'value': col_value}
            df_store = pd.DataFrame.from_dict(dict_df)
            df_store.to_sql("key_dep", self.psql_eng, schema="table_profile", if_exists='replace', index=False)

        return [key_return], col_depend, key_indexA[key_return], key_indexB[mapping[key_return]]

    def detect_kfjey_constraints(self, tableA_name, tableB_name, mapping, mapping2C, candidate_keys, tableA, tableB,
                                 key_thres=0.1, dep_thres=0.05):

        key_rank = []
        key_depen = {}

        cached_dep_key = []
        for ck in candidate_keys:

            if ck not in mapping:
                continue

            if ck not in mapping2C:
                continue

            if mapping2C[ck] in self.cached_table_fdepen:
                cached_dep_key.append(ck)

        if len(cached_dep_key) > 0:
            cached_dep_key_rank = []
            for ck in cached_dep_key:
                return_key = ck
                return_dep = []
                possible_dep = self.cached_table_fdepen[mapping2C[ck]]
                for ckey in mapping2C.keys():
                    if ckey == ck:
                        continue
                    if mapping2C[ckey] in possible_dep:
                        return_dep.append(ckey)
                cached_dep_key_rank.append((return_key, return_dep, len(return_dep)))

            cached_dep_key_rank = sorted(cached_dep_key_rank, key=lambda d: d[2], reverse=True)
            return cached_dep_key_rank[0][0], cached_dep_key_rank[0][1]

        # logging.info(candidate_keys)
        for ck in candidate_keys:

            if ck not in mapping:
                continue

            if tableA[ck].dtype == np.float64:
                continue

            key_depen[ck] = []
            keyA = ck
            keyB = mapping[keyA]

            try:
                prunningA = float(len(set(tableA[keyA].dropna().values))) / float(len(tableA[keyA].dropna().values))
            except:
                prunningA = 0

            try:
                prunningB = float(len(set(tableB[keyB].dropna().values))) / float(len(tableB[keyB].dropna().values))
            except:
                prunningB = 0

            if prunningA < 0.5 and prunningB < 0.5:
                continue

            if keyA in mapping2C:
                keyC = mapping2C[keyA]
                try:
                    join_index = self.cached_group_tables[keyC][[tableA_name, tableB_name]].dropna()

                    index_pairA = []
                    index_pairB = []

                    for iter, row in join_index.iterrows():
                        indexA_list = json.loads(row[tableA_name])
                        indexB_list = json.loads(row[tableB_name])
                        if len(indexA_list) == 1:
                            index_pairA.append((indexA_list, indexB_list))
                        if len(indexB_list) == 1:
                            index_pairB.append((indexA_list, indexB_list))
                except:
                    index_pairA = []
                    index_pairB = []

            else:
                join_index = {}
                value_all = np.union1d(tableA[keyA].dropna().values, tableB[keyB].dropna().values)

                for v in value_all:
                    join_index[v] = {}
                    join_index[v]['A'] = []
                    join_index[v]['B'] = []

                for iter, row in tableA.iterrows():
                    if row[keyA] in join_index:
                        join_index[row[keyA]]['A'].append(iter)

                for iter, row in tableB.iterrows():
                    if row[keyB] in join_index:
                        join_index[row[keyB]]['B'].append(iter)

                index_pairA = []
                index_pairB = []

                for v in join_index.keys():
                    indexA_list = join_index[v]['A']
                    indexB_list = join_index[v]['B']
                    if len(indexA_list) == 1:
                        index_pairA.append((indexA_list, indexB_list))
                    if len(indexB_list) == 1:
                        index_pairB.append((indexA_list, indexB_list))

            index_pair = []
            if len(index_pairA) > len(index_pairB):

                index_pair = index_pairA

                if len(index_pair) == 0:
                    continue

                identical_num = 0
                for cck in mapping.keys():

                    if cck == keyA:
                        continue

                    cckb = mapping[cck]
                    identical_cnt = 0
                    for a, b in index_pair:
                        try:
                            if tableA[cck].loc[a[0]] in tableB[cckb].loc[b].tolist():
                                identical_num = identical_num + 1
                                identical_cnt = identical_cnt + 1
                        except:
                            logging.info(a)
                            logging.info(b)

                    if len(index_pair) == 0:
                        identical_flt = 0
                    else:
                        identical_flt = float(identical_cnt) / float(len(index_pair))

                    # print(identical_flt)
                    if identical_flt > dep_thres:
                        if ck not in key_depen:
                            key_depen[ck] = []
                        key_depen[ck].append(cck)

                key_rank.append((ck, identical_num))

            else:
                index_pair = index_pairB

                if len(index_pair) == 0:
                    continue

                identical_num = 0
                for cck in mapping.keys():

                    if cck == keyA:
                        continue

                    cckb = mapping[cck]
                    identical_cnt = 0
                    for a, b in index_pair:
                        if tableB[cckb].loc[b[0]] in tableA[cck].loc[a].tolist():
                            identical_num = identical_num + 1
                            identical_cnt = identical_cnt + 1

                    if len(index_pair) == 0:
                        identical_flt = 0
                    else:
                        identical_flt = float(identical_cnt) / float(len(index_pair))

                    # print(identical_flt)
                    if identical_flt > dep_thres:
                        if ck not in key_depen:
                            key_depen[ck] = []
                        key_depen[ck].append(cck)

                key_rank.append((ck, identical_num))

        key_rank = sorted(key_rank, key=lambda d: d[1], reverse=True)
        # print(key_rank)

        if len(key_rank) == 0:
            return [], {}

        key_return = key_rank[0][0]
        col_depend = key_depen[key_return]

        if key_return in mapping2C:
            key_return_cached = mapping2C[key_return]
            col_depend_cached = []
            for col in col_depend:
                col_depend_cached.append(mapping2C[col])
            if key_return_cached not in self.cached_table_fdepen:
                self.cached_table_fdepen[key_return_cached] = col_depend_cached
            else:
                a_cached = self.cached_table_fdepen[key_return_cached]
                for col_insert in col_depend_cached:
                    if col_insert not in a_cached:
                        a_cached.append(col_insert)
                self.cached_table_fdepen[key_return_cached] = a_cached

            store_dep = self.dumps_json(self.cached_table_fdepen)
            col = []
            col_value = []
            for t_key in store_dep.keys():
                col.append(t_key)
                col_value.append(store_dep[t_key])
            dict_df = {'col': col, 'value': col_value}
            df_store = pd.DataFrame.from_dict(dict_df)
            df_store.to_sql("fkey_dep", self.psql_eng, schema="table_profile", if_exists='replace', index=False)

        return key_return, col_depend



    def continue_full_mapping(self, tableA, tableB, mapped):

        # start_time = timeit.default_timer()
        matching = []
        Mpair = mapped
        MpairR = {}
        for i in Mpair.keys():
            MpairR[Mpair[i]] = i

        scma = tableA.columns.tolist()
        scmb = tableB.columns.tolist()
        shmal = len(scma)
        shmbl = len(scmb)

        acol_set = {}
        for i in range(shmal):
            nameA = scma[i]
            if nameA in Mpair:
                continue

            if tableA[nameA].dtype != float:
                continue

            if nameA not in acol_set:
                colA = tableA[scma[i]].dropna().tolist()
                acol_set[nameA] = list(set(colA))
            else:
                colA = acol_set[nameA]

            for j in range(shmbl):
                nameB = scmb[j]
                if nameB in MpairR:
                    continue

                if tableA[nameA].dtype != tableB[nameB].dtype:
                    continue

                colB = tableB[nameB].dropna().tolist()
                sim_col = jaccard_similarity(colA, colB)

                if sim_col > self.sim_thres:
                    matching.append((nameA, nameB, sim_col))
                    if sim_col > 0.8:
                        break

        matching = sorted(matching, key=lambda d: d[2], reverse=True)

        for i in range(len(matching)):
            if matching[i][2] < self.sim_thres:
                break
            else:
                if matching[i][0] not in Mpair and matching[i][1] not in MpairR:
                    Mpair[matching[i][0]] = matching[i][1]
                    MpairR[matching[i][1]] = matching[i][0]

        return Mpair

    def continue_full_dependency(self, tableA, tableB, tableA_name, tableB_name, key, full_mapping, mapping2c, dep,
                                 dep_thres):

        keyc = mapping2c[key]
        join_index = self.cached_group_tables[keyc][[tableA_name, tableB_name]].dropna()
        index_pair = []
        key_indexA = []
        key_indexB = []

        for iter, row in join_index.iterrows():
            indexA_list = json.loads(row[tableA_name])
            indexB_list = json.loads(row[tableB_name])

            if len(indexA_list) != 1 or len(indexB_list) != 1:
                continue
            else:
                index_pair.append((indexA_list[0], indexB_list[0]))
                key_indexA.append(indexA_list[0])
                key_indexB.append(indexB_list[0])

        for cck in full_mapping.keys():
            if cck == key:
                continue

            if cck in dep:
                continue

            cckb = full_mapping[cck]

            identical_cnt = 0
            for a, b in index_pair:
                try:
                    if tableA[cck].loc[a] == tableB[cckb].loc[b]:
                        identical_cnt = identical_cnt + 1
                except:
                    identical_cnt = identical_cnt + 0
            if len(index_pair) == 0:
                continue
            identical_flt = float(identical_cnt) / float(len(index_pair))

            if identical_flt > dep_thres:
                dep.append(cck)

        return dep, key_indexA, key_indexB

    def continue_full_foreign_dependency(self, tableA, tableB, tableA_name, tableB_name, key, full_mapping, mapping2c,
                                         dep, dep_thres):

        keyc = mapping2c[key]

        if tableA_name in self.cached_group_tables[keyc].columns and tableB_name in self.cached_group_tables[
            keyc].columns:

            join_index = self.cached_group_tables[keyc][[tableA_name, tableB_name]].dropna()
            index_pairA = []
            index_pairB = []
            for iter, row in join_index.iterrows():
                try:
                    indexA_list = json.loads(row[tableA_name])
                except:
                    indexA_list = row[tableA_name]

                try:
                    indexB_list = json.loads(row[tableB_name])
                except:
                    indexB_list = row[tableB_name]

                if len(indexA_list) == 1:
                    index_pairA.append((indexA_list, indexB_list))
                if len(indexB_list) == 1:
                    index_pairB.append((indexA_list, indexB_list))

            index_pair = []
            if len(index_pairA) > len(index_pairB):
                index_pair = index_pairA
                if len(index_pair) == 0:
                    return dep

                for cck in full_mapping.keys():
                    if cck == key:
                        continue
                    if cck in dep:
                        continue

                    cckb = full_mapping[cck]

                    identical_cnt = 0
                    for a, b in index_pair:
                        if tableA[cck].loc[a[0]] in tableB[cckb].loc[b].tolist():
                            identical_cnt = identical_cnt + 1
                    identical_flt = float(identical_cnt) / float(len(index_pair))
                    if identical_flt > dep_thres:
                        dep.append(cck)
            else:
                index_pair = index_pairB
                if len(index_pair) == 0:
                    return dep

                for cck in full_mapping.keys():
                    if cck == key:
                        continue
                    if cck in dep:
                        continue

                    cckb = full_mapping[cck]

                    identical_cnt = 0
                    for a, b in index_pair:
                        if tableB[cckb].loc[b[0]] in tableA[cck].loc[a].tolist():
                            identical_cnt = identical_cnt + 1

                    identical_flt = float(identical_cnt) / float(len(index_pair))
                    if identical_flt > dep_thres:
                        dep.append(cck)

        return dep

