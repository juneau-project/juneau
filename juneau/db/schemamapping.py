# Copyright 2020 Juneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Handling of both indexing and search which helps us
understand the relationship between two tables.
"""

import numpy as np
import pandas as pd
import timeit


class SchemaMapping:
    def __init__(self, sim_thres=0.3):
        self.sim_thres = sim_thres

    @staticmethod
    def jaccard_similarity(colA, colB):
        """
        The Jaccard similarity between two sets A and B is defined as
        |intersection(A, B)| / |union(A, B)|.
        """
        if min(len(colA), len(colB)) == 0:
            return 0
        colA = np.array(colA)
        union = len(np.union1d(colA, colB))
        inter = len(np.intersect1d(colA, colB))
        return float(inter) / float(union)

    def mapping_naive(self, tableA, tableB, mapped=None):

        start_time = timeit.default_timer()
        time1 = 0
        c1 = 0

        matching = []
        Mpair = mapped
        MpairR = {}
        for i in Mpair.keys():
            MpairR[Mpair[i]] = i

        scma = tableA.columns.values
        scmb = tableB.columns.values
        shmal = len(scma)
        shmbl = len(scmb)

        acol_set = {}

        for i in range(shmal):

            nameA = scma[i]
            if nameA in Mpair or nameA == "Unnamed: 0" or "index" in nameA:
                continue

            colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values
            if nameA not in acol_set:
                acol_set[nameA] = list(set(colA))

            for j in range(shmbl):

                nameB = scmb[j]
                if nameB in MpairR or nameB == "Unnamed: 0" or "index" in nameB or \
                        tableA[scma[i]].dtype != tableB[scmb[j]].dtype:
                    continue

                colB = tableB[scmb[j]][~pd.isnull(tableB[scmb[j]])].values
                colB = colB[~np.isnan(colB)]

                s1 = timeit.default_timer()
                sim_col = SchemaMapping.jaccard_similarity(acol_set[nameA], colB)
                e1 = timeit.default_timer()
                time1 += e1 - s1
                c1 += 1

                matching.append((nameA, nameB, sim_col))

        matching = sorted(matching, key=lambda d: d[2], reverse=True)

        for i in range(len(matching)):
            if matching[i][2] < self.sim_thres:
                break
            else:
                if matching[i][0] not in Mpair and matching[i][1] not in MpairR:
                    Mpair[matching[i][0]] = matching[i][1]
                    MpairR[matching[i][1]] = matching[i][0]
        if len(matching) == 0:
            rv = 0
        else:
            rv = matching[0][2]

        end_time = timeit.default_timer()
        print("raw schema mapping: ", end_time - start_time)
        print("sim schema mapping: ", time1)
        print("sim times: ", c1)
        return Mpair, rv

    # Do full schema mapping
    def mapping_naive_incremental(
            self, tableA, tableB, gid, meta_mapping, schema_linking, unmatched, mapped=None
    ):

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

            if nameA in Mpair or nameA == "Unnamed: 0" or "index" in nameA:
                continue

            if nameA not in acol_set:
                colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values
                acol_set[nameA] = list(set(colA))
            else:
                colA = acol_set[nameA]

            for j in range(shmbl):

                nameB = scmb[j]
                if nameB in MpairR or nameB == "Unnamed: 0" or "index" in nameB or \
                        tableA[nameA].dtype != tableB[nameB].dtype or nameB in unmatched[gid][nameA]:
                    continue

                colB = tableB[scmb[j]][~pd.isnull(tableB[scmb[j]])].values

                colB = colB[~np.isnan(colB)]

                s1 = timeit.default_timer()
                sim_col = self.jaccard_similarity(colA, colB)
                e1 = timeit.default_timer()
                time1 += e1 - s1

                if sim_col < self.sim_thres:
                    unmatched[gid][nameA][nameB] = ""

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
                        unmatched[gid][i][j] = ""

        end_time = timeit.default_timer()
        time_total = end_time - start_time
        return Mpair, meta_mapping, unmatched, time_total

    # Do schema mapping for tables when looking for similar tables
    def mapping_naive_tables(
            self, tableA, valid_keys, schema_element, schema_dtype, tflag=False
    ):

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
                if nameA == "Unnamed: 0" or "index" in nameA or nameA not in valid_keys:
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

                    colB = colB[~np.isnan(colB)]

                    s1 = timeit.default_timer()

                    sim_col = self.jaccard_similarity(colA, colB)

                    e1 = timeit.default_timer()
                    time1 += e1 - s1

                    matching.append((nameA, nameB, sim_col))

            matching = sorted(matching, key=lambda d: d[2], reverse=True)

            for i in range(len(matching)):
                if matching[i][2] < self.sim_thres:
                    break
                else:
                    if (
                            matching[i][0] not in Mpair[group]
                            and matching[i][1] not in MpairR[group]
                    ):
                        Mpair[group][matching[i][0]] = matching[i][1]
                        MpairR[group][matching[i][1]] = matching[i][0]

        end_time = timeit.default_timer()

        if tflag:
            print("Schema Mapping Before Search: %s Seconds." % (end_time - start_time))

        return Mpair

    # Do schema mapping for tables when looking for joinable tables
    def mapping_naive_tables_join(
            self,
            tableA,
            valid_keys,
            schema_element_sample,
            schema_element,
            schema_dtype,
            unmatched,
            tflag=False,
    ):

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
                    colB = colB[~np.isnan(colB)]

                    s1 = timeit.default_timer()
                    sim_col = self.jaccard_similarity(colA, colB)
                    if sim_col < self.sim_thres:
                        unmatched[group][nameA][nameB] = ""

                    e1 = timeit.default_timer()
                    time1 += e1 - s1

                    matching.append((nameA, nameB, sim_col))

            for i in schema_element_sample[group].keys():

                nameB = i

                if nameB == "Unnamed: 0" or "index" in nameB:
                    continue

                colB = np.array(schema_element_sample[group][nameB])
                colB = colB[~np.isnan(colB)]

                for j in range(shmal):

                    nameA = scma[j]
                    if nameA == "Unnamed: 0" or "index" in nameA or nameB in unmatched[group][nameA]:
                        continue

                    if nameA not in acol_set:
                        colA = tableA[nameA][~pd.isnull(tableA[nameA])].values
                        acol_set[nameA] = list(set(colA))
                    else:
                        colA = acol_set[nameA]

                    if schema_dtype[group][nameB] is not tableA[nameA].dtype:
                        continue

                    s1 = timeit.default_timer()
                    sim_col = self.jaccard_similarity(colA, colB)
                    e1 = timeit.default_timer()
                    time1 += e1 - s1

                    if sim_col < self.sim_thres:
                        unmatched[group][nameA][nameB] = ""

                    matching.append((nameA, nameB, sim_col))

            matching = sorted(matching, key=lambda d: d[2], reverse=True)

            for i in range(len(matching)):
                if matching[i][2] < self.sim_thres:
                    break
                else:
                    if (
                        matching[i][0] not in Mpair[group]
                        and matching[i][1] not in MpairR[group]
                    ):
                        Mpair[group][matching[i][0]] = matching[i][1]
                        MpairR[group][matching[i][1]] = matching[i][0]

        end_time = timeit.default_timer()

        if tflag:
            print("raw schema mapping: ", end_time - start_time)
            print("sim schema mapping: ", time1)

        return Mpair, unmatched

    # Do schema mapping on Groups
    def mapping_naive_groups(self, tableA, tableA_valid, schema_element):

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
                if nameA not in tableA_valid or nameA == "Unnamed: 0" or "index" in nameA:
                    continue

                colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values
                if nameA not in acol_set:
                    acol_set[nameA] = list(set(colA))

                for j in schema_element[group].keys():

                    nameB = j
                    colB = np.array(schema_element[group][nameB])

                    colB = colB[~np.isnan(colB)]

                    s1 = timeit.default_timer()

                    sim_col = self.jaccard_similarity(acol_set[nameA], colB)
                    e1 = timeit.default_timer()
                    time1 += e1 - s1
                    matching.append((nameA, nameB, sim_col))

            matching = sorted(matching, key=lambda d: d[2], reverse=True)

            if len(matching) == 0:
                continue

            if matching[0][2] < self.sim_thres:
                continue
            else:
                group_list.append(group)

        return group_list
