import numpy as np
import pandas as pd
import timeit


class SchemaMapping_SK:

    def __init__(self, sim_thres = 0.2):
        self.sim_thres = sim_thres

    def jaccard_similarity(self, colA, colB):

        if min(len(colA), len(colB)) == 0:
            return 0
        #colA = np.array(colA)
        union = len(np.union1d(colA, colB))
        inter = len(np.intersect1d(colA, colB))
        return float(inter)/float(union)

    def mapping_naive(self, tableA, tableB):

        start_time = timeit.default_timer()
        time1 = 0
        c1 = 0

        matching = []
        Mpair = {}
        MpairR = {}

        scma = tableA.columns.values
        scmb = tableB.columns.values
        shmal = len(scma)
        shmbl = len(scmb)

        acol_set = {}

        for i in range(shmal):

            nameA = scma[i]
            if nameA == "Unnamed: 0" or "index" in nameA:
                continue

            colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values
            if nameA not in acol_set:
                acol_set[nameA] = list(set(colA))

            try:
                colA = colA[~np.isnan(colA)]
            except:
                try:
                    colA = colA[colA != np.array(None)]
                except:
                    colA = colA

            for j in range(shmbl):

                nameB = scmb[j]  # .split('_')[0].lower()

                if nameB == "Unnamed: 0" or "index" in nameB:
                    continue

                if tableA[scma[i]].dtype != tableB[scmb[j]].dtype:
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
                sim_col = self.jaccard_similarity(acol_set[nameA], colB)
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
        print('raw schema mapping: ', end_time - start_time)
        print('sim schema mapping: ', time1)
        print('sim times: ', c1)
        return Mpair, rv

    def mapping_naive_tables(self, tableA, valid_keys, schema_element, group_array):

        start_time = timeit.default_timer()
        time1 = 0
        c1 = 0

        Mpair = {}
        MpairR = {}

        scma = list(tableA.keys())
        shmal = len(scma)

        for group in schema_element.keys():

            if group not in group_array:
                continue

            Mpair[group] = {}
            MpairR[group] = {}
            matching = []

            for i in range(shmal):

                nameA = scma[i]

                if nameA not in valid_keys:
                    continue

                colA = tableA[nameA]

                for j in schema_element[group].keys():

                    nameB = j
                    colB = np.array(schema_element[group][nameB])

                    s1 = timeit.default_timer()

                    sim_col = self.jaccard_similarity(colA, colB)
                    e1 = timeit.default_timer()
                    time1 += e1 - s1
                    c1 += 1

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
        print('raw schema mapping: ', end_time - start_time)
        print('sim schema mapping: ', time1)
        print('sim times: ', c1)
        return Mpair

    def mapping_naive_groups(self, tableA, schema_element):

        start_time = timeit.default_timer()
        time1 = 0
        c1 = 0

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

                if nameA == "Unnamed: 0" or "index" in nameA:
                    continue

                colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values
                if nameA not in acol_set:
                    acol_set[nameA] = list(set(colA))

                try:
                    colA = colA[~np.isnan(colA)]
                except:
                    try:
                        colA = colA[colA != np.array(None)]
                    except:
                        colA = colA

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

                    sim_col = self.jaccard_similarity(acol_set[nameA], colB)
                    e1 = timeit.default_timer()
                    time1 += e1 - s1
                    c1 += 1

                    matching.append((nameA, nameB, sim_col))

            matching = sorted(matching, key=lambda d: d[2], reverse=True)
            if len(matching) == 0:
                #print(scma)
                #print(schema_element[group].keys())
                continue

            if matching[0][2] < self.sim_thres:
                continue
            else:
                group_list.append(group)

        end_time = timeit.default_timer()
#        print('raw schema mapping: ', end_time - start_time)
#        print('sim schema mapping: ', time1)
#        print('sim times: ', c1)

        return group_list

