import pandas as pd
import numpy as np
from juneau.utils.utils import jaccard_similarity

class Map:

    @staticmethod
    def mapping(tableA, tableB, Mpair = {}, MpairR = {}):

        matching = []

        scma = tableA.columns.values
        scmb = tableB.columns.values
        shmal = len(scma)
        shmbl = len(scmb)

        acol_set = {}

        for i in range(shmal):

            nameA = scma[i]

            if nameA in Mpair:
                continue
            if nameA == "Unnamed: 0" or "index" in nameA:
                continue

            colA = tableA[nameA][~pd.isnull(tableA[nameA])].values
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
                if nameB in MpairR:
                    continue

                if nameB == "Unnamed: 0" or "index" in nameB:
                    continue

                if tableA[nameA].dtype != tableB[nameB].dtype:
                    continue

                colB = tableB[nameB][~pd.isnull(tableB[nameB])].values

                try:
                    colB = colB[~np.isnan(colB)]
                except:
                    try:
                        colB = colB[colB != np.array(None)]
                    except:
                        colB = colB


                sim_col = jaccard_similarity(acol_set[nameA], colB)
                matching.append((nameA, nameB, sim_col))

        matching = sorted(matching, key=lambda d: d[2], reverse=True)


        return matching

