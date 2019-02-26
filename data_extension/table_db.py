from sqlalchemy import create_engine
from py2neo import Graph, Node, Relationship, cypher, NodeMatcher
import numpy as np
import pandas as pd

user_name = "yizhang"
password = "yizhang"
dbname = "joinstore"

def connect2db(dbname):
    engine = create_engine("postgresql://" + user_name + ":" + password + "@localhost/" + dbname)
    return engine.connect()

def connect2gdb():
    graph = Graph("http://neo4j:yizhang@localhost:7474/db/data")
    return graph

def fetch_all_table_names(schema, eng):
    tables = eng.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = \'" + schema + "\';")
    base_table_name = []
    for tb in tables :
        base_table_name.append(tb[0])
    return base_table_name

def fetch_all_views(eng):
    tables = eng.execute("select table_name from INFORMATION_SCHEMA.views;")
    views = []
    for rows in tables:
        t_name = rows[0]
        if "exp_view_table" in t_name:
            views.append(t_name)
    return views

class SchemaMapping:

    def __init__(self, sim_thres = 0.5):
        self.sim_thres = sim_thres

    def __jaccard_similarity(self, colA, colB):

        if min(len(colA), len(colB)) == 0:
            return 0

        union = len(np.union1d(colA, colB))
        inter = len(np.intersect1d(colA, colB))
        return float(inter)/float(union)

    def mapping_naive(self, tableA, tableB):

        matching = []
        Mpair = {}
        MpairR = {}

        scma = tableA.columns.values
        scmb = tableB.columns.values
        shmal = len(scma)
        shmbl = len(scmb)

        for i in range(shmal):

            nameA = scma[i]
            if nameA == "Unnamed: 0" or "index" in nameA:
                continue

            colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values

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

                sim_col = self.__jaccard_similarity(colA, colB)
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
        return Mpair, rv

    def mapping_naive_groups(self, tableA, schema_element):

        Mpair = {}
        MpairR = {}

        scma = tableA.columns.values
        shmal = len(scma)

        for group in schema_element.keys():
            Mpair[group] = {}
            MpairR[group] = {}
            matching = []

            for i in range(shmal):

                nameA = scma[i]
                if nameA == "Unnamed: 0" or "index" in nameA:
                    continue

                colA = tableA[scma[i]][~pd.isnull(tableA[scma[i]])].values

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

                        sim_col = self.__jaccard_similarity(colA, colB)
                        matching.append((nameA, nameB, sim_col))

            matching = sorted(matching, key=lambda d: d[2], reverse=True)

            for i in range(len(matching)):
                if matching[i][2] < self.sim_thres:
                    break
                else:
                    if matching[i][0] not in Mpair[group] and matching[i][1] not in MpairR[group]:
                        Mpair[group][matching[i][0]] = matching[i][1]
                        MpairR[group][matching[i][1]] = matching[i][0]

        return Mpair