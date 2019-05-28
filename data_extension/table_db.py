from sqlalchemy import create_engine
from py2neo import Graph, Node, Relationship, cypher, NodeMatcher
import numpy as np
import pandas as pd
import timeit
import ast
import queue
import networkx as nx

user_name = "yizhang"
password = "yizhang"
dbname = "joinstore"

special_type = ['np', 'pd']

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

    def __init__(self, sim_thres = 0.3):
        self.sim_thres = sim_thres

    def jaccard_similarity(self, colA, colB):

        if min(len(colA), len(colB)) == 0:
            return 0
        colA = np.array(colA)
        union = len(np.union1d(colA, colB))
        inter = len(np.intersect1d(colA, colB))
        return float(inter)/float(union)

    def mapping_naive(self, tableA, tableB, mapped = {}):

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
            if nameA in Mpair:
                continue
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
                if nameB in MpairR:
                    continue

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
                sim_col = self.jaccard_similarity(colA, colB)
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

                    sim_col = self.jaccard_similarity(colA, colB)

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
                        sim_col = self.jaccard_similarity(colA, colB)
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
                    sim_col = self.jaccard_similarity(colA, colB)
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

                    sim_col = self.jaccard_similarity(acol_set[nameA], colB)
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

class FuncLister(ast.NodeVisitor):
    def __init__(self):
        self.dependency = {}

    def visit_Attribute(self, node):
        return_node = []
        if 'id' in node.value.__dict__:
            return_node.append((node.value.id, node.attr))
        else:
            ret = self.visit(node.value)
            if ret:
                return_node = return_node + ret

        return return_node

    def visit_Index(self, node):
        return_node = []
        if 'id' in node.value.__dict__:
            return_node.append((node.value.id, 'Index'))
        else:
            ret = self.visit(node.value)
            if ret:
                return_node = return_node + ret
        return return_node

    def visit_Subscript(self, node):

        return_node = []

        if 'id' in node.value.__dict__:
            return_node.append((node.value.id, 'Index'))
        else:
            ret = self.visit(node.value)
            if ret:
                return_node = return_node + ret

        ret = self.visit(node.slice)
        if ret:
            return_node = return_node + ret

        return return_node

    def visit_UnaryOp(self, node):
        return_node = []
        if 'id' in node.operand.__dict__:
            return_node.append((node.operand.id, str(node.op)))
        else:
            ret = self.visit(node.operand)
            if ret != None:
                return_node = return_node + ret
        return return_node

    def visit_BinOp(self, node):

        return_node = []
        if 'id' in node.left.__dict__:
            return_node.append((node.left.id, str(node.op)))
        else:
            ret = self.visit(node.left)
            if ret != None:
                return_node = return_node + ret

        if 'id' in node.right.__dict__:
            return_node.append((node.right.id, str(node.op)))
        else:
            ret = self.visit(node.right)
            if ret != None:
                return_node = return_node + ret

        return return_node

    def visit_BoolOp(self, node):
        return_node = []
        for nv in node.values:
            if 'id' in nv.__dict__:
                return_node.append((nv.id, str(node.op)))
            else:
                ret = self.visit(nv)
                if ret != None:
                    return_node = return_node + ret

        return return_node

    def visit_Compare(self, node):
        return_node = []
        if 'id' in node.left.__dict__:
            return_node.append((node.left.id, str(node.ops)))
        else:
            ret = self.visit(node.left)
            if ret:
                return_node = return_node + ret

        for nc in node.comparators:
            if 'id' in nc.__dict__:
                return_node.append((nc.id, str(node.ops)))
            else:
                ret = self.visit(nc)
                if ret:
                    return_node = return_node + ret
        return return_node

    def visit_List(self, node):
        return_node = []
        if 'elts' in node.__dict__:
            for ele in node.elts:
                if 'id' in ele.__dict__:
                    return_node.append((ele.id,'List'))
                else:
                    ret = self.visit(ele)
                    if ret != None:
                        return_node = return_node + ret
        return return_node

    def visit_Tuple(self, node):
        return_node = []
        if 'elts' in node.__dict__:
            for ele in node.elts:
                if 'id' in ele.__dict__:
                    return_node.append((ele.id, 'Tuple'))
                else:
                    ret = self.visit(ele)
                    if ret != None:
                        return_node = return_node + ret
        return return_node

    def visit_Call(self, node):

        return_node = []
#        print(node.__dict__)
#        print(node.func.__dict__)

        ret = self.visit(node.func)
        if ret:
            return_node = return_node + ret

        for na in node.args:
            if 'id' in na.__dict__:
                if 'id' in node.func.__dict__:
                    return_node.append((na.id, node.func.id))
                else:
                    #print(node.func.attr)
                    return_node.append((na.id, node.func.attr))
            else:
                if 'id' in node.func.__dict__:
                    fname = node.func.id
                else:
                    fname = node.func.attr

                ret = self.visit(na)
                if ret != None:
                    for ri, rj in ret:
                        return_node.append((ri, fname))

        for nk in node.keywords:
            if 'value' in nk.__dict__:
                if 'id' in nk.value.__dict__:
                    if 'id' in node.func.__dict__:
                        return_node.append((nk.value.id, node.func.id))
                    else:
                        return_node.append((nk.value.id, node.func.attr))
                else:
                    if 'id' in node.func.__dict__:
                        fname = node.func.id
                    else:
                        fname = node.func.attr

                    ret = self.visit(nk.value)
                    if ret != None:
                        for ri, rj in ret:
                            return_node.append((ri, fname))



        return return_node

    def visit_Assign(self, node):
        #print("here!")
        #print(node.__dict__)

        left_array = []
        for nd in node.targets:
            if 'id' in nd.__dict__:
                left_array.append(nd.id)
            else:
                ret = self.visit(nd)
                if ret:
                    left_array = left_array + ret

        #print(node.value.__dict__)
        right_array = []
        if 'id' in node.value.__dict__:
            right_array.append((node.value.id, 'Assign'))
        else:
            ret = self.visit(node.value)
            if ret != None:
                right_array = right_array + ret

        self.dependency[node.lineno] = (left_array, right_array)



#test2 = Store_Provenance()
#test2.create_prov_graph(test.dependency, 0)



#print(G.nodes)
#print(list(G.successors('var_data_19_0')))
#print(str(G))


            #candidate_list = matcher.match('Var_temp', var=dep).order_by("_.line_num")
            #candidate = None
            #for cand_node in candidate_list.__iter__():
            #    candidate = cand_node

def parse_code(all_code):

    test = FuncLister()
    #print(all_code)
    tree = ast.parse(all_code)
    #print(tree)
    test.visit(tree)
    #print(test.dependency)

    return test.dependency

def generate_graph(dependency):
    G = nx.DiGraph()
    for i in dependency.keys():
        left = dependency[i][0]
        right = list(set(dependency[i][1]))

        left_node = []
        for ele in left:
            if type(ele) is tuple:
                ele = ele[0]
            left_node.append('var_' + ele + '_' + str(i))

        for ele in left:
            if type(ele) is tuple:
                ele = ele[0]

            new_node = 'var_' + ele + '_' + str(i)
            G.add_node(new_node, line_id = i, var = ele)

            for dep, ename in right:
                candidate_list = G.nodes
                rankbyline = []
                for cand in candidate_list:
                    if G.nodes[cand]['var'] == dep:
                        if cand in left_node:
                            continue
                        rankbyline.append((cand, G.nodes[cand]['line_id']))
                rankbyline = sorted(rankbyline, key = lambda d:d[1], reverse= True)

                if len(rankbyline) == 0:
                    if dep not in special_type:
                        candidate_node = 'var_' + dep + '_' + str(1)
                        G.add_node(candidate_node, line_id = 1, var = dep)
                    else:
                        candidate_node = dep
                        G.add_node(candidate_node, line_id = 1, var = dep)

                else:
                    candidate_node = rankbyline[0][0]

                G.add_edge(new_node, candidate_node, label = ename)

    return G

def pre_vars(node, graph):
    node_list = {}
    q = queue.Queue()
    q.put(node)
    while(not q.empty()):
        temp_node = q.get()
        if temp_node not in node_list:
            node_list[temp_node] = {}
        predecessors = graph.successors(temp_node)
        for n in predecessors:
            q.put(n)
            node_list[temp_node][n] = '+' + graph[temp_node][n]['label']
            successors = graph.predecessors(n)
            for s in successors:
                if s in node_list:
                    if n not in node_list:
                        node_list[n] = {}
                    node_list[n][s] = '-' + graph[s][n]['label']
    return node_list