from data_extension.table_db import generate_graph
from data_extension.table_db import parse_code
from data_extension.table_db import pre_vars
import data_extension.config as cfg

import json
from sys import getsizeof
import os
import pickle

import random
import sys
import timeit
import copy
import queue
import logging
import networkx as nx

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

special_type = ['np', 'pd']

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def _getsizeof(x):
    # return the size of variable x. Amended version of sys.getsizeof
    # which also supports ndarray, Series and DataFrame
    if type(x).__name__ in ['ndarray', 'Series']:
        return x.nbytes
    elif type(x).__name__ == 'DataFrame':
        return x.memory_usage().sum()
    else:
        return getsizeof(x)

def last_line_var(varname, code):
    code = code.split('\n')
    ret = 0
    for id, i in enumerate(code):
        if '=' not in i:
            continue
        j = i.split('=')
        if varname in j[0]:
            ret = id + 1
    return ret


class TableSearch:
    query = None
    eng = None
    geng = None

    real_tables = {}

    def __init__(self, dbname, schema = None):
        self.query = None
        self.eng = connect2db(dbname)
        self.geng = connect2gdb()

        self.real_tables = {}


class SearchProv:

    def __star_dis(self, listA, listB):
        listA = sorted(listA)
        listB = sorted(listB)
        #print(listA, listB)
        lenA = len(listA)
        lenB = len(listB)
        i = 0
        j = 0
        interset = []
        while(i < lenA and j < lenB):
            #print(listA[i], listB[j], listA[i] == listB[j])
            if listA[i] == listB[j]:
                interset.append(listA[i])
                i += 1
                j += 1
            elif listA[i] < listB[j]:
                i += 1
            elif listA[i] > listB[j]:
                j += 1
            else:
                print(i,j)
        dist = abs(lenA - lenB) + max(lenA, lenB) - len(interset)
        return dist

    def Star_Mapping_Dis(self, query, root):
        mapo = {}
        mapr = {}
        distance_pair = []
        for i in query.keys():
            stara = query[i].values()
            for j in root.keys():
                starb = root[j].values()
                simAB = self.__star_dis(stara, starb)
                distance_pair.append((i, j, simAB))
        distance_pair = sorted(distance_pair, key=lambda d: d[2])
        #    print(distance_pair)

        distance_ret = 0
        for i, j, k in distance_pair:
            if i not in mapo and j not in mapr:
                mapo[i] = j
                mapr[j] = i
                distance_ret = distance_ret + k

        return distance_ret

    def Graph_Edit_Dis(self, query, k):
        distance_rank = []
        for i in self.Graphs_Dependencies.keys():
            for j in self.Graphs_Dependencies[i].keys():
                dist = self.Star_Mapping_Dis(query, self.Graphs_Dependencies[i][j])
                distance_rank.append((j, dist))
        distance_rank = sorted(distance_rank, key=lambda d: d[1])
        if k > len(distance_rank):
            k = len(distance_rank)
        res = []
        for i, j in distance_rank[:k]:
            res.append(i)
        return res

    def __get_pre_depedencies4node(self, graph, qnode):
        node_list = {}
        q = queue.Queue()
        q.put(qnode)
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

    def __get_all_node_dependencies(self):
        gnodes_subgraph = {}
        for g in self.Graphs.keys():
            graph = self.Graphs[g]
            gnodes_subgraph[g] = {}
            gnodes = list(graph.nodes)
            for n in gnodes:
                gnodes_subgraph[g][n] = self.__get_pre_depedencies4node(graph, n)
        return gnodes_subgraph

    def __read_models(self):
        files = os.listdir(self.model_address)
        Graphs = {}
        for i in files:
            G = pickle.load(open(self.model_address + "/" + i, "rb"))
            Graphs[i] = G
        return Graphs

    def __init__(self, graphs):
        self.Graphs = graphs
        self.Graphs_Dependencies = self.__get_all_node_dependencies()

    def search_topk(self, query, k):
        return self.Graph_Edit_Dis(query, k)



def search_tables(search_test, var_df, mode, code, var_name):

    #if mode == 0:
    #    table = pd.read_sql_table(var_df, search_test.eng, schema = 'rowstore')
    #    return table.to_html()
    #else:
    query_table = var_df #dataframe
    query_name = var_name #name of the table

    if mode == 1:
        logging.info("Search for Similar Tables!")
        tables = search_test.search_similar_tables_threshold2(query_table, 0.5, 10, 1.5, 0.9, 0.2)
        logging.info("%s Similar Tables are returned!"%len(tables))
        #tables = search_test.search_similar_tables_threshold2(query_table, 10, 0.5, 5, 1, 0.9, 0.2, True, 10)
    elif mode == 2:
        logging.info("Search for Joinable Tables!")
        tables = search_test.search_joinable_tables_threshold2(query_table, 0.1, 10, 1.5, 0.9, 0.2)
        logging.info("%s Joinable Tables are returned!"%len(tables))
    elif mode == 3:
        logging.info("Search for Provenance Similar Tables!")
        code = '\n'.join([t for t in code.split('\\n') if len(t)> 0 and t[0]!='%'])
        code = '\''.join(code.split('\\\''))
        line_id = last_line_var(var_name, code)
        dependency = parse_code(code)
        graph = generate_graph(dependency)
        query_name = 'var_' + var_name + '_' + str(line_id)
        query_node = pre_vars(query_name, graph)
        tables = search_test.search_role_sim_tables(query_node, 10)
        logging.info("%s Provenance Similar Tables are returned!"%len(tables))



    if len(tables) == 0:
        return ""
    else:
        vardic = [{'varName': v[0], 'varType': type(v[1]).__name__, 'varSize': str(v[1].size), 'varContent': v[1].to_html(index_names = True, justify = 'center', max_rows = 10, max_cols = 5, header = True)} for v in tables] # noqa
        return json.dumps(vardic)
    #return search_test

#print(search_tables())