from sqlalchemy import create_engine
from py2neo import Graph, Node, Relationship, cypher, NodeMatcher
import queue
import networkx as nx

from data_extension.funclister import FuncLister
import ast

import data_extension.config as cfg

special_type = ['np', 'pd']


def create_tables_as_needed(eng):
    """
    Creates the PostgreSQL schema and Juneau's metadata tables, if necessary

    :param eng: SQL engine
    """
    eng.execute("create schema if not exists " + cfg.sql_dbs + ';')
    eng.execute("create schema if not exists " + cfg.sql_graph + ';')
    eng.execute("create schema if not exists " + cfg.sql_provenance + ';')

    eng.execute("CREATE TABLE IF NOT EXISTS graph_model.dependen (" + \
                "view_id character varying(1000)," + \
                "view_cmd text" + \
                ");")

    eng.execute("CREATE TABLE IF NOT EXISTS graph_model.line2cid (" + \
                "view_id character varying(1000)," + \
                "view_cmd text" + \
                ");")


def connect2db(dbname):
    """
    Connect to the PostgreSQL instance, creating it if necessary

    :param dbname:
    :return:
    """
    try:
        engine = create_engine("postgresql://" + cfg.sql_name + ":" + cfg.sql_password + "@" + cfg.sql_host +\
                               "/" + dbname)#cfg.sql_dbname)

        eng = engine.connect()
        create_tables_as_needed(eng)

        return eng
    except:
        engine = create_engine("postgresql://" + cfg.sql_name + ":" + cfg.sql_password + "@" + cfg.sql_host + \
                               "/")
        eng = engine.connect()
        eng.connection.connection.set_isolation_level(0)
        eng.execute("create database " + dbname + ';')#'#cfg.sql_dbname + ';')

        create_tables_as_needed(eng)
        eng.connection.connection.set_isolation_level(1)

        engine = create_engine("postgresql://" + cfg.sql_name + ":" + cfg.sql_password + "@" + cfg.sql_host +\
                               "/" + dbname)#cfg.sql_dbname)
        return engine.connect()

def connect2gdb():
    """
    Connect to Neo4J
    :return:
    """
    graph = Graph("http://" + cfg.neo_name + ":" + cfg.neo_password + "@" + cfg.neo_host + "/db/" + cfg.neo_db)
    return graph

def fetch_all_table_names(schema, eng):
    """
    Find all tables within a given schema

    :param schema: Schema to search
    :param eng: Engine connection
    :return:
    """
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

#test2 = Store_Provenance()
#test2.create_prov_graph(test.dependency, 0)



#print(G.nodes)
#print(list(G.successors('var_data_19_0')))
#print(str(G))


            #candidate_list = matcher.match('Var_temp', var=dep).order_by("_.line_num")
            #candidate = None
            #for cand_node in candidate_list.__iter__():
            #    candidate = cand_node

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