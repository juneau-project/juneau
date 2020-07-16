import ast
import queue

import networkx as nx
from py2neo import Graph
from sqlalchemy import create_engine

from juneau import config
from juneau.utils.funclister import FuncLister

from sqlalchemy.orm import sessionmaker


def create_tables_as_needed(engine, eng):
    """
    Creates the PostgreSQL schema and Juneau's metadata tables, if necessary

    :param eng: SQL engine
    """
    # Open the session
    Session = sessionmaker(bind=engine)
    session = Session()

    eng.execute("create schema if not exists " + config.sql_dbs + ";")
    eng.execute("create schema if not exists " + config.sql_graph + ";")
    eng.execute("create schema if not exists " + config.sql_provenance + ";")

    eng.execute(
        "CREATE TABLE IF NOT EXISTS " + config.sql_graph + ".dependen ("
        + "view_id character varying(1000),"
        + "view_cmd text"
        + ");"
    )

    eng.execute(
        "CREATE TABLE IF NOT EXISTS " + config.sql_graph + ".line2cid ("
        + "view_id character varying(1000),"
        + "view_cmd text"
        + ");"
    )

    eng.execute(
        "CREATE TABLE IF NOT EXISTS " + config.sql_graph + ".lastliid ("
        + "view_id character varying(1000),"
        + "view_cmd text"
        + ");"
    )
    # Commit the changes
    session.commit()

    # Close the session
    session.close()


def connect2db(dbname):
    """
    Connect to the PostgreSQL instance, creating it if necessary

    :param dbname:
    :return:
    """
    try:
        engine = create_engine(
            "postgresql://"
            + config.sql_name
            + ":"
            + config.sql_password
            + "@"
            + config.sql_host
            + "/"
            + dbname
        )  # config.sql_dbname)

        eng = engine.connect()
        create_tables_as_needed(engine, eng)

        return eng
    except:
        engine = create_engine(
            "postgresql://"
            + config.sql_name
            + ":"
            + config.sql_password
            + "@"
            + config.sql_host
            + "/"
        )
        eng = engine.connect()
        eng.connection.connection.set_isolation_level(0)
        eng.execute("create database " + dbname + ";")  # '#config.sql_dbname + ';')

        create_tables_as_needed(engine, eng)
        eng.connection.connection.set_isolation_level(1)

        engine = create_engine(
            "postgresql://"
            + config.sql_name
            + ":"
            + config.sql_password
            + "@"
            + config.sql_host
            + "/"
            + dbname
        )  # config.sql_dbname)
        return engine.connect()


def connect2db_engine(dbname):
    """
    Connect to the PostgreSQL instance, creating it if necessary

    :param dbname:
    :return:
    """
    try:
        engine = create_engine(
            "postgresql://"
            + config.sql_name
            + ":"
            + config.sql_password
            + "@"
            + config.sql_host
            + "/"
            + dbname,
            isolation_level="AUTOCOMMIT",
        )  # config.sql_dbname)

        eng = engine.connect()
        create_tables_as_needed(engine, eng)
        eng.close()

        return engine
    except:
        engine = create_engine(
            "postgresql://"
            + config.sql_name
            + ":"
            + config.sql_password
            + "@"
            + config.sql_host
            + "/"
        )
        eng = engine.connect()
        eng.connection.connection.set_isolation_level(0)
        eng.execute("create database " + dbname + ";")  # '#config.sql_dbname + ';')

        create_tables_as_needed(engine, eng)
        eng.connection.connection.set_isolation_level(1)
        eng.close()

        engine = create_engine(
            "postgresql://"
            + config.sql_name
            + ":"
            + config.sql_password
            + "@"
            + config.sql_host
            + "/"
            + dbname,
            isolation_level="AUTOCOMMIT",
        )  # config.sql_dbname)
        return engine


def connect2gdb():
    """
    Connect to Neo4J
    :return:
    """
    graph = Graph(
        "http://"
        + config.neo_name
        + ":"
        + config.neo_password
        + "@"
        + config.neo_host
        + "/db/"
        + config.neo_db
    )
    return graph


def fetch_all_table_names(schema, eng):
    """
    Find all tables within a given schema

    :param schema: Schema to search
    :param eng: Engine connection
    :return:
    """
    tables = eng.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = '"
        + schema
        + "';"
    )
    base_table_name = []
    for tb in tables:
        base_table_name.append(tb[0])
    return base_table_name


def fetch_all_views(eng):
    tables = eng.execute("select table_name from information_schema.views;")
    views = []
    for rows in tables:
        t_name = rows[0]
        if "exp_view_table" in t_name:
            views.append(t_name)
    return views


def last_line_var(varname, code):
    code = code.split("\n")
    ret = 0
    for id, i in enumerate(code):
        if "=" not in i:
            continue
        j = i.split("=")
        if varname in j[0]:
            ret = id + 1
    return ret


def parse_code(all_code):
    test = FuncLister()
    # print(all_code)
    tree = ast.parse(all_code)
    # print(tree)
    test.visit(tree)
    # print(test.dependency)

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
            left_node.append("var_" + ele + "_" + str(i))

        for ele in left:
            if type(ele) is tuple:
                ele = ele[0]

            new_node = "var_" + ele + "_" + str(i)
            G.add_node(new_node, line_id=i, var=ele)

            for dep, ename in right:
                candidate_list = G.nodes
                rankbyline = []
                for cand in candidate_list:
                    if G.nodes[cand]["var"] == dep:
                        if cand in left_node:
                            continue
                        rankbyline.append((cand, G.nodes[cand]["line_id"]))
                rankbyline = sorted(rankbyline, key=lambda d: d[1], reverse=True)

                if len(rankbyline) == 0:
                    if dep not in ["np", "pd"]:
                        candidate_node = "var_" + dep + "_" + str(1)
                        G.add_node(candidate_node, line_id=1, var=dep)
                    else:
                        candidate_node = dep
                        G.add_node(candidate_node, line_id=1, var=dep)

                else:
                    candidate_node = rankbyline[0][0]

                G.add_edge(new_node, candidate_node, label=ename)

    return G


def pre_vars(node, graph):
    node_list = {}
    q = queue.Queue()
    q.put(node)
    dep = 0
    while not q.empty():
        temp_node = q.get()
        dep = dep + 1
        if temp_node not in node_list:
            node_list[temp_node] = {}
        predecessors = graph.successors(temp_node)
        for n in predecessors:
            q.put(n)
            node_list[temp_node][n] = "+" + graph[temp_node][n]["label"]
            successors = graph.predecessors(n)
            for s in successors:
                if s in node_list:
                    if n not in node_list:
                        node_list[n] = {}
                    node_list[n][s] = "-" + graph[s][n]["label"]
        if dep > 100:
            break
    return node_list
