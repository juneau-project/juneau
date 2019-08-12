from data_extension.funclister import FuncLister
import ast
import networkx as nx
import json
import data_extension.config as cfg
import psycopg2
import pandas as pd


special_type = ['np', 'pd']

class Store_Lineage:

    def __connect2db(self):
        engine = create_engine("postgresql://" + user_name + ":" + password + "@localhost/" + self.dbname)
        return engine.connect()

    def __connect2db_init(self):
        # Define our connection string
        conn_string = "host=\'" + cfg.sql_host + "\' dbname=\'" + cfg.sql_dbname + "\' user=\'" + cfg.sql_name + "\' password=\'" + cfg.sql_password + "\'"

        # print the connection string we will use to connect
        #print("Connecting to database\n	->%s" % (conn_string))

        # get a connection, if a connect cannot be made an exception will be raised here
        try:
            # conn.cursor will return a cursor object, you can use this cursor to perform queries
            conn = psycopg2.connect(conn_string)
#            print("Connecting Database Succeeded!\n")
            cursor = conn.cursor()
            #query1 = "DROP SCHEMA IF EXISTS graph_model CASCADE;"
            #query2 = "CREATE SCHEMA graph_model;"
            query3 = "CREATE TABLE IF NOT EXISTS graph_model.dependen (view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"
            query4 = "CREATE TABLE IF NOT EXISTS graph_model.line2cid (view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"


            #try:
            #    cursor.execute(query1)
            #    conn.commit()

#            except:
#                print("Drop Schema Failed!\n")
            try:
#                cursor.execute(query2)
                cursor.execute(query3)
                cursor.execute(query4)
                conn.commit()

            except:
                print("Create Tables Failed!\n")

            cursor.close()
            conn.close()
            return True

        except:
            print("Connecting Database Failed!\n")
            return False

    def __init__(self, psql_eng):

        self.eng = psql_eng
        self.__connect2db_init()
        self.Variable = []
        self.view_cmd = {}
        self.l2d_cmd = {}


    def __parse_code(self, code_list):

        test = FuncLister()
        all_code = ""
        line2cid = {}

        lid = 1
        for cid, cell in enumerate(code_list):
            codes = cell.split("\\n")
            for code in codes:
                line2cid[lid] = cid
                lid = lid + 1
                if len(code) == 0:
                    continue
                if code[0] == '%':
                    codes.remove(code)
            all_code = all_code + '\\n'.join(codes) + '\\n'

        tree = ast.parse(all_code)
        test.visit(tree)
        return test.dependency, line2cid

    def generate_graph(self, code_list, nb_name):

        #self.notebook = nb_name
        #self.nid = nid

        dependency, line2cid = self.__parse_code(code_list)
        G = nx.DiGraph()
        for i in dependency.keys():
            left = dependency[i][0]
            right = list(set(dependency[i][1]))

            left_node = []
            for ele in left:
                if type(ele) is tuple:
                    ele = ele[0]
                left_node.append('var_' + ele + '_' + str(i) + '_' + str(nb_name))
        #print('left',left_node)

            for ele in left:
                if type(ele) is tuple:
                    ele = ele[0]

                new_node = 'var_' + ele + '_' + str(i) + '_' + str(nb_name)
                G.add_node(new_node, cell_id = line2cid[i], line_id = i, var = ele)

                #print(nbname)
                #print(right)
                for dep, ename in right:
                    candidate_list = G.nodes
                    rankbyline = []
                    for cand in candidate_list:
                        #print('cand', cand)
                        if G.nodes[cand]['var'] == dep:
                            if cand in left_node:
                                #print(cand)
                                continue
                            rankbyline.append((cand, G.nodes[cand]['line_id']))
                    rankbyline = sorted(rankbyline, key = lambda d:d[1], reverse= True)

                    if len(rankbyline) == 0:
                        if dep not in special_type:
                            candidate_node = 'var_' + dep + '_' + str(1) + '_' + str(nb_name)
                            G.add_node(candidate_node, cell_id = 0, line_id = 1, var=dep)
                        else:
                            candidate_node = dep + str(nb_name)
                            G.add_node(candidate_node, cell_id = 0, line_id = 1, var = dep)

                    else:
                        candidate_node = rankbyline[0][0]

                #print(new_node, candidate_node)
                    if dep in special_type:
                        ename = dep + "." + ename
                        G.add_edge(new_node, candidate_node, label = ename)
                    else:
                        G.add_edge(new_node, candidate_node, label=ename)

        return G, line2cid

    def InsertTable_Model(self, var_name, code_list, nb_name):

        dep_db = pd.read_sql_table("dependen", self.eng, schema = cfg.sql_graph)
        l2c_db = pd.read_sql_table("line2cid", self.eng, schema = cfg.sql_graph)
        var_list = dep_db['view_id'].tolist()


        dep, c2i = self.__parse_code(code_list)

        #self.generate_graph(code_list, nb_name)
        dep_str = json.dumps(dep)
        l2c_str = json.dumps(c2i)

        self.Variable.append(var_name)
        self.view_cmd[var_name] = dep_str
        self.l2d_cmd[var_name] = l2c_str

        encode1 = dep_str #base64.b64encode(dep)
        encode2 = l2c_str #base64.b64encode(c2i)
        if var_name not in var_list:
            self.eng.execute("INSERT INTO " + cfg.sql_graph + ".dependen VALUES (\'" + var_name + "\', \'" + encode1 + "\')")
            self.eng.execute("INSERT INTO " + cfg.sql_graph + ".line2cid VALUES (\'" + var_name + "\', \'" + encode2 + "\')")

    def close_dbconnection(self):
        self.eng.close()





