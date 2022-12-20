import networkx as nx
import json
import data_extension.config as cfg
import psycopg2
import pandas as pd
import base64
from data_extension.util import last_line_var, parse_code
#from data_extension.table_db import parse_code

import sys

import logging

class Store_Lineage:

    conn = None

    def __connect2db_init(self):
        # get a connection, if a connect cannot be made an exception will be raised here
        if not self.conn:
            # Define our connection string
            conn_string = "host=\'" + cfg.sql_host + "\' dbname=\'" + cfg.sql_dbname + \
                          "\' user=\'" + cfg.sql_name + "\' password=\'" + cfg.sql_password + "\'"

            # print the connection string we will use to connect
            # logging.info("Connecting to database\n	->%s" % (conn_string))

            try:
                # conn.cursor will return a cursor object, you can use this cursor to perform queries
                conn = psycopg2.connect(conn_string)
                logging.info("Connecting Database Succeeded!\n")
                cursor = conn.cursor()
                #query1 = "DROP SCHEMA IF EXISTS graph_model CASCADE;"
                #query2 = "CREATE SCHEMA graph_model;"
                query3 = "CREATE TABLE IF NOT EXISTS " + cfg.sql_graph + ".dependen (view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"
                query4 = "CREATE TABLE IF NOT EXISTS " + cfg.sql_graph + ".line2cid (view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"
                query5 = "CREATE TABLE IF NOT EXISTS " + cfg.sql_graph + ".lastliid (view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"
                query6 = "CREATE TABLE IF NOT EXISTS " + cfg.sql_graph + ".var_code (view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"

                #try:
                #    cursor.execute(query1)
                #    conn.commit()

    #            except:
    #                print("Drop Schema Failed!\n")
                try:
    #                cursor.execute(query2)
                    cursor.execute(query3)
                    cursor.execute(query4)
                    cursor.execute(query5)
                    cursor.execute(query6)
                    conn.commit()

                except:
                    logging.error("Create Tables Failed!\n")

                cursor.close()
                conn.close()
                return True

            except:
                logging.error("Connecting Database Failed!\n")
                logging.info(sys.exc_info())
                return False
        else:
            return True

    def __init__(self, psql_eng):

        self.__connect2db_init()
        self.eng = psql_eng
        self.Variable = []
        self.view_cmd = {}
        self.l2d_cmd = {}
        self.special_type = ['np', 'pd']

    def generate_graph_from_sql(self, dependency, line2cid, nb_name):
        G = nx.DiGraph()
        for i in dependency.keys():
            left = dependency[i][0]
            right = list(set([tuple(x) for x in dependency[i][1]]))

            left_node = []
            for ele in left:

                if type(ele) is tuple or (type(ele) is list and len(ele) >= 2):
                    ele = ele[0]
                left_node.append('var_' + ele + '_' + str(i) + '_' + str(nb_name))
            # print('left',left_node)

            for ele in left:
                if type(ele) is tuple or (type(ele) is list and len(ele) >= 2):
                    ele = ele[0]

                new_node = 'var_' + ele + '_' + str(i) + '_' + str(nb_name)
                G.add_node(new_node, cell_id=line2cid[i], line_id=i, var=ele)

                # print(nbname)
                # print(right)
                for dep, ename in right:
                    candidate_list = G.nodes
                    rankbyline = []
                    for cand in candidate_list:
                        # print('cand', cand)
                        if G.nodes[cand]['var'] == dep:
                            if cand in left_node:
                                # print(cand)
                                continue
                            rankbyline.append((cand, G.nodes[cand]['line_id']))
                    rankbyline = sorted(rankbyline, key=lambda d: d[1], reverse=True)

                    if len(rankbyline) == 0:
                        if dep not in self.special_type:
                            # TODO: instead of having a line_id = 1, this should be i
                            candidate_node = 'var_' + dep + '_*_' + str(nb_name)
                            G.add_node(candidate_node, cell_id=0, line_id=i, var=dep)
                        else:
                            candidate_node = 'sep_' + dep + '_' + str(nb_name)
                            G.add_node(candidate_node, cell_id=0, line_id=i, var=dep)

                    else:
                        candidate_node = rankbyline[0][0]

                    if dep in self.special_type:
                        ename = dep + "." + ename
                        G.add_edge(new_node, candidate_node, label=ename)
                    else:
                        G.add_edge(new_node, candidate_node, label=ename)

        return G, line2cid

    def generate_graph(self, code_list, nb_name):

        # self.notebook = nb_name
        # self.nid = nid

        dependency, line2cid, all_code = parse_code(code_list)

        print(dependency)

        G = nx.DiGraph()
        for i in dependency.keys():
            left = dependency[i][0]
            right = list(set(dependency[i][1]))

            left_node = []
            for ele in left:
                if type(ele) is tuple:
                    ele = ele[0]
                left_node.append('var_' + ele + '_' + str(i) + '_' + str(nb_name))
            # print('left',left_node)

            for ele in left:
                if type(ele) is tuple:
                    ele = ele[0]

                new_node = 'var_' + ele + '_' + str(i) + '_' + str(nb_name)
                G.add_node(new_node, cell_id=line2cid[i], line_id=i, var=ele)

                # print(nbname)
                # print(right)
                for dep, ename in right:
                    candidate_list = G.nodes
                    rankbyline = []
                    for cand in candidate_list:
                        # print('cand', cand)
                        if G.nodes[cand]['var'] == dep:
                            if cand in left_node:
                                # print(cand)
                                continue
                            rankbyline.append((cand, G.nodes[cand]['line_id']))
                    rankbyline = sorted(rankbyline, key=lambda d: d[1], reverse=True)

                    if len(rankbyline) == 0:
                        if dep not in self.special_type:
                            # TODO: instead of having a line_id = 1, this should be i
                            candidate_node = 'var_' + dep + '_*_' + str(nb_name)
                            G.add_node(candidate_node, cell_id=0, line_id=i, var=dep)
                        else:
                            candidate_node = 'sep_' + dep + "_" + str(nb_name)
                            G.add_node(candidate_node, cell_id=0, line_id=i, var=dep)

                    else:
                        candidate_node = rankbyline[0][0]

                    if dep in self.special_type:
                        ename = dep + "." + ename
                        G.add_edge(new_node, candidate_node, label=ename)
                    else:
                        G.add_edge(new_node, candidate_node, label=ename)

        return G, line2cid

    def InsertTable_Model(self, store_name, var_name, code_list, nb_name):

        logging.info('Updating provenance...')
        conn = self.eng.connect()
        try:
            dep_db = pd.read_sql_table("dependen", conn, schema = cfg.sql_graph)
            l2c_db = pd.read_sql_table("line2cid", conn, schema = cfg.sql_graph)
            lid_db = pd.read_sql_table("lastliid", conn, schema = cfg.sql_graph)
            cod_db = pd.read_sql_table("var_code", conn, schema = cfg.sql_graph)

            var_list = dep_db['view_id'].tolist()

        except:
            logging.error("reading prov from db failed")
        finally:
            conn.close()

        try:
            dep, c2i, all_code = parse_code(code_list)
            lid = last_line_var(var_name, all_code)
        except Exception as e:
            print(e)
            logging.error("parse code failed")

        try:
            #self.generate_graph(code_list, nb_name)
            #logging.info("Detected Dependency: ", dep)
            #logging.info("Detected var_list: ", var_list)
            dep_str = json.dumps(dep)
            l2c_str = json.dumps(c2i)
            lid_str = json.dumps(lid)
            cod_str = json.dumps(code_list)

            logging.info('JSON created')

            self.view_cmd[store_name] = dep_str
            self.l2d_cmd[store_name] = l2c_str

            encode1 = dep_str #base64.b64encode(dep)
            encode2 = l2c_str #base64.b64encode(c2i)
            encode3 = lid_str
            encode4 = str(base64.b64encode(bytes(cod_str, 'utf-8')), 'utf-8')

            if store_name not in var_list and store_name not in self.Variable:

                logging.debug('Inserting values into dependen and line2cid')
                conn = self.eng.connect()
                try:
                    conn.execute("INSERT INTO " + cfg.sql_graph + ".dependen VALUES (\'" + store_name + "\', \'" + encode1 + "\')")
                    conn.execute("INSERT INTO " + cfg.sql_graph + ".line2cid VALUES (\'" + store_name + "\', \'" + encode2 + "\')")
                    conn.execute("INSERT INTO " + cfg.sql_graph + ".lastliid VALUES (\'" + store_name + "\', \'" + encode3 + "\')")
                    conn.execute("INSERT INTO " + cfg.sql_graph + ".var_code VALUES (\'" + store_name + "\', \'" + encode4 + "\')")
                    self.Variable.append(store_name)
                except:
                    logging.error('Unable to insert into tables')
                finally:
                    conn.close()
        except:
            logging.error('Unable to update provenance due to error ' + str(sys.exc_info()[0]))

    def close_dbconnection(self):
        self.eng.close()
