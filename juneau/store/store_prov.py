import ast
import json
import logging
import sys

import networkx as nx
import pandas as pd
import psycopg2

from juneau import config
from juneau.utils.funclister import FuncLister

special_type = ["np", "pd"]


class LineageStorage:
    conn = None

    @staticmethod
    def __connect2db():
        conn_string = (
            f"host='{config.sql_host}' dbname='{config.sql_dbname}' "
            f"user='{config.sql_name}' password='{config.sql_password}'"
        )

        try:
            # conn.cursor will return a cursor object, you can use this cursor to perform queries
            conn = psycopg2.connect(conn_string)
            logging.info("Connection to database successful")
            cursor = conn.cursor()

            def build_table(name):
                return (
                    f"CREATE TABLE IF NOT EXISTS {config.sql_graph}.{name} "
                    f"(view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"
                )

            tables = ["dependen", "line2cid", "lastliid"]
            try:
                for table in tables:
                    cursor.execute(build_table(table))
                    conn.commit()
            except Exception as e:
                logging.error(f"Creation of tables failed due to error {e}")

            cursor.close()
            conn.close()
        except Exception as e:
            logging.error(f"Connection to database failed due to error {e}")

    def __init__(self, psql_eng):

        self.eng = psql_eng
        self.__connect2db()
        self.Variable = []
        self.view_cmd = {}
        self.l2d_cmd = {}

    @staticmethod
    def __last_line_var(varname, code):
        ret = 0
        code = code.split("\n")
        for id, i in enumerate(code):
            if "=" not in i:
                continue
            j = i.split("=")
            j = [t.strip(" ") for t in j]

            if varname in j[0]:
                if varname == j[0][-len(varname) :]:
                    ret = id + 1
        return ret

    @staticmethod
    def __parse_code(code_list):

        test = FuncLister()
        all_code = ""
        line2cid = {}

        lid = 1
        fflg = False
        for cid, cell in enumerate(code_list):

            if "\\n" in cell:
                codes = cell.split("\\n")
            elif "\n" in cell:
                codes = cell.split("\n")
            new_codes = []
            for code in codes:

                if code[:3].lower() == "def":
                    fflg = True
                    continue

                temp_code = code.strip(" ")
                temp_code = temp_code.strip("\t")

                if temp_code[:6].lower() == "return":
                    fflg = False
                    continue

                code = code.strip("\n")
                code = code.strip(" ")
                code = code.split('"')
                code = "'".join(code)
                code = code.split("\\")
                code = "".join(code)

                if len(code) == 0:
                    continue
                if code[0] == "%":
                    continue
                if code[0] == "#":
                    continue
                if code[0] == " ":
                    continue
                if code == "":
                    continue
                if code == "\n":
                    continue

                try:
                    ast.parse(code)
                    if fflg == False:
                        new_codes.append(code)
                        line2cid[lid] = cid
                        lid = lid + 1
                except:
                    logging.info("error with " + code)

            all_code = all_code + "\n".join(new_codes) + "\n"

        all_code = all_code.strip("\n")
        all_code = all_code.split("\n")
        all_code = [t for t in all_code if t != ""]
        all_code = "\n".join(all_code)

        tree = ast.parse(all_code)
        test.visit(tree)
        return test.dependency, line2cid, all_code

    def generate_graph(self, code_list, nb_name):

        dependency, line2cid, all_code = self.__parse_code(code_list)
        G = nx.DiGraph()
        for i in dependency.keys():
            left = dependency[i][0]
            right = list(set(dependency[i][1]))

            left_node = []
            for ele in left:
                if type(ele) is tuple:
                    ele = ele[0]
                left_node.append("var_" + ele + "_" + str(i) + "_" + str(nb_name))

            for ele in left:
                if type(ele) is tuple:
                    ele = ele[0]

                new_node = "var_" + ele + "_" + str(i) + "_" + str(nb_name)
                G.add_node(new_node, cell_id=line2cid[i], line_id=i, var=ele)

                for dep, ename in right:
                    candidate_list = G.nodes
                    rankbyline = []
                    for cand in candidate_list:
                        # print('cand', cand)
                        if G.nodes[cand]["var"] == dep:
                            if cand in left_node:
                                # print(cand)
                                continue
                            rankbyline.append((cand, G.nodes[cand]["line_id"]))
                    rankbyline = sorted(rankbyline, key=lambda d: d[1], reverse=True)

                    if len(rankbyline) == 0:
                        if dep not in special_type:
                            candidate_node = (
                                "var_" + dep + "_" + str(1) + "_" + str(nb_name)
                            )
                            G.add_node(candidate_node, cell_id=0, line_id=1, var=dep)
                        else:
                            candidate_node = dep + str(nb_name)
                            G.add_node(candidate_node, cell_id=0, line_id=1, var=dep)

                    else:
                        candidate_node = rankbyline[0][0]

                    # print(new_node, candidate_node)
                    if dep in special_type:
                        ename = dep + "." + ename
                        G.add_edge(new_node, candidate_node, label=ename)
                    else:
                        G.add_edge(new_node, candidate_node, label=ename)

        return G, line2cid

    def InsertTable_Model(self, store_name, var_name, code_list, nb_name):

        logging.info("Updating provenance...")
        conn = self.eng.connect()
        try:
            dep_db = pd.read_sql_table("dependen", conn, schema=config.sql_graph)
            l2c_db = pd.read_sql_table("line2cid", conn, schema=config.sql_graph)
            lid_db = pd.read_sql_table("lastliid", conn, schema=config.sql_graph)
            var_list = dep_db["view_id"].tolist()

        except:
            logging.error("reading prov from db failed")
        finally:
            conn.close()

        try:
            dep, c2i, all_code = self.__parse_code(code_list)
            lid = self.__last_line_var(var_name, all_code)
        except:
            logging.error("parse code failed")

        try:
            dep_str = json.dumps(dep)
            l2c_str = json.dumps(c2i)
            lid_str = json.dumps(lid)

            logging.info("JSON created")

            self.view_cmd[store_name] = dep_str
            self.l2d_cmd[store_name] = l2c_str

            encode1 = dep_str  # base64.b64encode(dep)
            encode2 = l2c_str  # base64.b64encode(c2i)

            if store_name not in var_list and store_name not in self.Variable:

                logging.debug("Inserting values into dependen and line2cid")
                conn = self.eng.connect()
                try:
                    conn.execute(
                        "INSERT INTO "
                        + config.sql_graph
                        + ".dependen VALUES ('"
                        + store_name
                        + "', '"
                        + encode1
                        + "')"
                    )
                    conn.execute(
                        "INSERT INTO "
                        + config.sql_graph
                        + ".line2cid VALUES ('"
                        + store_name
                        + "', '"
                        + encode2
                        + "')"
                    )
                    conn.execute(
                        "INSERT INTO "
                        + config.sql_graph
                        + ".lastliid VALUES ('"
                        + store_name
                        + "', '"
                        + lid_str
                        + "')"
                    )
                    self.Variable.append(store_name)
                except:
                    logging.error("Unable to insert into tables")
                finally:
                    conn.close()
        except:
            logging.error(
                "Unable to update provenance due to error " + str(sys.exc_info()[0])
            )

    def close_dbconnection(self):
        self.eng.close()
