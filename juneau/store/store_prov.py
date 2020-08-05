# Copyright 2020 Juneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
TODO: Explain what this module does.
"""

import ast
import json
import logging

import networkx as nx
import pandas as pd
import psycopg2

from juneau.config import config
from juneau.utils.funclister import FuncLister

special_type = ["np", "pd"]


class LineageStorage:
    def __init__(self, psql_eng):
        self.eng = psql_eng
        self.__connect2db()
        self.variable = []
        self.view_cmd = {}
        self.l2d_cmd = {}

    @staticmethod
    def __connect2db():
        conn_string = (
            f"host='{config.sql.host}' dbname='{config.sql.dbname}' "
            f"user='{config.sql.name}' password='{config.sql.password}'"
        )

        try:
            # conn.cursor will return a cursor object, you
            # can use this cursor to perform queries.
            conn = psycopg2.connect(conn_string)
            logging.info("Connection to database successful")
            cursor = conn.cursor()

            def build_table(name):
                return (
                    f"CREATE TABLE IF NOT EXISTS {config.sql.graph}.{name} "
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

                if not code or code[0] in ["%", "#", " ", "\n"]:
                    continue

                try:
                    ast.parse(code)
                    if not fflg:
                        new_codes.append(code)
                        line2cid[lid] = cid
                        lid = lid + 1
                except Exception as e:
                    logging.info(
                        f"Parsing error in code fragment {code} due to error {e}"
                    )

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
                left_node.append(f"var_{ele}_{i}_{nb_name}")

            for ele in left:
                if type(ele) is tuple:
                    ele = ele[0]

                new_node = f"var_{ele}_{i}_{nb_name}"
                G.add_node(new_node, cell_id=line2cid[i], line_id=i, var=ele)

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

                    if dep in special_type:
                        ename = dep + "." + ename
                        G.add_edge(new_node, candidate_node, label=ename)
                    else:
                        G.add_edge(new_node, candidate_node, label=ename)

        return G, line2cid

    def insert_table_model(self, store_name, var_name, code_list):
        logging.info("Updating provenance...")
        with self.eng.connect() as conn:
            try:
                dep_db = pd.read_sql_table("dependen", conn, schema=config.sql.graph)
                var_list = dep_db["view_id"].tolist()
            except Exception as e:
                logging.error(f"Reading prov from database failed due to error {e}")

        try:
            dep, c2i, all_code = self.__parse_code(code_list)
            lid = self.__last_line_var(var_name, all_code)
        except Exception as e:
            logging.error(f"Parse code failed due to error {e}")
            return

        try:
            dep_str = json.dumps(dep)
            l2c_str = json.dumps(c2i)
            lid_str = json.dumps(lid)

            logging.info("JSON created")

            self.view_cmd[store_name] = dep_str
            self.l2d_cmd[store_name] = l2c_str

            encode1 = dep_str
            encode2 = l2c_str

            if store_name not in var_list and store_name not in self.variable:
                logging.debug("Inserting values into dependen and line2cid")

                def insert_value(table_name, encode):
                    conn.execute(
                        f"INSERT INTO {config.sql.graph}.{table_name} VALUES ('{store_name}', '{encode}')"
                    )

                with self.eng.connect() as conn:
                    try:
                        for table, encoded in [
                            ("dependen", encode1),
                            ("line2cid", encode2),
                            ("lastliid", lid_str),
                        ]:
                            insert_value(table, encoded)
                        self.variable.append(store_name)
                    except Exception as e:
                        logging.error(f"Unable to insert into tables due to error {e}")
        except Exception as e:
            logging.error(f"Unable to update provenance due to error {e}")

    def close_dbconnection(self):
        self.eng.close()
