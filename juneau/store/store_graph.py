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

import base64
import logging

import pandas as pd
from py2neo import Node, Relationship, NodeMatcher


class ProvenanceStorage:
    def __init__(self, postgres_eng, graph_eng):
        self.graph_db = graph_eng
        self.postgres_eng = postgres_eng
        self.code_dict = {}

    def __initialize_code_dict(self):

        query1 = "DROP SCHEMA IF EXISTS nb_provenance CASCADE;"
        query2 = "CREATE SCHEMA nb_provenance;"
        query3 = "CREATE TABLE nb_provenance.code_dict (code VARCHAR(1000), cell_id INTEGER);"

        with self.postgres_eng.connect() as conn:
            try:
                conn.execute(query1)
            except Exception as e:
                logging.error(
                    f"Store Provenance: dropping of provenance schema failed with error {e}"
                )

            try:
                conn.execute(query2)
                conn.execute(query3)
            except Exception as e:
                logging.error(
                    f"Store Provenance: creation of provenance schema failed with error {e}"
                )
            finally:
                conn.close()

    def __fetch_code_dict(self):
        conn = self.postgres_eng.connect()
        try:
            code_table = pd.read_sql_table("code_dict", conn, schema="nb_provenance")
            for index, row in code_table.iterrows():
                self.code_dict[row["code"]] = int(row["cell_id"])
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Store Provenance: reading code table failed with error {e}")
            conn.close()
            return False

    def store_code_dict(self):
        dict_store = {"code": [], "cell_id": []}
        for i in self.code_dict.keys():
            dict_store["code"].append(i)
            dict_store["cell_id"].append(self.code_dict[i])
        dict_store_code = pd.DataFrame.from_dict(dict_store)
        conn = self.postgres_eng.connect()
        dict_store_code.to_sql(
            "code_dict", conn, schema="nb_provenance", if_exists="replace"
        )
        conn.close()
        return True

    def add_cell(self, code, prev_node, var, cell_id, nb_name):
        """
        Stores the Jupyter cell in base64 encoded form, and adds a link to the previously
        stored node (for cell provenance tracking)

        :param code:
        :param prev_node:
        :param var:
        :param cell_id:
        :param nb_name:
        :return:
        """
        self.__fetch_code_dict()

        bcode = str(base64.b64encode(bytes(code, "utf-8")))
        matcher = NodeMatcher(self.graph_db)

        # logging.info(bcode)
        nbcode = bcode[2:-1]
        # logging.info(list(self.code_dict.items())[:10])

        if bcode in self.code_dict:
            current_cell = matcher.match("Cell", source_code=bcode).first()
        elif nbcode in self.code_dict:
            current_cell = matcher.match("Cell", source_code=bcode).first()
        else:
            if len(list(self.code_dict.values())) != 0:
                max_id = max(list(self.code_dict.values()))
            else:
                max_id = 0
            current_cell = Node(
                "Cell", name="cell_" + str(max_id + 1), source_code=bcode
            )
            self.graph_db.create(current_cell)
            self.graph_db.push(current_cell)

            if prev_node is not None:
                cell_edge = Relationship(prev_node, "Successor", current_cell)
                self.graph_db.create(cell_edge)
                self.graph_db.push(cell_edge)
                cell_edge2 = Relationship(current_cell, "Parent", prev_node)
                self.graph_db.create(cell_edge2)
                self.graph_db.push(cell_edge2)

            self.code_dict[bcode] = max_id + 1
            try:
                self.store_code_dict()
            except:
                logging.info("update code for neo4j failed!")

        var_name = str(cell_id) + "_" + var + "_" + nb_name

        current_var = matcher.match("Var", name=var_name).first()

        if current_var is None:
            current_var = Node("Var", name=var_name)

            self.graph_db.create(current_var)
            self.graph_db.push(current_var)

            edge = Relationship(current_cell, "Contains", current_var)
            edge2 = Relationship(current_var, "Containedby", current_cell)
            self.graph_db.create(edge)
            self.graph_db.push(edge)
            self.graph_db.create(edge2)
            self.graph_db.push(edge2)

        return current_cell
