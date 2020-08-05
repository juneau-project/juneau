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
Module to store a table's provenance.
"""

import base64
import logging

import pandas as pd
from py2neo import Node, Relationship, NodeMatcher
from juneau.config import config


class ProvenanceStorage:
    def __init__(self, postgres_eng, graph_eng):
        self.graph_db = graph_eng
        self.postgres_eng = postgres_eng
        self.code_dict = {}

    def _initialize_code_dict(self):
        query1 = "DROP SCHEMA IF EXISTS " + config.sql.provenance + " CASCADE;"
        query2 = "CREATE SCHEMA " + config.sql.provenance + ";"
        query3 = "CREATE TABLE " + config.sql.provenance + ".code_dict (code VARCHAR(1000), cell_id INTEGER);"

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

    def _fetch_code_dict(self):
        with self.postgres_eng.connect() as conn:
            try:
                code_table = pd.read_sql_table(
                    "code_dict", conn, schema=config.sql.provenance
                )
                for index, row in code_table.iterrows():
                    self.code_dict[row["code"]] = int(row["cell_id"])
            except Exception as e:
                logging.error(
                    f"Store Provenance: reading code table failed with error {e}"
                )

    def _store_code_dict(self):
        dict_store = {"code": [], "cell_id": []}
        for i in self.code_dict.keys():
            dict_store["code"].append(i)
            dict_store["cell_id"].append(self.code_dict[i])
        dict_store_code = pd.DataFrame.from_dict(dict_store)
        with self.postgres_eng.connect() as conn:
            dict_store_code.to_sql(
                "code_dict", conn, schema=config.sql.provenance, if_exists="replace", index=False
            )

    def add_cell(self, code, prev_node, var, cell_id, nb_name):
        """
        Stores the Jupyter cell in base64 encoded form, and adds a link to the previously
        stored node (for cell provenance tracking).
        """
        self._fetch_code_dict()
        bcode = base64.b64encode(bytes(code, "utf-8"))
        nbcode = bcode.decode("utf-8")
        matcher = NodeMatcher(self.graph_db)

        if bcode in self.code_dict or nbcode in self.code_dict:
            current_cell = matcher.match("Cell", source_code=bcode).first()
        else:
            if len(list(self.code_dict.values())) != 0:
                max_id = max(list(self.code_dict.values()))
            else:
                max_id = 0
            current_cell = Node("Cell", name=f"cell_{max_id + 1}", source_code=bcode)
            self.graph_db.create(current_cell)
            self.graph_db.push(current_cell)

            if prev_node is not None:
                successor = Relationship(prev_node, "Successor", current_cell)
                self.graph_db.create(successor)
                self.graph_db.push(successor)
                parent = Relationship(current_cell, "Parent", prev_node)
                self.graph_db.create(parent)
                self.graph_db.push(parent)

            self.code_dict[bcode] = max_id + 1
            try:
                self._store_code_dict()
            except Exception as e:
                logging.info(f"Code update for Neo4j failed with error {e}")

        var_name = f"{cell_id}_{var}_{nb_name}"
        current_var = matcher.match("Var", name=var_name).first()

        if current_var is None:
            current_var = Node("Var", name=var_name)

            self.graph_db.create(current_var)
            self.graph_db.push(current_var)

            contains_edge = Relationship(current_cell, "Contains", current_var)
            self.graph_db.create(contains_edge)
            self.graph_db.push(contains_edge)
            contained_by_edge = Relationship(current_var, "Containedby", current_cell)
            self.graph_db.create(contained_by_edge)
            self.graph_db.push(contained_by_edge)

        return current_cell
