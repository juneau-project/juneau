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

import json
import logging

import pandas as pd
from notebook.base.handlers import IPythonHandler

from juneau.config import config
from juneau.db.table_db import connect2db_engine, connect2gdb
from juneau.jupyter import jupyter
from juneau.search.search import search_tables
from juneau.store.variable import Var_Info
from juneau.store.store_func import Storage
from juneau.search.query import Query

class JuneauHandler(IPythonHandler):
    """
    The Juneau Handler that coordinates the notebook server app instance. Essentially,
    this class is in charge of communicating the frontend with the backend via PUT and POST.
    """

    def initialize(self):
        """
        Initializes all the metadata related to a table in a Jupyter Notebook.
        Note we use `initialize()` instead of `__init__()` as per Tornado's docs:
        https://www.tornadoweb.org/en/stable/web.html#request-handlers

        The metadata related to the table is:
            - var: the name of the variable that holds the table.
            - kernel_id: the id of the kernel that executed the table.
            - cell_id: the id of the cell that created the table.
            - code: the actual code associated to creating the table.
            - mode: TODO
            - nb_name: the name of the notebook.
            - done: TODO
            - data_trans: TODO
            - graph_db: the Neo4J graph instance.
            - psql_engine: Postgresql engine.
            - store_graph_db_class: TODO
            - store_prov_db_class: TODO
            - prev_node: TODO

        Notes:
            Depending on the type of request (PUT/POST), some of the above will
            be present or not. For instance, the notebook name will be present
            on PUT but not on POST. That is why we check if the key is present in the
            dictionary and otherwise assign it to `None`.

            This function is called on *every* request.

        """
        data = self.request.arguments
        self.var = data["var"][0].decode("utf-8")
        self.kernel_id = data["kid"][0].decode("utf-8")
        self.code = data["code"][0].decode("utf-8")
        self.cell_id = (
            int(data["cell_id"][0].decode("utf-8")) if "cell_id" in data else None
        )
        self.mode = int(data["mode"][0].decode("utf-8")) if "mode" in data else None
        self.nb_name = data["nb_name"][0].decode("utf-8") if "nb_name" in data else None

        self.done = set()
        self.data_trans = {}
        self.store_class = Storage()

    def find_variable(self):
        """
        Finds and tries to return the contents of a variable in the notebook.

        Returns:
            tuple - the status (`True` or `False`), and the variable if `True`.
        """
        # Make sure we have an engine connection in case we want to read.
        if self.kernel_id not in self.done:
            o2, err = jupyter.exec_connection_to_psql(self.kernel_id)
            self.done.add(self.kernel_id)
            logging.info(o2)
            logging.info(err)

        logging.info(f"Looking up variable {self.var}")
        output, error = jupyter.request_var(self.kernel_id, self.var)
        logging.info("Returned with variable value.")

        if error or not output:
            sta = False
            return sta, error
        else:
            try:
                var_obj = pd.read_json(output, orient="split")
                sta = True
            except Exception as e:
                logging.error(f"Found error {e}")
                var_obj = None
                sta = False

        return sta, var_obj

    def put(self):
        logging.info(f"Juneau indexing request: {self.var}")
        logging.info(f"Stored tables: {self.application.indexed}")

        new_var = Var_Info(self.var, self.cell_id, self.nb_name, self.code)

        if new_var.store_name in self.application.indexed:
            logging.info("Request to index is already registered.")
        elif new_var.var_name not in new_var.code_list[-1]:
            logging.info("Not a variable in the current cell.")
        else:
            logging.info(f"Starting to store {new_var.var_name}")
            success, output = self.find_variable()

            if success:
                logging.info(f"Getting value of {new_var.var_name}")
                logging.info(output.head())

                new_var.get_value(output)

                if new_var.nb not in self.application.nb_cell_id_node:
                    self.application.nb_cell_id_node[new_var.nb] = {}
                try:
                    new_var.get_prev_node(self.application.nb_cell_id_node)
                    self.prev_node = self.store_class.store_table(new_var, self.application.schema_mapping_class)
                    if new_var.cid not in self.application.nb_cell_id_node[new_var.nb]:
                        self.application.nb_cell_id_node[new_var.nb][new_var.cid] = self.prev_node
                except Exception as e:
                    logging.error(f"Unable to store in graph store due to error {e}")
            else:
                logging.error("Find variable failed!")

        self.data_trans = {"res": "", "state": str("true")}
        self.write(json.dumps(self.data_trans))

    def post(self):
        logging.info("Juneau handling search request")
        if self.mode == 0:  # return table
            self.data_trans = {
                "res": "",
                "state": self.var in self.application.search_test_class.real_tables,
            }
            self.write(json.dumps(self.data_trans))
        else:
            success, output = self.find_variable()
            if success:
                codes = "\\n".join(self.code.strip("\\n#\\n").split("\\n#\\n"))
                query = Query(self.application.search_test_class.eng, self.cell_id, codes, self.var, self.nb_name, output)
                data_json = search_tables(
                    self.application.search_test_class, self.application.schema_mapping_class, self.mode, query)
                self.data_trans = {"res": data_json, "state": data_json != ""}
                self.write(json.dumps(self.data_trans))
            else:
                logging.error(f"The table was not found: {output}")
                self.data_trans = {"error": output, "state": False}
                self.write(json.dumps(self.data_trans))

