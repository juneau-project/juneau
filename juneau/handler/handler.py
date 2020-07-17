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

import juneau.config as cfg
from juneau.db.table_db import connect2db_engine, connect2gdb
from juneau.jupyter import jupyter
from juneau.search.search import search_tables
from juneau.search.search_withprov_opt import WithProv_Optimized
from juneau.store.store_graph import ProvenanceStorage
from juneau.store.store_prov import LineageStorage
from juneau.utils.utils import clean_notebook_name

INDEXED = set()
nb_cell_id_node = {}
search_test_class = WithProv_Optimized(cfg.sql_dbname, cfg.sql_dbs)


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
        self.graph_db = None
        self.psql_engine = None
        self.store_graph_db_class = None
        self.store_prov_db_class = None
        self.prev_node = None

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
        logging.info(f"Stored tables: {INDEXED}")

        cleaned_nb_name = clean_notebook_name(self.nb_name)
        code_list = self.code.strip("\\n#\\n").split("\\n#\\n")
        store_table_name = f"{self.cell_id}_{self.var}_{cleaned_nb_name}"

        if store_table_name in INDEXED:
            logging.info("Request to index is already registered.")
        elif self.var not in code_list[-1]:
            logging.info("Not a variable in the current cell.")
        else:
            logging.info(f"Starting to store {self.var}")
            success, output = self.find_variable()

            if success:
                logging.info(f"Getting value of {self.var}")
                logging.info(output.head())

                if not self.graph_db:
                    self.graph_db = connect2gdb()
                if not self.psql_engine:
                    self.psql_engine = connect2db_engine(cfg.sql_dbname)
                if not self.store_graph_db_class:
                    self.store_graph_db_class = ProvenanceStorage(
                        self.psql_engine, self.graph_db
                    )
                if not self.store_prov_db_class:
                    self.store_prov_db_class = LineageStorage(self.psql_engine)

                if cleaned_nb_name not in nb_cell_id_node:
                    nb_cell_id_node[cleaned_nb_name] = {}

                try:
                    for cid in range(self.cell_id - 1, -1, -1):
                        if cid in nb_cell_id_node[cleaned_nb_name]:
                            self.prev_node = nb_cell_id_node[cleaned_nb_name][cid]
                            break
                    self.prev_node = self.store_graph_db_class.add_cell(
                        self.code,
                        self.prev_node,
                        self.var,
                        self.cell_id,
                        cleaned_nb_name,
                    )
                    if self.cell_id not in nb_cell_id_node[cleaned_nb_name]:
                        nb_cell_id_node[cleaned_nb_name][self.cell_id] = self.prev_node
                except Exception as e:
                    logging.error(f"Unable to store in graph store due to error {e}")

                self.store_table(output, store_table_name)

            else:
                logging.error("find variable failed!")

        self.data_trans = {"res": "", "state": str("true")}
        self.write(json.dumps(self.data_trans))

    def post(self):
        logging.info("Juneau handling search request")
        if self.mode == 0:  # return table
            self.data_trans = {
                "res": "",
                "state": self.var in search_test_class.real_tables,
            }
            self.write(json.dumps(self.data_trans))
        else:
            success, output = self.find_variable()
            if success:
                data_json = search_tables(
                    search_test_class, output, self.mode, self.code, self.var
                )
                self.data_trans = {"res": data_json, "state": data_json != ""}
                self.write(json.dumps(self.data_trans))
            else:
                logging.error(f"The table was not found: {output}")
                self.data_trans = {"error": output, "state": False}
                self.write(json.dumps(self.data_trans))

    def store_table(self, output, store_table_name):
        """
        Asynchronously stores a table into the database.

        Notes:
            This is the refactored version of `fn`.

        """
        logging.info(f"Indexing new table {store_table_name}")
        conn = self.psql_engine.connect()

        try:
            output.to_sql(
                name=f"rtable{store_table_name}",
                con=conn,
                schema=cfg.sql_dbs,
                if_exists="replace",
                index=False,
            )
            logging.info("Base table stored")
            try:
                code_list = self.code.split("\\n#\\n")
                self.store_prov_db_class.insert_table_model(
                    store_table_name, self.var, code_list
                )
                INDEXED.add(store_table_name)
            except Exception as e:
                logging.error(
                    f"Unable to store provenance of {store_table_name} "
                    f"due to error {e}"
                )
            logging.info(f"Returning after indexing {store_table_name}")
        except Exception as e:
            logging.error(f"Unable to store {store_table_name} due to error {e}")
        finally:
            conn.close()
