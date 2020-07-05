# MIT License
#
# Copyright (c) 2020 Juneau
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
import logging
import sys

import pandas as pd
from notebook.base.handlers import IPythonHandler
from sqlalchemy.exc import NoSuchTableError

import juneau.config as cfg
from juneau.jupyter import jupyter
from juneau.search.search import search_tables
from juneau.search.search_withprov_opt import WithProv_Optimized
from juneau.store.store_graph import Store_Provenance
from juneau.store.store_prov import Store_Lineage
from juneau.db.table_db import connect2db_engine, connect2gdb
from juneau.utils.utils import clean_notebook_name

INDEXED = set()
nb_cell_id_node = {}
search_test_class = WithProv_Optimized(cfg.sql_dbname, cfg.sql_dbs)


class JuneauHandler(IPythonHandler):
    """
    The Juneau Handler that coordinates the notebook server app instance. Essentially,
    this class is in charge of communicating the frontend with the backend.
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

        """

        data = self.request.arguments

        self.var = data['var'][0].decode("utf-8")
        self.kernel_id = data['kid'][0].decode("utf-8")
        self.code = data['code'][0].decode("utf-8")
        self.cell_id = int(data['cell_id'][0].decode("utf-8")) if 'cell_id' in data else None
        self.mode = int(data['mode'][0].decode("utf-8")) if 'mode' in data else None
        self.nb_name = data['nb_name'][0].decode("utf-8") if 'nb_name' in data else None

        self.done = {}
        self.data_trans = {}
        self.graph_db = None
        self.psql_engine = None
        self.store_graph_db_class = None
        self.store_prov_db_class = None
        self.prev_node = None

    def find_variable(self, search_var, kernel_id):
        # Make sure we have an engine connection in case we want to read
        if kernel_id not in self.done:
            o2, err = jupyter.exec_ipython(kernel_id, search_var, 'connect_psql')
            self.done[kernel_id] = {}
            logging.info(o2)
            logging.info(err)

        logging.info('Looking up variable ' + search_var)
        output, error = jupyter.request_var(kernel_id, search_var)
        logging.info('Returned with variable value.')

        if error != "" or output == "" or output is None:
            sta = False
            return sta, error
        else:
            try:
                var_obj = pd.read_json(output, orient='split')
                sta = True
            except:
                logging.info('Parsing: ' + output)
                sta = False
                var_obj = None

        return sta, var_obj

    def put(self):
        global nb_cell_id_node

        logging.info(f'Juneau indexing request: {self.var}')

        cleaned_nb_name = clean_notebook_name(self.nb_name)
        code_list = self.code.strip("\\n#\\n").split("\\n#\\n")
        store_table_name = f'{self.cell_id}_{self.var}_{cleaned_nb_name}'
        logging.info(f'Stored tables: {INDEXED}')
        if store_table_name in INDEXED:
            logging.info('Request to index is already registered.')
        elif self.var not in code_list[-1]:
            logging.info('Not a variable in the current cell.')
        else:
            logging.info("Start to store " + self.var)
            success, output = self.find_variable(self.var, self.kernel_id)

            if success:
                logging.info("Get Value of " + self.var)
                logging.info(output.head())

                if not self.graph_db:
                    self.graph_db = connect2gdb()

                if not self.psql_engine:
                    self.psql_engine = connect2db_engine(cfg.sql_dbname)

                if not self.store_graph_db_class:
                    psql_db = self.psql_engine
                    self.store_graph_db_class = Store_Provenance(psql_db, self.graph_db)

                if not self.store_prov_db_class:
                    psql_db = self.psql_engine
                    self.store_prov_db_class = Store_Lineage(psql_db)

                self.prev_node = None
                if cleaned_nb_name not in nb_cell_id_node:
                    self.prev_node = None
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
                        cleaned_nb_name
                    )
                    if self.cell_id not in nb_cell_id_node[cleaned_nb_name]:
                        nb_cell_id_node[cleaned_nb_name][self.cell_id] = self.prev_node
                except:
                    logging.error('Unable to store in graph store due to error ' + str(sys.exc_info()[0]))

                self.store_table(
                    output,
                    store_table_name,
                    cleaned_nb_name
                )
            else:
                logging.error("find variable failed!")

        self.data_trans = {'res': "", 'state': str('true')}
        self.write(json.dumps(self.data_trans))

    def post(self):
        logging.info('Juneau handling search request')

        if self.mode == 0:  # return table
            if self.var in search_test_class.real_tables:
                self.data_trans = {'res': "", 'state': str('true')}
                self.write(json.dumps(self.data_trans))
            else:
                self.data_trans = {'res': "", 'state': str('false')}
                self.write(json.dumps(self.data_trans))
        else:
            success, output = self.find_variable(self.var, self.kernel_id)

            if success:
                data_json = search_tables(search_test_class, output, self.mode, self.code, self.var)
                if data_json != "":
                    self.data_trans = {'res': data_json, 'state': str('true')}
                    self.write(json.dumps(self.data_trans))
                else:
                    self.data_trans = {'res': data_json, 'state': str('false')}
                    self.write(json.dumps(self.data_trans))

            else:
                logging.error("The table was not found:")
                logging.error(output)
                self.data_trans = {'error': str(output), 'state': str('false')}
                self.write(json.dumps(self.data_trans))

    def store_table(self, output, store_table_name, var_nb_name):
        """
        Asynchronously stores a table into the database.

        Args:
            output:
            store_table_name:
            var_nb_name:

        """
        logging.info("Indexing new table " + store_table_name)
        conn = self.psql_engine.connect()

        try:
            output.to_sql(name='rtable' + store_table_name, con=conn,
                          schema=cfg.sql_dbs, if_exists='replace', index=False)
            logging.info('Base table stored')
            try:
                code_list = self.code.split("\\n#\\n")
                self.store_prov_db_class.InsertTable_Model(store_table_name, self.var, code_list, var_nb_name)
                INDEXED.add(store_table_name)
            except:
                logging.error(
                    'Unable to store provenance of ' + store_table_name + ' due to error' + str(sys.exc_info()[0]))

            logging.info("Returning after indexing " + store_table_name)
        except ValueError:
            logging.error('Unable to store ' + store_table_name + ' due to value error')
        except NoSuchTableError:
            logging.error('Unable to store ' + store_table_name + ' due to no-such-table error')
        except KeyboardInterrupt:
            return
        except:
            logging.error('Unable to store ' + store_table_name + ' due to error ' + str(sys.exc_info()[0]))
            raise
        finally:
            conn.close()
