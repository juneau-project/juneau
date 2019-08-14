from notebook.utils import url_path_join
from notebook.base.handlers import IPythonHandler

import json
import threading
import pandas as pd

from data_extension.search_withprov_opt import WithProv_Optimized
from data_extension.search import search_tables
import data_extension.jupyter
from data_extension.table_db import connect2gdb
from data_extension.table_db import connect2db
import data_extension.config as cfg
from data_extension.store_graph import Store_Provenance
from data_extension.store_prov import Store_Lineage

#from multiprocessing import Pool, TimeoutError
import concurrent.futures

import os
import sys

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

import logging

logging.basicConfig(level=logging.DEBUG)

HERE = os.path.dirname(__file__)

import data_extension.config as cfg

stdflag = False
pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)#Pool(processes=2)
indexed = {}

types_to_exclude = [ \
    'module', \
    'function', \
    'builtin_function_or_method', \
    'instance', \
    '_Feature', \
    'type', \
    'ufunc']

types_to_include = [ \
    'ndarray', \
    'DataFrame', \
    'list']

done = {}
search_test_class = None


# Asynchronous storage of tables
def fn(output, store_table_name, var_code, var_nb_name, psql_db, store_prov_db_class):
    logging.info("Synchronization process starting storing table " + store_table_name)
    try:
        output.to_sql(name='rtable' + store_table_name, con=psql_db, \
                          schema=cfg.sql_dbs, if_exists='replace', index=False)
        logging.info('Base table stored')
    except ValueError:
        logging.error('Unable to store table ' + store_table_name + ' due to value error')

    code_list = var_code.split("\\n#\\n")

    try:
        logging.info(var_nb_name)
        logging.info(code_list)
        store_prov_db_class.InsertTable_Model(store_table_name, code_list, var_nb_name)
    except:
        logging.error('Unable to store table ' + store_table_name + ' provenance due to error')
    logging.info("Synchronization process exiting after storing table " + store_table_name)


class JuneauHandler(IPythonHandler):
    kernel_id = None
    search_var = None
    code = None
    mode = None
    dbinfo = {}
    data = {}
    data_trans = {}
    graph_db = None
    psql_db = None
    store_graph_db_class = None
    prev_node = None
    prev_nb = ""
    data_to_store = {}

    def initialize(self):
        logging.info('Calling Juneau handler...')
        self.graph_db = connect2gdb()
        self.psql_db = connect2db(cfg.sql_dbname)
        self.store_graph_db_class = Store_Provenance(self.psql_db, self.graph_db)
        self.store_prov_db_class = Store_Lineage(self.psql_db)
        # self.search_test_class = WithProv(dbname, 'rowstore')

    def find_variable(self, search_var, kernel_id):
        logging.info('Looking up variable ' + search_var)

        # Make sure we have an engine connection for each kernel
        global done
        if kernel_id not in done:
            o2, err = data_extension.jupyter.exec_ipython( \
                kernel_id, search_var, 'connect_psql')
            done[kernel_id] = {}
            logging.info(o2)
            logging.info(err)

        output, error = data_extension.jupyter.request_var(kernel_id, search_var)
        logging.info('Returned with variable: ' + str(output))

        if error != "" or output == "" or output is None:
            sta = False
            return sta, error
        else:
            sta = True
            # logging.info('Parsing: ' + output)
            var_obj = pd.read_json(output, orient='split')
            return sta, var_obj

    def fetch_kernel_id(self):
        self.kernel_id = str(self.data['kid'][0])[2:-1]

    def fetch_search_var(self):
        self.search_var = str(self.data['var'][0])[2:-1]

    def fetch_search_mode(self):
        self.mode = int(self.data['mode'][0])

    def fetch_code(self):
        self.code = str(self.data['code'][0])[2:-1]

    def fetch_dbinfo(self):
        self.dbinfo = {'dbnm': cfg.sql_dbname, \
                       'host': cfg.sql_host, \
                       'user': cfg.sql_name, \
                       'pswd': cfg.sql_password, \
                       'schema': cfg.sql_dbs}

    def put(self):
        global pool
        global indexed
        global fn
        self.data_to_store = self.request.arguments

        if len(self.data_to_store) == 0:
            self.data_trans = {'res': "", 'error': "Invalid request", 'state': str('false')}
            self.write(json.dumps(self.data_trans))
            return

        var_to_store = str(self.data_to_store['var'][0])[2:-1]
        logging.info('Juneau indexing request: ' + var_to_store)

        if var_to_store in indexed:
            logging.info('Request to index is already registered')
        else:
            indexed[var_to_store] = True

            var_code = str(self.data_to_store['code'][0])[2:-1]
            var_nb_name = str(self.data_to_store['nb_name'][0])[2:-1]
            var_cell_id = int(str(self.data_to_store['cell_id'][0])[2:-1])
            kernel_id = str(self.data_to_store['kid'][0])[2:-1]

            success, output = self.find_variable(var_to_store, kernel_id)
            if success:
                if self.prev_nb != var_nb_name:
                    self.prev_node = None

                try:
                    self.prev_node = self.store_graph_db_class.add_cell(var_code, \
                                                                        self.prev_node, \
                                                                        var_to_store, \
                                                                        var_cell_id, var_nb_name)
                except:
                    logging.error('Unable to store in graph store')
                self.prev_nb = var_nb_name

                store_table_name = str(var_cell_id) + "_" + var_to_store + "_" + str(var_nb_name)

                #fn(output, store_table_name, var_code, var_nb_name, self.psql_db, self.store_prov_db_class)
                res = pool.submit(fn, output, store_table_name, var_code, var_nb_name, \
                                            self.psql_db, self.store_prov_db_class)
                #
                # res.result()

        self.data_trans = {'res': "", 'state': str('true')}
        self.write(json.dumps(self.data_trans))

    def post(self):

        # Check if we are initialized yet
        global search_test_class
        if not search_test_class:
            self.data_trans = {'error': 'The Juneau server is still initializing', \
                               'state': str('false')}
            self.write(json.dumps(self.data_trans))
            return

        logging.info('Juneau handling search request')
        self.data = self.request.arguments
        self.fetch_search_var()
        self.fetch_kernel_id()
        self.fetch_search_mode()

        if self.mode == 0:  # return table
            if self.search_var in search_test_class.real_tables:
                self.data_trans = {'res': "", 'state': str('true')}
                self.write(json.dumps(self.data_trans))
            else:
                self.data_trans = {'res': "", 'state': str('false')}
                self.write(json.dumps(self.data_trans))
        else:
            self.fetch_code()

            success, output = self.find_variable(self.search_var, self.kernel_id)

            if success:
                data_json = search_tables(search_test_class, output, self.mode, self.code, self.search_var)
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


def background_load(nb_server_app):
    global search_test_class
    search_test_class = WithProv_Optimized(cfg.sql_dbname, cfg.sql_dbs)
    nb_server_app.log.info("Juneau tables indexed...")


def load_jupyter_server_extension(nb_server_app):
    """
    Called when the extension is loaded.

    Args:
        nb_server_app (NotebookWebApplication): handle to the Notebook webserver instance.
    """
    nb_server_app.log.info("Juneau extension loading...")
    # search_test_class = WithProv_Optimized(cfg.sql_dbname, cfg.sql_dbs)
    loader = threading.Thread(target=background_load, args=(nb_server_app,))
    web_app = nb_server_app.web_app
    host_pattern = r'.*$'
    route_pattern = url_path_join(web_app.settings['base_url'], '/juneau')
    web_app.add_handlers(host_pattern, [(route_pattern, JuneauHandler)])
    nb_server_app.log.info("Juneau extension loaded!")
    loader.start()
