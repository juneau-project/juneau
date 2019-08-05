from notebook.utils import url_path_join
from notebook.base.handlers import IPythonHandler

import json
import threading
import pandas as pd

from data_extension.search import WithProv_Optimized
from data_extension.search import search_tables
import data_extension.jupyter

import os
import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

import logging
logging.basicConfig(level=logging.DEBUG)


HERE = os.path.dirname(__file__)

import shutil

import data_extension.config as cfg

stdflag = False

types_to_exclude = [ \
    'module',  \
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

global done
done = {}

class JuneauHandler(IPythonHandler):

    def initialize(self):
        logging.info('Juneau handler initializing')
        #self.search_test_class = WithProv(dbname, 'rowstore')

    def find_variable(self):
        logging.info('Looking for ' + self.search_var)
        # Make sure we have an engine connection for each kernel
        if self.kernel_id not in done:
            o2,err = data_extension.jupyter.exec_ipython( \
                self.kernel_id, self.search_var, 'connect_psql')
            done[self.kernel_id] = {}
            logging.info(o2)
            logging.info(err)

        output, error = data_extension.jupyter.exec_ipython( \
            self.kernel_id, self.search_var, 'print_var')

        if error != "" or output == "" or output is None:
            sta = False
            return (sta, error)
        else:
            sta = True
            logging.info('Parsing: ' + output)
            var_obj = pd.read_json(output, orient='split')
            return (sta, var_obj)

    def fetch_kernel_id(self):
        self.kernel_id = str(self.data['kid'][0])[2:-1]

    def fetch_search_var(self):
        self.search_var = str(self.data['var'][0])[2:-1]

    def fetch_search_mode(self):
        self.mode = int(self.data['mode'][0])

    def fetch_code(self):
        self.code = str(self.data['code'][0])[2:-1]

    def fetch_dbinfo(self):
        self.dbinfo = {}
        self.dbinfo['dbnm'] = cfg.sql_dbname#'joinstore'#self.settings['postgres_dbnm']
        self.dbinfo['host'] = cfg.sql_host#'localhost'#self.settings['postgres_url']
        self.dbinfo['user'] = cfg.sql_name#'yizhang'#self.settings['postgres_user']
        self.dbinfo['pswd'] = cfg.sql_password#'yizhang'#self.settings['postgres_pswd']
        self.dbinfo['schema'] = cfg.sql_dbs#'rowstore'

    def put(self):
        logging.info('Juneau indexing request')
        self.data_trans = {'res': "", 'state': str('true')}
        self.write(json.dumps(self.data_trans))

    def get(self):
        if not search_test_class:
            self.data_trans = {'res': {'status': 'The Juneau server is still initializing'}, 'state': str('false')}
            self.write(json.dumps(self.data_trans))
            return

        logging.info('Juneau handling search request')
        self.data = self.request.arguments
        self.fetch_search_var()
        self.fetch_kernel_id()
        self.fetch_search_mode()

        if self.mode == 0: # return table
            if self.search_var in search_test_class.real_tables:
                self.data_trans = {'res': "", 'state': str('true')}
                self.write(json.dumps(self.data_trans))
            else:
                self.data_trans = {'res': "", 'state': str('false')}
                self.write(json.dumps(self.data_trans))
        else:
            self.fetch_code()

            success, output = self.find_variable()

            if success == True:
                data_json = search_tables(search_test_class, output, self.mode, self.code, self.search_var)
                if data_json != "":
                    self.data_trans = {'res': data_json, 'state':str('true')}
                    self.write(json.dumps(self.data_trans))
                else:
                    self.data_trans = {'res': data_json, 'state': str('false')}
                    self.write(json.dumps(self.data_trans))
            else:
                logging.error("The table was not found:")
                logging.error(output)
                self.data_trans = {'res':str(""), 'state':str('false')}
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
    global search_test_class
    search_test_class = None
    #search_test_class = WithProv_Optimized(cfg.sql_dbname, cfg.sql_dbs)
    loader = threading.Thread(target=background_load, args=(nb_server_app,))
    web_app = nb_server_app.web_app
    host_pattern = r'.*$'
    route_pattern = url_path_join(web_app.settings['base_url'], '/juneau')
    web_app.add_handlers(host_pattern, [(route_pattern, JuneauHandler)])
    nb_server_app.log.info("Juneau extension loaded!")
    loader.start()

