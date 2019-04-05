from notebook.utils import url_path_join
from notebook.base.handlers import IPythonHandler
import json
import sys
import subprocess
import pandas as pd
import jupyter_core
from jupyter_client import find_connection_file
from jupyter_client import MultiKernelManager, BlockingKernelClient, KernelClient
from ipython_genutils.path import filefind
from search import WithProv, WithProv_Sk
import site
from search import search_tables
from table_db import dbname
#import ast_test
import os


stdflag = False

types_to_exclude = ['module', 'function', 'builtin_function_or_method',
                    'instance', '_Feature', 'type', 'ufunc']

types_to_include = ['ndarray', 'DataFrame', 'list']

class HelloWorldHandler(IPythonHandler):

    #def __init__(self, application, request, **kwargs):
    #    super().__init__(application, request, **kwargs)
    #    self.search_test_class = WithProv(dbname, 'rowstore')

    def find_variable(self):

        if stdflag == True:
            print("Finding Variables:\n")

        file_name = site.getsitepackages()[0] + '/data_extension/print_var.py'
        msg_id = subprocess.Popen(['python', file_name, self.kernel_id, self.search_var],
                                  stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        output, error = msg_id.communicate()

        if sys.version[0] == '3':
            output = output.decode("utf-8")
            error = error.decode("utf-8")

        if stdflag == True:
            print("*** THE OUTPUT ***")
            print(output)
            print(error)

        msg_id.stdout.close()

        if error != "":
            sta = False
            return (sta, error)
        else:
            sta = True
            var_obj = pd.read_csv("var_dir/" + self.search_var + ".csv")
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
        self.dbinfo['dbnm'] = 'joinstore'#self.settings['postgres_dbnm']
        self.dbinfo['host'] = 'localhost'#self.settings['postgres_url']
        self.dbinfo['user'] = 'yizhang'#self.settings['postgres_user']
        self.dbinfo['pswd'] = 'yizhang'#self.settings['postgres_pswd']
        self.dbinfo['schema'] = 'rowstore'

    def get(self):
        self.data = self.request.arguments
        self.fetch_search_var()
        self.fetch_kernel_id()
        self.fetch_search_mode()
        #self.fetch_dbinfo()

        if self.mode == 0:
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
                print("Variable can not be founded!")
                print(output)
                self.data_trans = {'res':str(""), 'state':str('false')}
                self.write(json.dumps(self.data_trans))

def load_jupyter_server_extension(nb_server_app):
    """
    Called when the extension is loaded.

    Args:
        nb_server_app (NotebookWebApplication): handle to the Notebook webserver instance.
    """
    nb_server_app.log.info("Hello World!")
    global search_test_class
    search_test_class = WithProv(dbname, 'rowstore')
    web_app = nb_server_app.web_app
    host_pattern = '.*$'
    route_pattern = url_path_join(web_app.settings['base_url'], r'/stable')
    web_app.add_handlers(host_pattern, [(route_pattern, HelloWorldHandler)])
    nb_server_app.log.info("Data Extension Loaded!")
