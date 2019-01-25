from notebook.utils import url_path_join
from notebook.base.handlers import IPythonHandler
import json
import sys
import subprocess
import pandas as pd
import ast
import jupyter_core
from jupyter_client import find_connection_file
from jupyter_client import MultiKernelManager, BlockingKernelClient, KernelClient
from ipython_genutils.path import filefind
import site
from search import search_tables



class HelloWorldHandler(IPythonHandler):

    def print_variable_value(self):

        kernel_id = self.kernel_id[2:-1]
        file_name = site.getsitepackages()[0] + '/data_extension/print_var.py'

        msg_id = subprocess.Popen(['python', file_name, kernel_id, self.search_var],
                                  stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        output, error = msg_id.communicate()

        if sys.version[0] == '3':
            output = output.decode('utf-8')
            error = error.decode('utf-8')

        msg_id.stdout.close()

        if error != "":
            sta = False
            return (sta, error)
        else:
            sta = True
            return (sta, output)

    def fetch_kernel_id(self):
        self.kernel_id = str(self.data['kid'][0])

    def fetch_search_var(self):
        self.search_var = str(self.data['var'][0])[2:-1]

    def fetch_search_mode(self):
        self.mode = int(self.data['mode'][0])


    def get(self):
        self.data = self.request.arguments
        self.fetch_search_var()
        self.fetch_kernel_id()
        self.fetch_search_mode()
        print(self.search_var)
        print(self.mode)

        if self.mode == 0:
            output = search_tables(self.search_var, self.mode)
            self.data_trans = {'res': output, 'state': str('true')}
            self.write(json.dumps(self.data_trans))
        else:
            success, output = self.print_variable_value()
            if success == True:
                data_json = search_tables(self.search_var, self.mode)
                self.data_trans = {'res': data_json, 'state':str('true')}
                self.write(json.dumps(self.data_trans))
            else:
                self.data_trans = {'res':str(""), 'state':str('false')}
                self.write(json.dumps(self.data_trans))

def load_jupyter_server_extension(nb_server_app):
    """
    Called when the extension is loaded.

    Args:
        nb_server_app (NotebookWebApplication): handle to the Notebook webserver instance.
    """
    nb_server_app.log.info("Hello World!")
    web_app = nb_server_app.web_app
    host_pattern = '.*$'
    route_pattern = url_path_join(web_app.settings['base_url'], r'/stable')
    web_app.add_handlers(host_pattern, [(route_pattern, HelloWorldHandler)])
    nb_server_app.log.info("Data Extension Loaded!")
