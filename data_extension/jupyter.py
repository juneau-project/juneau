import subprocess
import sys
import site
import logging

#logging.basicConfig(level=logging.DEBUG)

import sys

from jupyter_client import find_connection_file
from jupyter_client import MultiKernelManager, BlockingKernelClient, KernelClient
TIMEOUT=6

from data_extension.file_lock import FileLock
from queue import Empty
jupyter_lock = FileLock('my.lock')

import data_extension.config as cfg

def request_var(kid, var):
    """
    Request the contents of a dataframe or matrix

    :param kid:
    :param var:
    :return: Tuple with first parameter being JSON form of output, 2nd parameter being error if
             1st is None
    """
    code = "import pandas as pd\nimport numpy as np\nif type(" + var + ") " \
                                                                       "is pd.DataFrame or type(" + var + ") is np.ndarray or type(" + var + ") is list:\n"
    code = code + "\tprint(" + var + ".to_json(orient='split', index = False))\n"
    return exec_code(kid, var, code)

def connect_psql(kid, var):
    """
    Create a juneau_connect() function for use in the notebook

    :param kid:
    :param var:
    :return:
    """
    code = 'from sqlalchemy import create_engine\n' + \
        'user_name = \'' + cfg.sql_name + '\'\n' + \
        'password = \'' + cfg.sql_password + '\'\n' + \
        'dbname = \'' + cfg.sql_dbname + '\'\n' + \
        'def juneau_connect():\n\tengine = create_engine(\'postgresql://\' + user_name + \':\' + password + \'@localhost/\' + dbname,connect_args={\'options\': \'-csearch_path={}\'.format("' + cfg.sql_dbs + '")})\n\treturn engine.connect()';
    return exec_code(kid, var, code)

def exec_code(kid, var, code):
    # load connection info and init communication
    cf = find_connection_file(kid)  # str(port))

    global jupyter_lock

    jupyter_lock.acquire()
    try:
        km = BlockingKernelClient(connection_file=cf)
        km.load_connection_file()
        km.start_channels()

        # logging.debug('Executing:\n' + str(code))
        msg_id = km.execute(code, store_history=False)

        reply = km.get_shell_msg(msg_id,timeout=60)
        #logging.info('Execution reply:\n' + str(reply))
        state = 'busy'

        output = None
        idle_count = 0
        try:
            while km.is_alive():
                try:
                    msg = km.get_iopub_msg(timeout=10)
                    #logging.debug('Read ' + str(msg))
                    if not 'content' in msg:
                        continue
                    if 'name' in msg['content'] and msg['content']['name'] == 'stdout':
                        #logging.debug('Got data '+ msg['content']['text'])
                        output = msg['content']['text']
                        break
                    if 'execution_state' in msg['content']:
                        #logging.debug('Got state')
                        state = msg['content']['execution_state']
                    if state == 'idle':
                        idle_count = idle_count + 1
                except Empty:
                    pass
        except KeyboardInterrupt:
            logging.error('Keyboard interrupt')
            pass
        finally:
            #logging.info('Kernel IO finished')
            km.stop_channels()

        # logging.info(str(output))
        error = ''
        if reply['content']['status'] != 'ok':
            logging.error('Status is ' + reply['content']['status'])
            logging.error(str(output))
            error = output
            output = None
    finally:
        jupyter_lock.release()

    return output, error

# Execute via IPython kernel
def exec_ipython(kernel_id, search_var, py_file):
    global jupyter_lock

    jupyter_lock.acquire()
    try:
        logging.debug('Exec ' + py_file)
        file_name = site.getsitepackages()[0] + '/data_extension/' + py_file + '.py'
        try:
            if sys.version_info[0] >= 3:
                msg_id = subprocess.Popen(['python3', file_name, \
                                           kernel_id, search_var], \
                                          stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            else:
                msg_id = subprocess.Popen(['python', file_name, \
                                           kernel_id, search_var], \
                                          stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        except FileNotFoundError:
            msg_id = subprocess.Popen(['python', file_name, \
                                       kernel_id, search_var], \
                                      stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        output, error = msg_id.communicate()
    finally:
        jupyter_lock.release()

    if sys.version[0] == '3':
        output = output.decode("utf-8")
        error = error.decode("utf-8")
    output = output.strip('\n')

    msg_id.stdout.close()
    msg_id.stderr.close()

    logging.debug(output)

    return output, error
