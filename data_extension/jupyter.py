import subprocess
import sys
import site
import logging

logging.basicConfig(level=logging.INFO)

import sys

from jupyter_client import find_connection_file
from jupyter_client import MultiKernelManager, BlockingKernelClient, KernelClient
TIMEOUT=6

from queue import Empty

def request_var(kid, var):

    # load connection info and init communication
    cf = find_connection_file(kid)  # str(port))
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    code = "import pandas as pd\nimport numpy as np\nif type(" + var + ") " \
        "is pd.DataFrame or type(" + var + ") is np.ndarray or type(" + var + ") is list:\n"
    code = code + "\tprint(" + var + ".to_json(orient='split', index = False))\n"
    logging.debug('Executing:\n' + str(code))
    msg_id = km.execute(code, store_history=False)

    reply = km.get_shell_msg(msg_id)
    logging.debug('Execution reply:\n' + str(reply))
    state = 'busy'

    output = None
    try:
        while state != 'idle' and km.is_alive():
            try:
                msg = km.get_iopub_msg(timeout=1)
                logging.debug('Read ' + str(msg))
                if not 'content' in msg:
                    continue
                if 'name' in msg['content'] and msg['content']['name'] == 'stdout':
                    logging.debug('Got data '+ msg['content']['text'])
                    output = msg['content']['text']
                if 'execution_state' in msg['content']:
                    logging.debug('Got state')
                    state = msg['content']['execution_state']
            except Empty:
                pass
    except KeyboardInterrupt:
        logging.error('Keyboard interrupt')
        pass
    finally:
        logging.info('Done reading')
        km.stop_channels()

    logging.debug(str(output))
    error = ''
    if reply['content']['status'] != 'ok':
        error = output
        output = None

    return output, error

# Execute via IPython kernel
def exec_ipython(kernel_id, search_var, py_file):
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

    if sys.version[0] == '3':
        output = output.decode("utf-8")
        error = error.decode("utf-8")
    output = output.strip('\n')

    msg_id.stdout.close()
    msg_id.stderr.close()

    logging.debug(output)

    return output, error
