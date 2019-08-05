import sys
import logging
import config as cfg
logging.basicConfig(level=logging.INFO)


from jupyter_client import find_connection_file
from jupyter_client import MultiKernelManager, BlockingKernelClient, KernelClient

TIMEOUT=60
from traitlets import CUnicode

def main(kid, var):

    # load connection info and init communication
    cf = find_connection_file(kid)  # str(port))
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    code = 'from sqlalchemy import create_engine\n' + \
        'user_name = \'' + cfg.sql_name + '\'\n' + \
        'password = \'' + cfg.sql_password + '\'\n' + \
        'dbname = \'' + cfg.sql_dbname + '\'\n' + \
        'def juneau_connect():\n\tengine = create_engine(\'postgresql://\' + user_name + \':\' + password + \'@localhost/\' + dbname,connect_args={\'options\': \'-csearch_path={}\'.format("' + cfg.sql_dbs + '")})\n\treturn engine.connect()';

    km.execute_interactive(code, timeout = TIMEOUT)
    km.stop_channels()

if __name__ == "__main__":
   main(sys.argv[1], sys.argv[2])