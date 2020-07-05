import logging
import sys

from jupyter_client import BlockingKernelClient
from jupyter_client import find_connection_file

from juneau import config

TIMEOUT = 60
logging.basicConfig(level=logging.INFO)


def main(kid, var):
    # load connection info and init communication
    cf = find_connection_file(kid)  # str(port))
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    code = 'from sqlalchemy import create_engine\n' + \
           'user_name = \'' + config.sql_name + '\'\n' + \
           'password = \'' + config.sql_password + '\'\n' + \
           'dbname = \'' + config.sql_dbname + '\'\n' + \
           'def juneau_connect():\n\tengine = create_engine(\'postgresql://\' + user_name + \':\' + password + \'@localhost/\' + dbname,connect_args={\'options\': \'-csearch_path={}\'.format("' + cfg.sql_dbs + '")})\n\treturn engine.connect()';

    km.execute_interactive(code, timeout=TIMEOUT)
    km.stop_channels()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
