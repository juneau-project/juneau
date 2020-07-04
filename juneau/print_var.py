import logging
import sys

logging.basicConfig(level=logging.INFO)


#from jupyter_client import kernelspec

from jupyter_client import find_connection_file
from jupyter_client import BlockingKernelClient

TIMEOUT=60


def main(kid, var):

    # load connection info and init communication
    cf = find_connection_file(kid)  # str(port))
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    code = "import pandas as pd\nimport numpy as np\nif type(" + var + ") " \
                                                                       "is pd.DataFrame or type(" + var + ") is np.ndarray or type(" + var + ") is list:\n"
    code = code + "\tprint(" + var + ".to_json(orient='split', index = False))\n"
    km.execute_interactive(code, timeout = TIMEOUT)
    km.stop_channels()

if __name__ == "__main__":
   main(sys.argv[1], sys.argv[2])