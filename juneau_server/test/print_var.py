import sys
import logging
from jupyter_client import find_connection_file
from jupyter_client import MultiKernelManager, BlockingKernelClient, KernelClient
from traitlets import CUnicode

logging.basicConfig(level=logging.INFO)

# from jupyter_client import kernelspec

TIMEOUT = 60


def main(kid, var):
    # load connection info and init communication
    cf = find_connection_file(kid)  # str(port))
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    code = "import json\nimport pandas as pd\nimport numpy as np\nif type(" + var + ") " \
                                                                       "is pd.DataFrame:\n"
    code = code + "\tprint(" + var + ".to_json(orient=\"split\"))\n"
    code = code + "elif type(" + var + ") is np.ndarray:\n"
    code = code + "\tprint(json.dumps(" + var + ".tolist()))\n"
    code = code + "elif type(" + var + ") is list:\n"
    code = code + "\tprint(json.dumps(" + var + "))\n"
    code += "elif type(" + var + ") is dict:\n"
    code += "\tprint(json.dumps(" + var + "))\n"
    km.execute_interactive(code, timeout=TIMEOUT)
    km.stop_channels()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
