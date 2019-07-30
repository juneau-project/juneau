import sys
import jupyter_core.paths
import uuid
import time
import tempfile
import logging
logging.basicConfig(level=logging.INFO)


from jupyter_client import kernelspec

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

    code = "print(" + var + ".to_json(orient='split', index = False))"
    km.execute_interactive(code, timeout = TIMEOUT)
    km.stop_channels()

if __name__ == "__main__":
   main(sys.argv[1], sys.argv[2])