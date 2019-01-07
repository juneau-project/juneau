import sys
import jupyter_core.paths
import uuid

from jupyter_client import kernelspec

from jupyter_client import find_connection_file
from jupyter_client import MultiKernelManager, BlockingKernelClient, KernelClient

TIMEOUT=60
from traitlets import CUnicode

def main(kid, tn):

    # load connection info and init communication

    cf = find_connection_file(kid)  # str(port))
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    #km.execute('%reset -f')
    #msg_id = km.execute(code, timeout=TIMEOUT)

    km.execute_interactive("print("+tn+")", timeout = TIMEOUT)
    km.stop_channels()

    return

if __name__ == "__main__":
   main(sys.argv[1], sys.argv[2])