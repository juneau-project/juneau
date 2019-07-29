import sys
import jupyter_core.paths
import uuid
import time
import tempfile
import logging
logging.basicConfig(level=logging.DEBUG)


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
    logging.debug(code);
    km.execute_interactive(code, timeout = TIMEOUT)
    km.stop_channels()


    #dirpath = tempfile.mkdtemp()

#    code = var + ".to_csv(\'" + dirpath + '/' +var + ".csv\', index = False)"

    #msg_id = km.execute(code, timeout=TIMEOUT)
    #km.execute_interactive("whos", timeout=TIMEOUT)
    #time.sleep(5)
    #km.stop_channels()

    #print(dirpath)
    #print(var)

    #return dirpath


    #km.execute('%reset -f')
    #msg_id = km.execute(code, timeout=TIMEOUT)

    #km.execute_interactive(code, timeout = TIMEOUT)
    #km.execute_interactive("whos", timeout=TIMEOUT)
    #km.stop_channels()

    #return

if __name__ == "__main__":
   main(sys.argv[1], sys.argv[2])