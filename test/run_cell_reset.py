import sys
import timeit

from jupyter_client import find_connection_file
from jupyter_client import MultiKernelManager, BlockingKernelClient, KernelClient

TIMEOUT=60

def main(kid, code):

    # load connection info and init communication
    cf = find_connection_file(kid)  # str(port))
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    km.execute('%reset -f')
    #km.execute('import pickle')
    start_time = timeit.default_timer()
    msg_id = km.execute_interactive(str(code), timeout=TIMEOUT)

    end_time = timeit.default_timer()


    km.stop_channels()

    print('time:' + str(end_time - start_time))

    return

if __name__ == "__main__":
   main(sys.argv[1], sys.argv[2])