# Copyright 2020 Juneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module that injects code into the kernel to allow printing variables.
"""

import logging
import sys

from jupyter_client import BlockingKernelClient
from jupyter_client import find_connection_file

TIMEOUT = 60
logging.basicConfig(level=logging.INFO)


def main(kid, var):
    # Load connection info and init communications.
    cf = find_connection_file(kid)  # str(port))
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    code = f"""
        import pandas as pd
        import numpy as np
        if type({var}) in [pd.DataFrame, np.ndarray, list]:
            print({var}.to_json(orient='split', index=False))
        """
    km.execute_interactive(code, timeout=TIMEOUT)
    km.stop_channels()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
