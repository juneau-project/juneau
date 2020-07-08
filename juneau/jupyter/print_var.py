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

import logging
import sys

from jupyter_client import BlockingKernelClient
from jupyter_client import find_connection_file

TIMEOUT = 60
logging.basicConfig(level=logging.INFO)


def main(kid, var):
    # load connection info and init communication
    cf = find_connection_file(kid)  # str(port))
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    code = (
        "import pandas as pd\nimport numpy as np\nif type(" + var + ") "
        "is pd.DataFrame or type("
        + var
        + ") is np.ndarray or type("
        + var
        + ") is list:\n"
    )
    code = code + "\tprint(" + var + ".to_json(orient='split', index = False))\n"
    km.execute_interactive(code, timeout=TIMEOUT)
    km.stop_channels()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
