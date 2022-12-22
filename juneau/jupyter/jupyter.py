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
This module is in charge of defining functions that will
interact with the Jupyter kernel.
"""

import logging
import os
import subprocess
from threading import Lock

from jupyter_client import BlockingKernelClient
from jupyter_client import find_connection_file

jupyter_lock = Lock()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# FIXME: Why are we printing a variable and calling `.to_json()`? If the type
#        of var is a list, this will throw an error because `.to_json()` is not a list function.
def request_var(kid, var):
    """
    Requests the contents of a dataframe or matrix by executing some code.
    """
    code = f"""
        import pandas as pd
        import numpy as np
        if isinstance({var}, pd.DataFrame) or isinstance({var}, np.ndarray) or isinstance({var}, list):
            print({var}.to_json(orient='split', index=False))
        """
    return exec_code(kid, code)


def exec_code(kid, code):
    """
    Executes arbitrary `code` in the kernel with id `kid`.

    Returns:
        - tuple: the output of the code and the error, if any.
    """
    # Load connection info and init communications.
    cf = find_connection_file(kid)

    with jupyter_lock:
        km = BlockingKernelClient(connection_file=cf)
        km.load_connection_file()
        km.start_channels()
        msg_id = km.execute(code, store_history=False)
        reply = km.get_shell_msg(msg_id, timeout=60)
        output, error = None, None

        while km.is_alive():
            msg = km.get_iopub_msg(timeout=10)
            if (
                "content" in msg
                and "name" in msg["content"]
                and msg["content"]["name"] == "stdout"
            ):
                output = msg["content"]["text"]
                break

        km.stop_channels()
        if reply["content"]["status"] != "ok":
            logging.error(f"Status is {reply['content']['status']}")
            logging.error(output)
            error = output
            output = None

    return output, error


def exec_connection_to_psql(kernel_id):
    """
    Runs the `connect_psql.py` script inside the Jupyter kernel.
    Args:
        kernel_id: The kernel id.

    Returns:
        tuple - the output of the code, and the error if any.
    """
    with jupyter_lock:
        psql_connection = os.path.join(os.path.join(BASE_DIR, "db"), "connect_psql.py")
        msg_id = subprocess.Popen(
            ["python3", psql_connection, kernel_id],
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        output, error = msg_id.communicate()

    output = output.decode("utf-8")
    error = error.decode("utf-8")
    msg_id.stdout.close()
    msg_id.stderr.close()

    return output, error
