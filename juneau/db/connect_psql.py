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
A module that allows the user to connect to Postgresql.
"""

import logging
import sys

from jupyter_client import BlockingKernelClient
from jupyter_client import find_connection_file

from juneau.config import config

TIMEOUT = 60
logging.basicConfig(level=logging.INFO)


def main(kid):
    # Load connection info and init communications.
    cf = find_connection_file(kid)
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    # Define a function that is useful from within the user's notebook: juneau_connect() can be
    # used to directly connect the notebook to the source database.  Note that this includes the
    # full "root" credentials.

    # FIXME: allow for user-specific credentials on SQL tables.  The DBMS may also not be at localhost.
    code = f"""
        from sqlalchemy import create_engine
        
        def juneau_connect():
            engine = create_engine(
                "postgresql://{config.sql.name}:{config.sql.password}@{config.sql.host}/{config.sql.dbname}",
                connect_args={{ 
                    "options": "-csearch_path='{config.sql.dbs}'" 
                }}
            )
            return engine.connect()
        """
    km.execute_interactive(code, timeout=TIMEOUT)
    km.stop_channels()


if __name__ == "__main__":
    main(sys.argv[1])
