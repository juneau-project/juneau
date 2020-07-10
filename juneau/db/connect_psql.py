import logging
import sys

from jupyter_client import BlockingKernelClient
from jupyter_client import find_connection_file

from juneau import config

TIMEOUT = 60
logging.basicConfig(level=logging.INFO)


def main(kid):
    # Load connection info and init communications.
    cf = find_connection_file(kid)
    km = BlockingKernelClient(connection_file=cf)
    km.load_connection_file()
    km.start_channels()

    # FIXME: Why are we defining the function if we are not calling it?
    code = f"""
        from sqlalchemy import create_engine
        
        def juneau_connect():
            engine = create_engine(
                "postgresql://{config.sql_name}:{config.sql_password}@localhost/{config.sql_dbname}",
                connect_args={{ 
                    "options": "-csearch_path='{config.sql_dbs}'" 
                }}
            )
            return engine.connect()
        """
    km.execute_interactive(code, timeout=TIMEOUT)
    km.stop_channels()


if __name__ == "__main__":
    main(sys.argv[1])
