from sqlalchemy import create_engine
from py2neo import Graph
import sys
import logging

import data_extension.config as cfg


class Connection:
    def __init__(self, sql_username=cfg.sql_name, sql_password=cfg.sql_password, sql_host=cfg.sql_host,
                 sql_dbname=cfg.sql_dbname, neo_username=cfg.neo_name, neo_password=cfg.neo_password,
                 neo_host=cfg.neo_host, neo_port=cfg.neo_port):
        self.sql_username = sql_username
        self.sql_password = sql_password
        self.sql_host = sql_host
        self.sql_dbname = sql_dbname
        self.neo_username = neo_username
        self.neo_password = neo_password
        self.neo_host = neo_host
        self.neo_port = neo_port

    def psql_engine(self):
        try:
            conn_string = f"postgresql://{self.sql_username}:{self.sql_password}@{self.sql_host}/{self.sql_dbname}"
            engine = create_engine(conn_string)
            logging.info("Connected to POSTGRES!\n")
            return engine
        except:
            logging.error('Unable to establish connection with POSTGRES\n')
            logging.error(sys.exc_info())

    def graph_engine(self):
        try:
            engine = Graph(auth=(self.neo_username, self.neo_password), host=self.neo_host, port=self.neo_port, scheme="bolt")
            logging.info("Connected to NEO4J!\n")
            return engine
        except:
            logging.error('Unable to establish connection with NEO4J\n')
            logging.error(sys.exc_info())
