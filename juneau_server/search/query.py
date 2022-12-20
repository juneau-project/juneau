import sys
import logging
import ast
import data_extension.config as cfg
from data_extension.util import last_line_var, parse_code
from data_extension.table_db import pre_vars, generate_graph


import pandas as pd


class Query:

    def __read_table(self, name):

        conn = self.eng.connect()

        if name[:6] != 'rtable':
            name = 'rtable' + name

        try:
            table_r = pd.read_sql_table(name, conn, schema=cfg.sql_dbs)
            if 'Unnamed: 0' in table_r.columns:
                table_r.drop(['Unnamed: 0'], axis=1, inplace=True)
            return table_r

        except KeyboardInterrupt:
            return None
        except ValueError:
            logging.info("Value error, skipping table " + name)
            return None
        except TypeError:
            logging.info("Type error, skipping table " + name)
            return None
        except:
            logging.info("Error, skipping table " + name)
            logging.error("Unexpected error:", sys.exc_info())
            return None

    def __generate_query_node_from_code(self, var_name, code):

        if ("\\n" in code):
            code = '\n'.join([t for t in code.split('\\n') if len(t)> 0 and t[0] != '%' and t[0] != '#'])
        else:
            code = '\n'.join([t for t in code.split('\n') if len(t) > 0 and t[0] != '%' and t[0] != '#'])

        code = '\''.join(code.split('\\\''))
        #logging.info("2split code: " + str(code))
        code = code.split('\n')
        #logging.info("3split code: " + str(code))
        dependency, _, all_code = parse_code(code)
        #logging.info('All code ' + str(all_code))
        #logging.info('Dependency ' + str(dependency))
        line_id = last_line_var(var_name, all_code)
        #logging.info(line_id)
        #dependency = parse_code(code)
        graph = generate_graph(dependency)
        #logging.info("Output Graph")
        #logging.info(list(graph.nodes))

        query_name = 'var_' + var_name + '_' + str(line_id)
        try:
            query_node = pre_vars(query_name, graph)
        except:
            query_node = None
        return query_node

    def __init__(self, db_eng, query_name, query_code, var_name, query_df = None):

        self.eng = db_eng

        self.name = query_name

        if query_df is None:
            self.value = self.__read_table(query_name)
        else:
            self.value = query_df

        self.code = query_code
        self.var = var_name
        self.node = self.__generate_query_node_from_code(self.var, self.code)

