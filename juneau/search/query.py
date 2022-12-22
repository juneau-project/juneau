import sys
import logging
import pandas as pd
import juneau.config as config
from juneau.utils.utils import last_line_var, parse_code
from juneau.db.table_db import pre_vars, generate_graph

from juneau.utils.utils import clean_notebook_name

class Query:

    def __read_table(self, name):

        conn = self.eng.connect()

        if name[:6] != 'rtable':
            name = 'rtable' + name

        try:
            table_r = pd.read_sql_table(name, conn, schema=config.sql.dbs)
            if 'Unnamed: 0' in table_r.columns:
                table_r.drop(['Unnamed: 0'], axis=1, inplace=True)
            conn.close()
            return table_r

        except KeyboardInterrupt:
            conn.close()
            return None
        except ValueError:
            logging.info("Value error, skipping table " + name)
            conn.close()
            return None
        except TypeError:
            logging.info("Type error, skipping table " + name)
            conn.close()
            return None
        except:
            logging.info("Error, skipping table " + name)
            logging.error("Unexpected error:", sys.exc_info())
            conn.close()
            return None

    def __generate_query_node_from_code(self, var_name, code):

        if ("\\n" in code):
            code = '\n'.join([t for t in code.split('\\n') if len(t)> 0 and t[0] != '%' and t[0] != '#'])
        else:
            code = '\n'.join([t for t in code.split('\n') if len(t) > 0 and t[0] != '%' and t[0] != '#'])

        code = '\''.join(code.split('\\\''))
        code = code.split('\n')
        dependency, _, all_code = parse_code(code)
        line_id = last_line_var(var_name, all_code)
        graph = generate_graph(dependency)


        query_name = 'var_' + var_name + '_' + str(line_id)
        try:
            query_node = pre_vars(query_name, graph)
        except:
            query_node = None
        return query_node

    def __init__(self, db_eng, cell_id, query_code, var_name, nb_name, query_df = None):

        self.eng = db_eng

        self.name = f"{cell_id}_{var_name}_{clean_notebook_name(nb_name)}"

        if query_df is None:
            self.value = self.__read_table(self.name)
        else:
            self.value = query_df

        self.code = query_code
        self.var = var_name
        self.node = self.__generate_query_node_from_code(self.var, self.code)

