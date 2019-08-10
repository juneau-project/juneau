from data_extension.table_db import generate_graph
from data_extension.table_db import parse_code
from data_extension.table_db import pre_vars
from data_extension.table_db import last_line_var
import data_extension.config as cfg

import json
from sys import getsizeof
import os
import pickle

import random
import sys
import timeit
import copy
import queue
import logging
import networkx as nx

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

special_type = ['np', 'pd']

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def _getsizeof(x):
    # return the size of variable x. Amended version of sys.getsizeof
    # which also supports ndarray, Series and DataFrame
    if type(x).__name__ in ['ndarray', 'Series']:
        return x.nbytes
    elif type(x).__name__ == 'DataFrame':
        return x.memory_usage().sum()
    else:
        return getsizeof(x)




class TableSearch:
    query = None
    eng = None
    geng = None

    real_tables = {}

    def __init__(self, dbname, schema = None):
        self.query = None
        self.eng = connect2db(dbname)
        self.geng = connect2gdb()

        self.real_tables = {}




def search_tables(search_test, var_df, mode, code, var_name):

    #if mode == 0:
    #    table = pd.read_sql_table(var_df, search_test.eng, schema = 'rowstore')
    #    return table.to_html()
    #else:
    query_table = var_df #dataframe
    query_name = var_name #name of the table

    if mode == 1:
        logging.info("Search for Additional Training/Validation Tables!")
        tables = search_test.search_additional_training_data(query_table, 10, code, var_name, 0.5, 1)
        #tables = search_test.search_similar_tables_threshold2(query_table, 0.5, 10, 1.5, 0.9, 0.2)
        logging.info("%s Tables are returned!"%len(tables))
        #tables = search_test.search_similar_tables_threshold2(query_table, 10, 0.5, 5, 1, 0.9, 0.2, True, 10)
    elif mode == 2:
        logging.info("Search for Joinable Tables!")
        tables = search_test.search_joinable_tables_threshold2(query_table, 0.1, 10, 1.5, 0.9, 0.2)
        logging.info("%s Joinable Tables are returned!"%len(tables))
    elif mode == 3:
        logging.info("Search for Alternative Feature Tables!")
        tables = search_test.search_alternative_features(query_table, 10, code, var_name, 0.8, 0.1, 1, 0.9, 0.2)
        logging.info("%s Tables are returned!"%len(tables))
        #
        # code = '\n'.join([t for t in code.split('\\n') if len(t)> 0 and t[0]!='%'])
        # code = '\''.join(code.split('\\\''))
        # line_id = last_line_var(var_name, code)
        # dependency = parse_code(code)
        # graph = generate_graph(dependency)
        # query_name = 'var_' + var_name + '_' + str(line_id)
        # query_node = pre_vars(query_name, graph)
        # tables = search_test.search_role_sim_tables(query_node, 10)
        # logging.info("%s Provenance Similar Tables are returned!"%len(tables))



    if len(tables) == 0:
        return ""
    else:
        vardic = [{'varName': v[0], 'varType': type(v[1]).__name__, 'varSize': str(v[1].size), 'varContent': v[1].to_html(index_names = True, justify = 'center', max_rows = 10, max_cols = 5, header = True)} for v in tables] # noqa
        return json.dumps(vardic)
    #return search_test

#print(search_tables())