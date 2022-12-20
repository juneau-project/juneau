from data_extension.table_db import generate_graph
from data_extension.table_db import parse_code
from data_extension.table_db import pre_vars
from data_extension.util import last_line_var
import data_extension.config as cfg

import json
import os
import pickle

import random
import sys
import timeit
import copy
import queue
import logging
import networkx as nx
from data_extension.search.Query import Query

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

special_type = ['np', 'pd']

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def search_tables(search_test, sm, var_df, mode, code, query_name, var_name):

    query_table = var_df #dataframe
    query_name = 'rtable' + query_name #name of the table
    var_name = var_name

    query = Query(search_test.db_eng, query_name, code, var_name, query_table)

    if mode == 1:
        logging.info("Search for Additional Training/Validation Tables!")
        tables = search_test.search_additional_training_data(sm, query, cfg.return_table_num, alpha=0.5, beta=0.2)
        logging.info("%s Tables are returned!"%len(tables))
    elif mode == 2:
        logging.info("Search for Joinable Tables!")
        tables = search_test.search_joinable_data(sm, query_name, 10, var_name, 0.5 , 1)
        logging.info("%s Joinable Tables are returned!"%len(tables))
    elif mode == 3:
        logging.info("Search for Alternative Feature Tables!")
        tables = search_test.search_alternative_features(sm, query_name, cfg.return_table_num, code, var_name, 0.4, 0.4)
        logging.info("%s Tables are returned!"%len(tables))
    elif mode == 4:
        logging.info("Search for Alternative Data Cleaning Tables!")
        tables = search_test.search_alternative_data_clean(sm, query_name, cfg.return_table_num,code, var_name, 0.4, 0.4, 0.1)
        logging.info("%s Tables are returned!"%len(tables))

    if len(tables) == 0:
        return ""
    else:
        vardic = [{'varName': v[0], 'varType': type(v[1]).__name__, 'varSize': str(v[1].size), 'varContent': v[1].to_html(index_names = True, justify = 'center', max_rows = 10, max_cols = 5, header = True)} for v in tables] # noqa
        return json.dumps(vardic)

#print(search_tables())