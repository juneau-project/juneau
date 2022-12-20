import base64
import json
import logging
import random

import pandas as pd

from data_extension.config import sql_dbname, return_table_num
from data_extension.schema_mapping.schemamapping import SchemaMapping
from data_extension.search.search_withprov_sk import WithProv_Cached
from data_extension.search.Query import Query

random.seed(101)
query_num = 20


def exp_search1(table_name, search_class, k, SM_test, candidate_table_list=None):
    query_name = table_name
    var_name = table_name[6:]
    var_name_list = var_name.split("_")
    var_name = "_".join(var_name_list[1:-1])
    code_list = json.loads(base64.b64decode(
        table_code[table_code['view_id'].transform(lambda x: x.replace("-", "")) == query_name[6:]][
            'view_cmd'].tolist()[0]).decode("utf-8"))
    code_list = "".join(code_list)
    query = Query(search_class.db_eng, query_name, code_list, var_name)

    return_tables = search_class.search_additional_training_data(SM_test, query, k, candidate_table_list=candidate_table_list)
    logging.info("results:")
    logging.info(return_tables)
    return return_tables


search_test_class = WithProv_Cached(sql_dbname, "rowstore", clear_cache=True)
all_tables = search_test_class.tables
query_tabless = [random.sample(all_tables, query_num)]
table_code = pd.read_sql_table("var_code", search_test_class.eng, schema="graph_model")

for query_tables in query_tabless:
    time_total = 0
    SM_test = SchemaMapping(sql_dbname, True)
    for qtable in query_tables:
        rtime = exp_search1(qtable, search_test_class, return_table_num, SM_test)
        time_total = time_total + rtime
    logging.info("Average 1 E top-" + str(return_table_num) + " Running Time: " + str(
        float(time_total) / float(len(query_tables))))


# top K on multiple tables
# TODO: figure out how to compute the return_table_num
def exp_search2(schema_string, tables):
    SM_test = SchemaMapping(sql_dbname, True)
    search_test_class = WithProv_Cached(sql_dbname, schema_string, clear_cache=True)

    similar_tables = []
    for table in tables:
        if not similar_tables:
            similar_tables = exp_search1(table, search_test_class, return_table_num, SM_test)
        else:
            similar_tables = exp_search1(table, search_test_class, return_table_num, SM_test, candidate_table_list=similar_tables)

    return similar_tables


