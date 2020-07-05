import json
import logging
import sys
from sys import getsizeof

if sys.version_info[0] < 3:
    pass
else:
    pass

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


def search_tables(search_test, var_df, mode, code, var_name):
    query_table = var_df  # dataframe
    query_name = var_name  # name of the table

    if mode == 1:
        logging.info("Search for Additional Training/Validation Tables!")
        tables = search_test.search_additional_training_data(query_table, 10, code, var_name, 0.5, 1)
        # tables = search_test.search_similar_tables_threshold2(query_table, 0.5, 10, 1.5, 0.9, 0.2)
        logging.info("%s Tables are returned!" % len(tables))
        # tables = search_test.search_similar_tables_threshold2(query_table, 10, 0.5, 5, 1, 0.9, 0.2, True, 10)
    elif mode == 2:
        logging.info("Search for Joinable Tables!")
        tables = search_test.search_joinable_tables_threshold2(query_table, 0.1, 10, 1.5, 0.9, 0.2)
        logging.info("%s Joinable Tables are returned!" % len(tables))
    elif mode == 3:
        logging.info("Search for Alternative Feature Tables!")
        tables = search_test.search_alternative_features(query_table, 10, code, var_name, 90, 200, 0.1, 10, 0.9, 0.2)
        logging.info("%s Tables are returned!" % len(tables))

    if len(tables) == 0:
        return ""
    else:
        vardic = [{'varName': v[0], 'varType': type(v[1]).__name__, 'varSize': str(v[1].size),
                   'varContent': v[1].to_html(index_names=True, justify='center', max_rows=10, max_cols=5, header=True)}
                  for v in tables]  # noqa
        return json.dumps(vardic)

