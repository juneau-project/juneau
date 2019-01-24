from sqlalchemy import create_engine
import json
import pandas as pd
from sys import getsizeof
import random


user_name = "yizhang"
password = "yizhang"

def _getsizeof(x):
    # return the size of variable x. Amended version of sys.getsizeof
    # which also supports ndarray, Series and DataFrame
    if type(x).__name__ in ['ndarray', 'Series']:
        return x.nbytes
    elif type(x).__name__ == 'DataFrame':
        return x.memory_usage().sum()
    else:
        return getsizeof(x)

def __connect2db(dbname):
    engine = create_engine("postgresql://" + user_name + ":" + password + "@localhost/" + dbname)
    return engine.connect()

def __fetch_all_views(eng):
    tables = eng.execute("select table_name from INFORMATION_SCHEMA.views;")
    views = []
    for rows in tables:
        t_name = rows[0]
        if "exp_view_table" in t_name:
            views.append(t_name)


    views_dict = {}
    for i in views:
        tb_df = pd.read_sql_table(i, eng)
        views_dict[i] = tb_df
    return views_dict


def search_tables():
    eng = __connect2db('joinstore')
    tables = __fetch_all_views(eng)
    tables_name = list(tables.keys())
    random.shuffle(tables_name)
    tables_name = tables_name[:10]
    vardic = [{'varName': v, 'varType': type(tables[v]).__name__, 'varSize': str(tables[v].size), 'varContent': tables[v].to_string()[:200]} for v in tables_name] # noqa
    return json.dumps(vardic)

#print(search_tables())