from sqlalchemy import create_engine
import time

from data_extension.search.search_sketch import query_cols_parallel
import data_extension.config as cfg


conn_string = f"postgresql://{cfg.sql_name}:{cfg.sql_password}@{cfg.sql_host}:{cfg.sql_port}/{cfg.sql_dbname}"
e1 = create_engine(conn_string)

columns = []

for tbl in ['rtable8_#r0#_tebooksworldbankeducation', 'rtable8_#r1#_tebooksworldbankeducation']:
    with e1.connect() as conn1:
        result = conn1.execute(
            f'SELECT column_name FROM information_schema.columns WHERE table_schema = \'rowstore\' AND table_name = \'{tbl}\';')
        for row in result:
            if '#self' not in row[0] and '#par' not in row[0]:
                columns.append({'schema': 'rowstore', 'table': tbl, 'col': row[0]})

t0 = time.time()
ls = query_cols_parallel(columns)
print(time.time() - t0)
print(len(ls))
