from sqlalchemy import create_engine
import data_extension.config as cfg
import json
import time


f2 = open('./time_join_score.txt', 'w')

start = time.time()
conn_string = f"postgresql://{cfg.sql_name}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
engine = create_engine(conn_string)

lines = ''

with open("./table_joinscore_tocompute.json") as f:
    var_obj = json.load(f)

    with engine.begin() as conn:
        for row in var_obj:
            # line = f"select utils.join_score('rowstore', '{row[0]}', '{row[2]}', 'rowstore', '{row[1]}', '{row[3]}');"
            # print(line)
            # conn.execute(line)
            lines += f"select utils.join_score('rowstore', '{row[0]}', '{row[2]}', 'rowstore', '{row[1]}', '{row[3]}');"
        conn.execute(lines)


f2.write(str(time.time() - start))
f2.write('\n')
f2.close()
