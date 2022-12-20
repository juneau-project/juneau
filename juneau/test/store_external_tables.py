import os
import json
import pandas as pd
import sys
from sqlalchemy import create_engine
import data_extension.config as cfg

conn_string = f"postgresql://{cfg.sql_name}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
engine = create_engine(conn_string, pool_size=20, max_overflow=20).connect()

group_ids = ['2', '3', '4', '5', '6']

for gid in group_ids:
    external_data_path = "/data2/table_corpus/web_tables/wdc_table_corpus_2015/" + gid
    json_files = os.listdir(external_data_path)

    tid = 0
    error_cnt = 0
    for jid, jfile in enumerate(json_files):

        with open(os.path.join(external_data_path, jfile), "r") as infp:
            jtable = json.load(infp)
            infp.close()

        df_table = pd.DataFrame([l[1:] for l in jtable['relation']], columns=list(range(len(jtable['relation'][0]) - 1)), index = [l[0] for l in jtable['relation']])
        df_table = df_table.T
        try:
            df_schema = df_table.columns
            df_schema = [' ' if h == '' else h for h in df_schema]
            df_table.columns = df_schema
            df_table.to_sql(name = f'etable_{gid}_{tid}', con = engine, schema='rowstore', if_exists='replace', index=False)
        except:
            print(tid)
            print(df_table.columns)
            print(df_table.index)
            print(sys.exc_info())
            error_cnt += 1

        tid = tid + 1

    print(f"{gid} directory finished!")
    print(f"{tid} tables stored!")
    print(f"Error found in {error_cnt} tables!")
    
