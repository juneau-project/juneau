import logging
import random
import data_extension.config as cfg
import pandas as pd
import json
import numpy as np
#from data_extension.schema_mapping.schemamapping import SchemaMapping
from data_extension.search.search_view import WithView
from data_extension.search.Query import Query
import json
import io
import sys
from psycopg2 import connect
import pickle
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

dbconfig = {
    "host": cfg.sql_host,
    "dbname": cfg.sql_dbname,
    "user": cfg.sql_name,
    "password": cfg.sql_password,
    "port": cfg.sql_port
}

def read_tables_faster(name, schema):

    try:
        query = f"SELECT * FROM {schema}.\"{name}\""

        copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
            query=query, head="HEADER")

        with connect(**dbconfig) as conn:
            with conn.cursor() as cur:
                store = io.StringIO()
                cur.copy_expert(copy_sql, store)
                store.seek(0)
                table_r = pd.read_csv(store)

    except:
        logging.info(sys.exc_info())

    return table_r


random.seed(101)
query_num = 20
topks = [5]
#topks = [20]

search_test_class = WithView(cfg.sql_dbname, cfg.sql_dbs, window_size=5)
all_queries = search_test_class.queries
logging.info("There are " + str(len(all_queries)) + " query views.\n")

search_queries = {}
search_queries[2] = []
search_queries[3] = []
search_queries[4] = []

query_join_number = {}
for vid, query_view in enumerate(all_queries):

    query_join_keys = json.loads(query_view[2])

    if len(query_join_keys) > 0:
        #logging.info(f"Handling the {vid} th query:")
        #logging.info(query_view)

        base_tables = json.loads(query_view[1])
        #if len(base_tables) not in query_join_number:
        #    query_join_number[len(base_tables)] = 0

        #for j in range(len(base_tables)):
        #    if j + 1 not in query_join_number:
        #        query_join_number[j+1] = 0
        #    query_join_number[j + 1] += 1

        #logging.info(f"There are {len(base_tables)} base tables")
        #if len(base_tables) < 4:
        #    continue

        base_table_query = []
        pflg = False
        cnt = 0
        for base_table in base_tables:

            base_table_df = read_tables_faster(base_table[9:], 'rowstore')

            if base_table_df.columns.tolist() == ["#self"]:
                continue

            if sorted(base_table_df.columns.tolist()) == sorted(['#self','#par']):
                continue



            if base_table[9:] in search_test_class.transpose_table:
                pflg = True
                continue
            cnt += 1
            base_table_query.append(base_table[9:])

        if 2 not in query_join_number:
            query_join_number[2] = 0
        if 3 not in query_join_number:
            query_join_number[3] = 0
        if 4 not in query_join_number:
            query_join_number[4] = 0

        if cnt >= 2:
            search_query = base_table_query[:2]
            search_queries[2].append(search_query)
            query_join_number[2] += 1

        if cnt >= 3:
            search_query = base_table_query[:3]
            search_queries[3].append(search_query)
            query_join_number[3] += 1

        if cnt >= 4:
            search_query = base_table_query[:4]
            search_queries[4].append(search_query)
            query_join_number[4] += 1

print(query_join_number)
print(np.sum(list(query_join_number.values())))


all_round_times = {}
all_round_ids = {}
all_round_sk_time = {}
all_round_topk_time = {}
all_round_jc_time = {}

tk = 5
sample_num = 10
query_lens = [3]



for qlen in query_lens:

    all_times = {}
    all_ids = {}
    all_sk_time = {}
    all_topk_time = {}
    all_jc_time = {}

    #sample_search_queries = queries3
    sample_search_queries = search_queries[qlen] #random.sample(search_queries[qlen], sample_num)
    print(sample_search_queries)
    #exit()

    for round in range(1):
        all_times[round] = []
        all_ids[round] = []
        all_sk_time[round] = []
        all_topk_time[round] = []
        all_jc_time[round] = []

        for squery in sample_search_queries:
            time_sk, time_jk = search_test_class.search_initialization(squery, True)
            time1 = search_test_class.search_multiple_tables(squery, tk)
            #time1 = search_test_class.baseline_two_loops(squery, tk, round + 1)
            #continue
            if time1 is not None:# and time_jk < 8:
                logging.info(time1)
                if time_sk > 3:
                    continue
                all_times[round].append(np.sum([time1, time_sk, time_jk]))
                all_topk_time[round].append(time1)
                all_sk_time[round].append(time_sk)
                all_jc_time[round].append(time_jk)

            else:
                continue

    all_round_times[qlen] = all_times
    all_round_jc_time[qlen] = all_jc_time
    all_round_sk_time[qlen] = all_sk_time
    all_round_topk_time[qlen] = all_topk_time

for i in all_round_times.keys():
    for j in all_round_times[i].keys():
        print(f'{i} qlen, round {j}:')
        print(np.mean(all_round_times[i][j]))
        print(all_round_times[i][j])

for i in all_round_sk_time.keys():
    for j in all_round_sk_time[i].keys():
        print(f'{i} qlen, round {j}:')
        print(np.mean(all_round_sk_time[i][j]))
        print(all_round_sk_time[i][j])

for i in all_round_topk_time.keys():
    for j in all_round_topk_time[i].keys():
        print(f'{i} qlen, round {j}:')
        print(np.mean(all_round_topk_time[i][j]))
        print(all_round_topk_time[i][j])

results = [all_round_times, all_round_sk_time, all_round_topk_time]
pickle.dump(results, open("results_run_best.p", "wb"))

#with open("query_table_name.txt", "w") as outfp:
#    for i in all_query_tables:
#        for j in i:
#            outfp.write(j + "\t")
#        outfp.write("\n")
#    outfp.close()


#for qid, query in enumerate(queries):
#    if query_join_number[qid] == 2:
#        time1 = search_test_class.search_multiple_tables(query, 5)
#        logging.info(time1)
#        break

#timelist1 = {5:[], 10:[], 15:[], 20:[]}
#timelist2 = {5:[], 10:[], 15:[], 20:[]}

#for query in queries:
#    logging.info(query)
#    query = query[:2]
#    search_test_class.search_initialization(query)
#    for topk in topks:
#        time1 = search_test_class.search_multiple_tables(query, topk)
#        time2 = search_test_class.baseline_two_loops(query, topk)
#        timelist1[topk].append(time1)
#        timelist2[topk].append(time2)
#logging.info(timelist1)
#for topk in timelist1.keys():
#    logging.info(f"{topk}: {np.mean(timelist1[topk])}")
#logging.info(timelist2)
#for topk in timelist2.keys():
#    logging.info(f"{topk}: {np.mean(timelist2[topk])}")

#with open("d2results.json","w") as outfp:
#    json.dump([timelist1, timelist2],outfp)
#    outfp.close()

