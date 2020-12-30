from juneau.config import config
import logging
import sys
import os

sql_funs_def = ["lshe_initialize.sql", "minhash.sql", "corpus_construct_sig.sql", \
 "corpus_construct_hash.sql", "q_construct_sig.sql", "q_construct_hash.sql", \
 "q_query_lshe.sql", "q_query_ks.sql"]

this_dir, this_filename = os.path.split(__file__)

class StoreLSHE:

    def __init__(self, psql_engine):
        self.psql_engine = psql_engine

        for file in sql_funs_def:
            self.psql_engine.execute(open(os.path.join(this_dir, "sql/" + file), "r").read())


    def store(self):
        try:
            schema = config.sql.dbs  # the schema that stores the corpus of tables
            num_hash = config.lshe.num_hash  # number of hash functions used
            max_k = config.lshe.max_k
            max_l = int(num_hash / max_k)  # number of bands
            num_part = config.lshe.num_part  # number of partitions
            corpus_sig_string = config.lshe.corpus_sig
            corpus_hash_string = config.lshe.corpus_hash
            exec_string = f"SELECT corpus_construct_sig('{schema}', {num_hash}, '{corpus_sig_string}');"
            exec_string += f"SELECT corpus_construct_hash('{corpus_sig_string}', '{corpus_hash_string}', {max_k}, {max_l}, {num_part});"

            with self.psql_engine.begin() as psql_connection:
                psql_connection.execute(exec_string)
        except:
            logging.error(f'Unable to complete LSH indexing corpus in {config.sql.dbs}')
            logging.info(sys.exc_info())