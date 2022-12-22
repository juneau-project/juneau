from juneau.config import config
import logging

class StoreLSHE:
    def __init__(self, psql_engine):
        self.psql_engine = psql_engine

    def indexed(self, schema_string, table_string):
        exec_string = f"SELECT EXISTS (SELECT * FROM utils.indexed_table WHERE schema_string='{schema_string}' AND table_string='{table_string}')"
        with self.psql_engine.begin() as psql_connection:
            data = psql_connection.execute(exec_string)
            boolean = data.first()[0]
        return boolean

    def store_from_array(self, tables):
        """
        construct LSHE indices for tables specified in the input
        :param
            tables: array of tuples (schema_string, table_string)
        """

        num_hash = config.lshe.num_hash
        max_k = config.lshe.max_k
        max_l = int(num_hash / max_k)  # number of bands
        num_part = config.lshe.num_part  # number of partitions
        corpus_sig_string = config.lshe.corpus_sig
        corpus_hash_string = config.lshe.corpus_hash

        exec_string = ''

        for table in tables:
            schema_string, table_string = table
            if self.indexed(schema_string, table_string):
                continue
            exec_string += f"SELECT corpus_construct_sig_single('{schema_string}', '{table_string}', {num_hash}, '{corpus_sig_string}');"

        # only execute the SQL commands if at least 1 table is not indexed
        if exec_string:
            exec_string = 'DELETE FROM utils.count_table;' + exec_string
            exec_string += f"SELECT corpus_construct_hash('{corpus_sig_string}', '{corpus_hash_string}', {max_k}, {max_l}, {num_part});"

            with self.psql_engine.begin() as psql_connection:
                psql_connection.execute(exec_string)


    def store_schema(self):
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
        logging.info("TABLE LSHE STORED!")