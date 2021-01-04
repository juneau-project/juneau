import logging
from sqlalchemy.exc import NoSuchTableError
from .store_table import SeparateStorage
from .store_prov import LineageStorage
from .store_graph import ProvenanceStorage
from .store_lshe import StoreLSHE
from juneau.config import config
from juneau.db.table_db import connect2gdb, connect2db_engine

import sys

class Storage:

    def __init__(self, eng = None, graph_eng = None):

        if eng != None:
            self.eng = eng
        else:
            self.eng = None

        if graph_eng == None:
            self.graph_eng = connect2gdb()
        else:
            self.graph_eng = graph_eng

        self.store_df_class = SeparateStorage(self.eng)
        self.eng = self.store_df_class.eng
        self.store_graph_class = ProvenanceStorage(self.eng, self.graph_eng)
        self.store_lineage_class = LineageStorage(self.eng)
        self.store_lshe_class = StoreLSHE(connect2db_engine(config.sql.dbname))
        self.store_lshe_class.store_schema()
        # to do : should keep a list to store indexed variable to avoid duplication


    def store_table(self, new_var, schema_mapping_class):


        logging.info("Indexing new table " + new_var.store_name)
        conn = self.eng.connect()


        try:
            self.store_df_class.insert_table_separately(new_var.store_name, new_var.value)
            logging.info('Base table stored!')

            try:
                new_node = self.store_graph_class.add_cell(new_var.code_str, new_var.neo4j_prev_node, new_var.var_name,
                                                       new_var.cid, new_var.nb)
                logging.info("Corresponding cell and variable inserted to the neo4j graph!")

            except:
                logging.info("Node not inserted: " + str(sys.exc_info()))
                new_node = None

            try:
                schema_mapping_class.load_index()
                schema_mapping_class.mapping_to_columns_index2(new_var.value, 'rtable' + new_var.store_name, 0.5)
                schema_mapping_class.dump_index()
                logging.info('Table Columns Indexed!')
            except:
                logging.error("Error when storing index: " + str(sys.exc_info()))

            try:
                self.store_lineage_class.insert_table_model(new_var.store_name, new_var.var_name, new_var.code_list)
                logging.info('Dependency graph inserted!')
            except:
                logging.error(
                    'Unable to store provenance of ' + new_var.store_name + ' due to error' + str(sys.exc_info()))

        # this process can take as long as an hour to complete, so if you don't want to wait for that long
        # feel free to comment it out when you are running this script
            #try:
            #    schema = sql_dbs  # the schema that stores the corpus of tables
            ##    num_hash = 256  # number of hash functions used
             #   max_k = 4
             #   max_l = int(num_hash / max_k)  # number of bands
             #   num_part = 32  # number of partitions
             #   exec_string = f"SELECT corpus_construct_sig('{schema}', {num_hash});"
             #   exec_string += f"SELECT corpus_construct_hash({max_k}, {max_l}, {num_part})"
             #   conn.execute(exec_string)
            #except:
            #    logging.error(f'Unable to complete LSH indexing corpus in rowstore')
            #    logging.info(sys.exc_info())

            #logging.info("Returning after indexing " + new_var.store_name)

        except ValueError:
            logging.error('Unable to store ' + new_var.store_name + ' due to value error')
            logging.error(sys.exc_info())
        except NoSuchTableError:
            logging.error('Unable to store ' + new_var.store_name + ' due to no-such-table error')
            logging.error(sys.exc_info())
        except KeyboardInterrupt:
            return
        except:
            logging.error('Unable to store ' + new_var.store_name + ' due to error ' + str(sys.exc_info()[0]))
            logging.error(sys.exc_info())
            raise
        finally:
            conn.close()

        return new_node