import logging
import pandas as pd
from sqlalchemy.exc import NoSuchTableError
from data_extension.store.store_prov import Store_Lineage
from data_extension.store.store_table import Store_Seperately
from data_extension.store.store_graph import Store_Provenance
from data_extension.table_db import connect2gdb, connect2db
from data_extension.schema_mapping.data_profile import Data_Profile
import data_extension.config as cfg

import sys
remote_flag = True

class Storage:

    def __init__(self, eng = None, graph_eng = None):

        if eng is not None:
            self.eng = eng
        else:
            self.eng = None

        if not remote_flag:
            if graph_eng is None:
                self.graph_eng = connect2gdb()
            else:
                self.graph_eng = graph_eng

        self.store_df_class = Store_Seperately(self.eng)
        self.eng = self.store_df_class.eng
        self.data_profile_class = Data_Profile(self.eng, 'data_profile')

        self.transpose_tables_df = pd.read_sql_table('transpose_table', con=self.eng, schema='utils')
        self.transpose_tables = []
        
        for iter, row in self.transpose_tables_df.iterrows():
            if row['_hashed'] ==  True:
                self.transpose_tables.append(row['_schema'] + "#sep#" + row['_tbl'] + "#sep#" + row['_key'] + "#sep#" + row['_val'] + "#sep#" + str(row['_hashed']))
            else:
                self.transpose_tables.append(row['_schema'] + "#sep#" + row['_tbl'] + "#sep#" + row['_key'] + "#sep#" + row['_val'] + "#sep#")

        #self.store_graph_class = Store_Provenance(self.eng, self.graph_eng)

        #self.store_lineage_class = Store_Lineage(self.eng)

        # to do : should keep a list to store indexed variable to avoid duplication

    def store_view(self, view_statements, table_name=None, base_tables=None, join_keys=None):

        logging.info("Indexing views in this cell: ")
        conn = self.eng.connect()
        logging.info(join_keys)
        #for view in view_statements:
        #   logging.info(view)
        #   conn.execute(view)

        if table_name is not None:
            if base_tables is None and join_keys is None:
                conn.execute(f"INSERT INTO utils.view_table VALUES ('{table_name}');")
            else:
                conn.execute(f"INSERT INTO utils.view_table VALUES ('{table_name}', '{base_tables}', '{join_keys}');")

        conn.close()

    def store_table(self, new_var, schema_mapping_class, base_flg = False, trans_flg = False):

        print("here we go!")
        logging.info(f"Indexing new { trans_flg } table {new_var.store_name} started:")
        conn = self.eng.connect()

        if True:
            try:
                self.store_df_class.InsertTable_Sperately(new_var.store_name, new_var.value)
                logging.info(f'Base table {new_var.store_name} stored!')
            except:
                logging.error('Base table store failed')
                logging.error(sys.exc_info())

            if trans_flg:
                s_keys = []
                s_vals = []
                for col in new_var.value.columns:
                    if col.startswith('key'):
                        s_keys.append(col)
                    if col.startswith('val'):
                        s_vals.append(col)
                assert len(s_keys) == 1
                assert len(s_vals) == 1
        
                if trans_flg:
                    self.transpose_tables.append(cfg.sql_dbs + "#sep#" + 'rtable' + new_var.store_name + "#sep#" + s_keys[0] + "#sep#" + s_vals[0] + "#sep#")
            
        #logging.info(new_var.value.shape)
        if cfg.profile_flag:
            if new_var.value.shape[0] > 1 and new_var.value.shape[1] > 1:
                #try:
                if base_flg:
                    if not trans_flg:
                        self.data_profile_class.schema_mapping(new_var.store_name, new_var.value)
                else:
                    self.data_profile_class.schema_mapping(new_var.store_name, new_var.value)
            #except:
            #    logging.error("Error in Creating Data Profiles: " + str(sys.exc_info()))
        

        if base_flg:
            logging.info('print base table info:')
            logging.info(new_var.store_name)
            logging.info(new_var.value.shape)
            logging.info(new_var.value.columns)
        
        del new_var

        try:
            if not remote_flag:
                try:
                    new_node = self.store_graph_class.add_cell(new_var.code_str, new_var.neo4j_prev_node, new_var.var_name, new_var.cid, new_var.nb)
                    logging.info("Corresponding cell and variable inserted to the neo4j graph!")
                except:
                    logging.info("Node Not Inserted: " + str(sys.exc_info()))
                    new_node = None
            else:
                new_node = None


            #try:
                #schema_mapping_class.mapping_to_columns_index2(new_var.value, 'rtable' + new_var.store_name, sql_dbname, 0.5)
                #logging.info('Table Columns Indexed!')
            # except:
            #     logging.error("Error when storing index: " + str(sys.exc_info()))

            #try:
            #    self.store_lineage_class.InsertTable_Model(new_var.store_name, new_var.var_name, new_var.code_list, new_var.nb)
            #    logging.info('Dependency graph inserted!')
            # except:
            #     logging.error('Unable to store provenance of ' + new_var.store_name + ' due to error' + str(sys.exc_info()[0]))


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

    def dump_table_info(self):
        transpose_tables = []
        self.transpose_tables = list(set(self.transpose_tables))

        for table in self.transpose_tables:
            table_str = table.split("#sep#")
            table_str[-1] = bool(table_str[-1])
            transpose_tables.append(table_str)

        transpose_tables_df = pd.DataFrame(transpose_tables, columns=['_schema', '_tbl', '_key', '_val', '_hashed'])
        transpose_tables_df.to_sql('transpose_table', con=self.eng, schema='utils', index=False, if_exists='replace')
