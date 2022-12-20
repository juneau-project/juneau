import codecs
import json
import logging
import sys
import pandas as pd

from data_extension.schema_mapping.schemamapping import SchemaMapping
from data_extension.store.store import Storage, remote_flag
from data_extension.table_db import connect2gdb
from data_extension.test.nb_utils import analyze_cell
from data_extension.util import handle_superlong_nbname, handle_superlong_varname
from data_extension.variable import Var_Info
import data_extension.config as cfg

restore_view_flg = False

class StoreNotebooks:

    def __init__(self, kernel_id, store_var_type, dbname):
        self.kernel_id = str(kernel_id)
        self.store_var_type = store_var_type
        self.dbname = dbname
        self.store = None
        self.store_prov = None
        self.store_ling = None
        self.std_flag = True
        self.graph_db = None
        self.stored_notebooks = []

        if not remote_flag:
            if not self.graph_db:
                self.graph_db = connect2gdb()

    def analyze_and_store(self, nbname):

        if self.store is None:
            self.store = Storage(graph_eng=self.graph_db)

        stored_notebooks_df = pd.read_sql_table('all_notebooks', con=self.store.eng, schema='utils')

        for iter, row in stored_notebooks_df.iterrows():
            self.stored_notebooks.append(row['name'])

        code_temp = ""

        with codecs.open(nbname, 'r', 'utf-8') as inFp:

            text_in_notebook = inFp.read()

            if remote_flag:
                text_in_notebook = text_in_notebook.replace('/Users/yizhang/PycharmProjects/notebook_management/evaluation/related_table_search','/data/juneau_data')
                if '/Users/yizhang/PycharmProjects/notebook_management/similar_table_search' in text_in_notebook:
                    text_in_notebook = text_in_notebook.replace(
                        '/Users/yizhang/PycharmProjects/notebook_management/similar_table_search', '/data/juneau_data')


            sm = SchemaMapping(self.dbname, False)
            # sm.loading_index(self.dbname)

            new_nbname = handle_superlong_nbname(nbname)

            #if new_nbname in self.stored_notebooks:
            #    inFp.close()
            #    return

            start_flag = True
            notebook = json.loads(text_in_notebook)
            prev_node = None
            ccid = 0

            code_List = []
            # indexing_time_notebook = []
            for cid, cell in enumerate(notebook['cells']):

                cell_type = cell['cell_type']
                if cell_type == 'markdown':
                    continue
                elif cell_type == 'code':

                    codes = cell['source']
                    codes = ''.join(codes)
                    code_temp = code_temp + codes

                    code_List.append(codes)
                    logging.info(f"Currently working on {nbname}!")
                    if start_flag:
                        cell_df = analyze_cell(self.kernel_id, codes, start_flag, self.std_flag, self.store_var_type,
                                               False)
                        start_flag = False
                    else:
                        cell_df = analyze_cell(self.kernel_id, codes, start_flag, self.std_flag, self.store_var_type,
                                               False)
                    #continue
                    #logging.info(cell_df)
                    var_name_list = []
                    var_table_list = []
                    var_info_list = []

                    if cell_df is not None:
                        for rindex, row in cell_df.iterrows():

                            if row['Variable'] not in codes:
                                continue

                            if "=" not in codes:
                                continue

                            var_name = handle_superlong_varname(row['Variable'])

                            base_tables = row['Base Tables']
                            var_obj = row['DataFrame']

                            base_table_names = []
                            if not pd.isnull(base_tables):
                                # base_tables = json.loads(base_tables)
                                join_keys = json.dumps(row['DataFrame'])
                                
                                store_table_name = var_name + "_" + str(new_nbname)

                                if restore_view_flg:
                                    drop_view_def = f'DROP VIEW IF EXISTS {cfg.sql_dbs}."{store_table_name}" CASCADE;'
                                    self.store.eng.execute(drop_view_def)

                                try:    
                                    # store all base tables associated with the view
                                    for idx, table_name in enumerate(base_tables):

                                        # var_name of a base table = t(idx) + var_name
                                        logging.info('store base table: ' + table_name)
                                    
                                        #store_table_name = str(ccid) + "_" + f't{idx}{var_name}' + "_" + str(new_nbname)
                                        store_table_name = str(ccid) + "_" + table_name + "_" + str(new_nbname)
                                        base_table_names.append(f'{cfg.sql_dbs}.rtable{store_table_name}')
                                        #var_obj = var_obj.replace(f'{cfg.sql_dbs}."{table_name}"', f'{cfg.sql_dbs}."rtable{store_table_name}"')

                                        join_keys = join_keys.replace(table_name, f'rtable{store_table_name}')
                                        new_var = Var_Info(table_name, ccid, store_table_name, new_nbname,
                                                       base_tables[table_name]['table'],
                                                       codes,
                                                       code_List, prev_node)

                                        if 'transpose' in base_tables[table_name]:
                                            new_node = self.store.store_table(new_var, sm, True, True)
                                        else:
                                            new_node = self.store.store_table(new_var, sm, True, False)
                                        
                                        #sm.load_index()
                                        #sm.dump_index()
                                        var_name_list.append(store_table_name)
                                        prev_node = new_node


                                # create this view in postgreSQL
                                # replace placeholder names of the base table with their actual names
                                # TODO: need to add the schema as well
                                #store_table_name = var_name + "_" + str(new_nbname)

                                # modify var_obj
                                    #view_def = f'CREATE OR REPLACE VIEW rowstore."{store_table_name}" AS {var_obj};'
                                    view_def = ""
                                    self.store.store_view([view_def], table_name=store_table_name, base_tables=json.dumps(base_table_names), join_keys=json.dumps(join_keys))
                                    continue
                                except:
                                    logging.error(sys.exc_info())
                                    import pickle
                                    error_file = open(f'error_msg_{store_table_name}.json','wb')
                                    pickle.dump(base_tables, error_file)
                                    error_file.close()
                                    continue

                            store_table_name = str(ccid) + "_" + var_name + "_" + str(new_nbname)

                            new_var = Var_Info(var_name, ccid, store_table_name, new_nbname, var_obj, codes,
                                               code_List, prev_node)

                            logging.info("store here.")
                            new_node = self.store.store_table(new_var, sm, False, False)
                            try:
                                #                                sm.load_index()

#                                sm.dump_index()

                                var_table_list.append(store_table_name)
                                var_name_list.append(var_name)
                                var_info_list.append(new_var)

                                prev_node = new_node

                            except:
                                logging.info("error in dumping index: " + str(sys.exc_info()))
                                continue

                        #view_store_class = Store_View(cell['source'], var_table_list, var_name_list, var_info_list)
                        #views_to_store = view_store_class.detect_view_from_code()
                        #view_statements_to_store = view_store_class.store_views(views_to_store)
                        #self.store.store_view(view_statements_to_store)

                    ccid = ccid + 1

                    # node = self.store_prov.add_cell(code_temp, node, var_name_list)
            # if len(indexing_time_notebook) != 0:
            #    logging.info("Average Indexing Time: " + str(float(sum(indexing_time_notebook))/float(len(indexing_time_notebook))))

            inFp.close()
            self.store.dump_table_info()
            if cfg.profile_flag:
                self.store.data_profile_class.store_new_profiles()
            self.stored_notebooks.append(new_nbname)
            # sm.dump_index()

    def close_dbconnection(self):
        notebook_df = pd.DataFrame(self.stored_notebooks, columns=['name'])
        #print(notebook_df)
        notebook_df.to_sql(name = 'all_notebooks', con=self.store.eng.connect(), schema='utils', if_exists='replace', index=False)
        
        if not remote_flag:
            self.store.store_graph_class.store_code_dict()
        self.store.store_df_class.close_dbconnection()
