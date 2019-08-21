from notebook.utils import url_path_join
from notebook.base.handlers import IPythonHandler

import json
import threading
import pandas as pd

from data_extension.search_withprov_opt import WithProv_Optimized
from data_extension.search import search_tables
import data_extension.jupyter
from data_extension.table_db import connect2gdb
from data_extension.table_db import connect2db, connect2db_engine
import data_extension.config as cfg
from data_extension.store_graph import Store_Provenance
from data_extension.store_prov import Store_Lineage

import sqlalchemy
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.orm import sessionmaker
#from multiprocessing import Pool, TimeoutError
import concurrent.futures

import os
import sys

#if sys.version_info[0] < 3:
#    from StringIO import StringIO
#else:
from io import StringIO

import logging

logging.basicConfig(level=logging.DEBUG)

HERE = os.path.dirname(__file__)

import data_extension.config as cfg

stdflag = False
pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)#Pool(processes=2)
indexed = {}

types_to_exclude = [ \
    'module', \
    'function', \
    'builtin_function_or_method', \
    'instance', \
    '_Feature', \
    'type', \
    'ufunc']

types_to_include = [ \
    'ndarray', \
    'DataFrame', \
    'list']

search_test_class = None

# From https://stackoverflow.com/questions/31997859/bulk-insert-a-pandas-dataframe-using-sqlalchemy
def write_df(engine, df_to_be_written, table_name, my_schema):
    Base = declarative_base()

    # Declaration of the class in order to write into the database. This structure is standard and should align with SQLAlchemy's doc.
    class Current(Base):
        __tablename__ = table_name

        id = Column(Integer, primary_key=True)
        Date = Column(String(500))
        Type = Column(String(500))
        Value = Column(Numeric())

        def __repr__(self):
            return "(id='%s', Date='%s', Type='%s', Value='%s')" % (self.id, self.Date, self.Type, self.Value)

    # The orient='records' is the key of this, it allows to align with the format mentioned in the doc to insert in bulks.
    listToWrite = df_to_be_written.to_dict(orient='records')

    metadata = sqlalchemy.schema.MetaData(bind=engine, reflect=True)
    table = sqlalchemy.Table(table_name, metadata, autoload=True, schema=my_schema)

    # Open the session
    Session = sessionmaker(bind=engine)
    conn = engine.connect()
    session = Session()

    logging.info("Starting with drop, create, insert")
    # Inser the dataframe into the database in one bulk
    try:
        conn.execute(table.drop())
    except:
        pass

    try:
        conn.execute(table.create())
    except:
        pass

    try:
        conn.execute(table.insert(), listToWrite)
    except:
        logging.error("Could not insert into table")

    # Commit the changes
    session.commit()

    # Close the session
    session.close()
    conn.close()

# Asynchronous storage of tables
def fn(output, store_table_name, var_name, var_code, var_nb_name, psql_engine, store_prov_db_class):
    logging.info("Indexing new table " + store_table_name)
    conn = psql_engine.connect()
    try:
        output.to_sql(name='rtable' + store_table_name, con=conn, \
                          schema=cfg.sql_dbs, if_exists='replace', index=False)
        #write_df(psql_engine, output, 'rtable' + store_table_name, cfg.sql_dbs)

        logging.info('Base table stored')

        try:
            code_list = var_code.split("\\n#\\n")

            logging.info(var_nb_name)

            logging.info(code_list)

            store_prov_db_class.InsertTable_Model(store_table_name, var_name, code_list, var_nb_name)
            indexed[store_table_name] = True
        except:
            logging.error(
                'Unable to store provenance of ' + store_table_name + ' due to error' + str(sys.exc_info()[0]))

        logging.info("Returning after indexing " + store_table_name)

    except ValueError:
        logging.error('Unable to store ' + store_table_name + ' due to value error')
    except NoSuchTableError:
        logging.error('Unable to store ' + store_table_name + ' due to no-such-table error')
    except KeyboardInterrupt:
        return
    except:
        logging.error('Unable to store ' + store_table_name + ' due to error ' + str(sys.exc_info()[0]))
        raise
    finally:
        conn.close()



class JuneauHandler(IPythonHandler):
    kernel_id = None
    search_var = None
    done = {}
    code = None
    mode = None
    dbinfo = {}
    data = {}
    data_trans = {}
    graph_db = None
    psql_engine = None
    store_graph_db_class = None
    store_prov_db_class = None
    prev_node = None
    prev_nb = ""
    data_to_store = {}

    def initialize(self):
        #logging.info('Calling Juneau handler...')
        # self.search_test_class = WithProv(dbname, 'rowstore')
        pass

    def find_variable(self, search_var, kernel_id):
        # Make sure we have an engine connection in case we want to read
        if kernel_id not in self.done:
            o2, err = data_extension.jupyter.exec_ipython( \
                kernel_id, search_var, 'connect_psql')
            # o2, err = data_extension.jupyter.connect_psql(kernel_id, search_var)
            self.done[kernel_id] = {}
            logging.info(o2)
            logging.info(err)

        logging.info('Looking up variable ' + search_var)


        #return json.dumps(vardic)

        output, error = data_extension.jupyter.request_var(kernel_id, search_var)
        #output, error = data_extension.jupyter.exec_ipython(kernel_id, search_var, 'print_var')
        logging.info('Returned with variable')

        if error != "" or output == "" or output is None:
            sta = False
            return sta, error
        else:
            try:
                #logging.info('Parsing: ' + output)
                var_obj = pd.read_json(output, orient='split')
                sta = True
            except:
                logging.info('Parsing: ' + output)
                sta = False
                var_obj = None


        return sta, var_obj

    def fetch_kernel_id(self):
        self.kernel_id = str(self.data['kid'][0])[2:-1]

    def fetch_search_var(self):
        self.search_var = str(self.data['var'][0])[2:-1]

    def fetch_search_mode(self):
        self.mode = int(self.data['mode'][0])

    def fetch_code(self):
        self.code = str(self.data['code'][0])[2:-1]

    def fetch_dbinfo(self):
        self.dbinfo = {'dbnm': cfg.sql_dbname, \
                       'host': cfg.sql_host, \
                       'user': cfg.sql_name, \
                       'pswd': cfg.sql_password, \
                       'schema': cfg.sql_dbs}

    def put(self):
        global pool
        global indexed
        global fn
        self.data_to_store = self.request.arguments

        if len(self.data_to_store) == 0:
            self.data_trans = {'res': "", 'error': "Invalid request", 'state': str('false')}
            self.write(json.dumps(self.data_trans))
            return

        var_to_store = str(self.data_to_store['var'][0])[2:-1]
        logging.info('Juneau indexing request: ' + var_to_store)

        var_code = str(self.data_to_store['code'][0])[2:-1]
        var_nb_name = str(self.data_to_store['nb_name'][0])[2:-1]
        var_cell_id = int(str(self.data_to_store['cell_id'][0])[2:-1])
        kernel_id = str(self.data_to_store['kid'][0])[2:-1]

        var_nb_name = var_nb_name.replace('.ipynb', '')
        var_nb_name = var_nb_name.split("_")
        var_nb_name = "-".join(var_nb_name)


        code_list = var_code.lower().strip("\\n#\\n").split("\\n#\\n")

        if (len(str(var_cell_id) + "_" + var_to_store + "_" + str(var_nb_name)) < 58):
            store_table_name = str(var_cell_id) + "_" + var_to_store + "_" + str(var_nb_name)
            var_nb_name = var_nb_name
        else:
            nb_len = (63 - len(str(var_cell_id)) - 2 - len(var_to_store) - 6)
            var_nb_name = str(var_nb_name)[-nb_len:]
            store_table_name = str(var_cell_id) + "_" + var_to_store + "_" + var_nb_name

        if (store_table_name in indexed) or (var_to_store.lower() not in code_list[-1]):
            logging.info('Request to index is already registered')
        else:
            logging.info(var_to_store.lower())
            logging.info(code_list[-1])

            success, output = self.find_variable(var_to_store, kernel_id)
            if success:
                if not self.graph_db:
                    self.graph_db = connect2gdb()
                if not self.psql_engine:
                    self.psql_engine = connect2db_engine(cfg.sql_dbname)

                if not self.store_graph_db_class:
                    psql_db = self.psql_engine#.connect()
                    self.store_graph_db_class = Store_Provenance(psql_db, self.graph_db)
                    #psql_db.close()

                if not self.store_prov_db_class:
                    psql_db = self.psql_engine#.connect()
                    self.store_prov_db_class = Store_Lineage(psql_db)
                    #psql_db.close()

                if self.prev_nb != var_nb_name:
                    self.prev_node = None

                try:
                    self.prev_node = self.store_graph_db_class.add_cell(var_code, \
                                                                        self.prev_node, \
                                                                        var_to_store, \
                                                                        var_cell_id, var_nb_name)
                except:
                    logging.error('Unable to store in graph store due to error ' + str(sys.exc_info()[0]))
                self.prev_nb = var_nb_name

                fn(output, store_table_name, var_to_store, var_code, var_nb_name,
                   self.psql_engine, self.store_prov_db_class)

                #res = pool.submit(fn, output, store_table_name, var_code, var_nb_name, \
                #                            self.psql_engine, self.store_prov_db_class)

                #res.result()

        self.data_trans = {'res': "", 'state': str('true')}
        self.write(json.dumps(self.data_trans))

    def post(self):

        # Check if we are initialized yet
        global search_test_class
        if not search_test_class:
            self.data_trans = {'error': 'The Juneau server is still initializing', \
                               'state': str('false')}
            self.write(json.dumps(self.data_trans))
            return

        logging.info('Juneau handling search request')
        self.data = self.request.arguments
        self.fetch_search_var()
        self.fetch_kernel_id()
        self.fetch_search_mode()

        if self.mode == 0:  # return table
            if self.search_var in search_test_class.real_tables:
                self.data_trans = {'res': "", 'state': str('true')}
                self.write(json.dumps(self.data_trans))
            else:
                self.data_trans = {'res': "", 'state': str('false')}
                self.write(json.dumps(self.data_trans))
        else:
            self.fetch_code()

            success, output = self.find_variable(self.search_var, self.kernel_id)

            if success:
                data_json = search_tables(search_test_class, output, self.mode, self.code, self.search_var)
                if data_json != "":
                    self.data_trans = {'res': data_json, 'state': str('true')}
                    self.write(json.dumps(self.data_trans))
                else:
                    self.data_trans = {'res': data_json, 'state': str('false')}
                    self.write(json.dumps(self.data_trans))

            else:
                logging.error("The table was not found:")
                logging.error(output)
                self.data_trans = {'error': str(output), 'state': str('false')}
                self.write(json.dumps(self.data_trans))


def background_load(nb_server_app):
    global search_test_class
    search_test_class = WithProv_Optimized(cfg.sql_dbname, cfg.sql_dbs)
    nb_server_app.log.info("Juneau tables indexed...")


def load_jupyter_server_extension(nb_server_app):
    """
    Called when the extension is loaded.

    Args:
        nb_server_app (NotebookWebApplication): handle to the Notebook webserver instance.
    """
    nb_server_app.log.info("Juneau extension loading...")
    # search_test_class = WithProv_Optimized(cfg.sql_dbname, cfg.sql_dbs)
    #loader = threading.Thread(target=background_load, args=(nb_server_app,))
    background_load(nb_server_app)
    web_app = nb_server_app.web_app
    host_pattern = r'.*$'
    route_pattern = url_path_join(web_app.settings['base_url'], '/juneau')
    web_app.add_handlers(host_pattern, [(route_pattern, JuneauHandler)])
    nb_server_app.log.info("Juneau extension loaded!")
    #loader.start()
