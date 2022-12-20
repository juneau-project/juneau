import psycopg2
import timeit
import pandas as pd
import numpy as np
import sys

from sqlalchemy import create_engine

from data_extension.cost_func import compute_table_size
import data_extension.config as cfg
from data_extension.table_db import connect2db

import logging
# logging.basicConfig(level=logging.DEBUG)

csv_flag = False
class Store_Seperately:

    def __connect2db(self):
        conn_string = f"postgresqla://{cfg.sql_name}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
        engine = create_engine(conn_string)
        return engine.connect()

    def __connect2db_init(self):
        # Define our connection string
        conn_string = "host=\'" + cfg.sql_host + "\' dbname=\'" + cfg.sql_dbname + "\' user=\'" + cfg.sql_name + \
                      "\' password=\'" + cfg.sql_password + "\'"

        # logging.info the connection string we will use to connect
        logging.info("Connecting to database\n	->%s" % (conn_string))

        # get a connection, if a connect cannot be made an exception will be raised here
        try:
            # conn.cursor will return a cursor object, you can use this cursor to perform queries
            conn = psycopg2.connect(conn_string)
            logging.info("Connecting Database Succeeded!\n")
            cursor = conn.cursor()


            query1 = "DROP SCHEMA IF EXISTS utils CASCADE;"
            query2 = "CREATE SCHEMA IF NOT EXISTS utils;"
            query3 = "CREATE TABLE IF NOT EXISTS utils.view_table(view_name varchar, base_tables varchar, join_keys varchar);"
            #query4 = "CREATE TABLE IF NOT ECISTS u"

            try:
                cursor.execute(query1)
                conn.commit()
            except:
                logging.error("Drop View Schema Failed!\n")
                print(sys.exc_info())

            try:
                cursor.execute(query2)
                conn.commit()
            except:
                logging.error("Create View Schema Failed!\n")
                print(sys.exc_info())

            try:
                cursor.execute(query3)
                conn.commit()
            except:
                print(sys.exc_info())

            query1 = "DROP SCHEMA IF EXISTS rowstore CASCADE;"
            query2 = "CREATE SCHEMA rowstore;"

            try:
                cursor.execute(query1)
                conn.commit()
            except:
                logging.error("Drop Table Schema Failed!\n")
                print(sys.exc_info())
            try:
                cursor.execute(query2)
                conn.commit()
            except:
                logging.error("Create Table Schema Failed!\n")

            cursor.close()
            conn.close()
            return True

        except:
            logging.info("Connecting Database Failed!\n")
            logging.info(str(sys.exc_info()))
            return False

    def __init__(self, eng = None, time_flag = True):

        #self.__connect2db_init()

        if eng == None:
            self.eng = connect2db(cfg.sql_dbname)
        else:
            self.eng = eng

        self.time_flag = time_flag
        self.Variable = []
        self.update_time = 0

        stored_tables = self.eng.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = \'rowstore\';")

        self.table_name = []
        for row in stored_tables:
            if row[0].startswith('rtable'):
                self.table_name.append(row[0])
            #else:
                #if '_t0' or '_t1' in row[0]:
                #    self.eng.execute(f"DROP TABLE IF EXISTS rowstore.\"{row[0]}\" CASCADE;")
                #    self.eng.execute(f"DROP VIEW IF EXISTS rowstore.\"{row[0]}\" CASCADE;")
        #exit()
        print("----Database Connection Created----")

    def InsertTable_Sperately(self, idi, new_table):

        if self.time_flag == True:
            start_time = timeit.default_timer()

        #if new_table.shape[1] > 1000:
        #    return
        
        if 'rtable' + str(idi) not in self.table_name:
            logging.info(new_table.shape)
            #logging.info(f"the new table to be stored {idi}!")
            if csv_flag:
                import os
                if new_table.shape[0] < 100:
                    new_table.to_sql(name='rtable' + str(idi), con=self.eng.connect(), schema='rowstore', if_exists='replace', index=False)
                else:
                    new_table.head().to_sql(name='rtable' + str(idi), con=self.eng.connect(), schema='rowstore', if_exists='replace', index=False)
                    # perform COPY test and print result
                    new_table_name = 'rtable' + str(idi)
                    csv_file_name = '/data/temp/temp_store_for_csv/' + new_table_name + '.csv'
                    new_table.to_csv(csv_file_name, index=False)

                    sql = '''
                    COPY {}
                    FROM '{}'
                    DELIMITER ',' CSV;
                    '''.format(cfg.sql_dbs + "." + new_table_name, csv_file_name)

                    cur = self.eng

                    cur.execute(
                        'TRUNCATE TABLE {}'.format(cfg.sql_dbs + "." + new_table_name))

                    cur.execute(sql)
                    cur.commit()
                    os.remove(csv_file_name)
                    #cur.close()
                    # Truncate the table in case you've already run the script before
            #else:
            self.table_name.append('rtable' + str(idi))
        
        #if new_table.shape[1] > 1000:
        #    return 
            new_table.to_sql(name='rtable' + str(idi), con=self.eng.connect(), schema=cfg.sql_dbs,\
                            if_exists='replace', index=False)
    

        #self.table_name.append('rtable' + str(idi))
        
        #del new_table 
        #if new_table.shape[1] > 1000:
        #    return
        
        #new_table.to_sql(name='rtable' + str(idi), con=self.eng.connect(), schema='rowstore',\
        #                         if_exists='replace', index=False)
        #self.table_name.append('rtable' + str(idi))
        
        self.Variable.append(idi)
    
        if self.time_flag == True:
            end_time = timeit.default_timer()
            logging.info(end_time - start_time)

    def Query_Tables_Times(self, vid):
        eng = psycopg2.connect("dbname=" + self.dbname + " user=\'" +
                               cfg.sql_name + "\' password=\'" + cfg.sql_password + "\'")
        cur = eng.cursor(cursor_factory=PreparingCursor)
        time_total = []
        for var in self.Variable:
            cur.prepare("select * from rowstore.rtable" + str(var) + ";")
            delta_time_array = []
            for i in range(10):
                start_time = timeit.default_timer()
                cur.execute()
                end_time = timeit.default_timer()
                delta_time = end_time - start_time
                delta_time_array.append(delta_time)

            delta_time_array = np.array(delta_time_array)
            time_total.append(np.mean(delta_time_array))
        cur.close()
        eng.close()
        return float(np.mean(time_total))

    def Query_Storage_Size(self):
        eng = self.__connect2db()
        mediate_tables = eng.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = \'rowstore\';")
        table_name = []
        storage_number = []
        for row in mediate_tables:
            table_name.append(('rowstore', row[0]))
        for sch, tn in table_name:
            try:
                table = pd.read_sql_table(tn, eng, schema=sch)
            except:
                logging.error(tn)
                continue
            storage_number.append(compute_table_size(table))
        eng.close()
        return float(sum(storage_number))

    def close_dbconnection(self):
        try:
            self.eng.close()
        except:
            return

    def Update_Data(self, idi, new_table, vid):
        start_time = timeit.default_timer()
        self.eng = self.__connect2db()
        nflg = True
        try:
            old_table = pd.read_sql_table('rtable'+str(idi), self.eng, schema='rowstore')
        except:
            old_table = None
            nflg = False

        if nflg == False:
            new_table.to_sql(name='rtable' + str(idi) + '_' + str(vid), con=self.eng, index=False, schema='rowstore', if_exists='replace')
        else:
            if new_table.equals(old_table) == False:
                new_table.to_sql(name = 'rtable' + str(idi) + '_' + str(vid), con = self.eng, index = False, schema = 'rowstore', if_exists = 'replace')
        end_time = timeit.default_timer()
        self.update_time = self.update_time + end_time - start_time
        self.eng.close()
