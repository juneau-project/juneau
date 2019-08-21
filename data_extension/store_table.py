import psycopg2
import timeit
import pandas as pd
import numpy as np

from sqlalchemy import create_engine

from data_extension.cost_func import compute_table_size

import data_extension.config as cfg

import logging
logging.basicConfig(level=logging.DEBUG)


class Store_Seperately:

    def __connect2db(self):
        engine = create_engine("postgresql://" + cfg.sql_name + ":" +
                               cfg.sql_password + "@localhost/" + self.dbname)
        return engine.connect()

    def __connect2db_init(self):
        # Define our connection string
        conn_string = "host='localhost' dbname=\'" + self.dbname + "\' user=\'" + cfg.sql_name + \
                      "\' password=\'" + cfg.sql_password + "\'"

        # logging.info the connection string we will use to connect
        logging.info("Connecting to database\n	->%s" % (conn_string))

        # get a connection, if a connect cannot be made an exception will be raised here
        try:
            # conn.cursor will return a cursor object, you can use this cursor to perform queries
            conn = psycopg2.connect(conn_string)
            logging.info("Connecting Database Succeeded!\n")
            cursor = conn.cursor()
            query1 = "DROP SCHEMA IF EXISTS rowstore CASCADE;"
            query2 = "CREATE SCHEMA rowstore;"

            try:
                cursor.execute(query1)
                conn.commit()
            except:
                logging.error("Drop Schema Failed!\n")
            try:
                cursor.execute(query2)
                conn.commit()
            except:
                logging.error("Create Schema Failed!\n")
            cursor.close()
            conn.close()
            return True

        except:
            logging.info("Connecting Database Failed!\n")
            return False

    def __init__(self, dbname, time_flag):

        self.dbname = dbname
        self.__connect2db_init()
        logging.info("heresss")
        self.eng = self.__connect2db()
        self.time_flag = time_flag
        self.Variable = []
        self.update_time = 0

    def InsertTable_Sperately(self, idi, new_table):

        if self.time_flag == True:
            start_time = timeit.default_timer()

        new_table.to_sql(name='rtable' + str(idi), con=self.eng, schema='rowstore', if_exists='fail', index=False)
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
        self.eng.close()

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