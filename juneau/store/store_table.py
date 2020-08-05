# Copyright 2020 Juneau
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import logging
import timeit

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from juneau.config import config
from juneau.utils.cost_func import compute_table_size


class SeparateStorage:
    def __init__(self, dbname, time_flag=False):
        self.dbname = dbname
        self.__connect2db_init()
        self.eng = self.__connect2db()
        self.time_flag = time_flag
        self.variable = []
        self.update_time = 0

    def __connect2db(self):
        engine = create_engine(
            f"postgresql://{config.sql.name}:{config.sql.password}@localhost/{self.dbname}"
        )
        return engine.connect()

    def __connect2db_init(self):
        """
        FIXME: We are catching the exceptions but only logging. We do not
              reraise the exception. The code will continue to run without a database
              connection.
        """

        conn_string = (
            f"host='localhost' dbname='{self.dbname}' "
            f"user='{config.sql.name}' password='{config.sql.password}'"
        )

        logging.info(f"Connecting to database\n	->{conn_string}")

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
            except Exception as e:
                logging.error(f"Drop schema failed due to error {e}")
            try:
                cursor.execute(query2)
                conn.commit()
            except Exception as e:
                logging.error(f"Creation of schema failed due to error {e}")
            cursor.close()
            conn.close()
        except Exception as e:
            logging.info(f"Connection to database failed due to error {e}")

    def insert_table_separately(self, idi, new_table):

        if self.time_flag:
            start_time = timeit.default_timer()

        new_table.to_sql(
            name=f"rtable{idi}",
            con=self.eng,
            schema="rowstore",
            if_exists="fail",
            index=False,
        )
        self.variable.append(idi)

        if self.time_flag:
            end_time = timeit.default_timer()
            logging.info(end_time - start_time)

    def query_storage_size(self):
        eng = self.__connect2db()
        mediate_tables = eng.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'rowstore';"
        )
        table_name = []
        storage_number = []
        for row in mediate_tables:
            table_name.append(("rowstore", row[0]))
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

    def update_data(self, idi, new_table, vid):
        start_time = timeit.default_timer()
        self.eng = self.__connect2db()
        nflg = True
        try:
            old_table = pd.read_sql_table(f"rtable{idi}", self.eng, schema="rowstore")
        except:
            old_table = None
            nflg = False

        if not nflg:
            new_table.to_sql(
                name=f"rtable{idi}_{vid}",
                con=self.eng,
                index=False,
                schema="rowstore",
                if_exists="replace",
            )
        else:
            if not new_table.equals(old_table):
                new_table.to_sql(
                    name=f"rtable{idi}_{vid}",
                    con=self.eng,
                    index=False,
                    schema="rowstore",
                    if_exists="replace",
                )
        end_time = timeit.default_timer()
        self.update_time = self.update_time + end_time - start_time
        self.eng.close()
