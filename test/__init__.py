
from data_extension.connect.connection import Connection
import sys
connection = Connection()
engine = connection.psql_engine()


# def initialize_general():
#     with engine.begin() as psql_connection:
#         #query_string = "DROP SCHEMA IF EXISTS rowstore CASCADE;"
#         #query_string += "CREATE SCHEMA rowstore;"
#         #query_string += "DROP SCHEMA IF EXISTS graph_model CASCADE;"
#         #query_string += "CREATE SCHEMA graph_model;"
#         #query_string += "CREATE TABLE graph_model.dependen (view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"
#         #query_string += "CREATE TABLE graph_model.line2cid (view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"
#         #query_string += "CREATE TABLE graph_model.lastliid (view_id VARCHAR(1000), view_cmd VARCHAR(10000000));"
#         #query_string += "DROP SCHEMA IF EXISTS nb_provenance CASCADE;"
#     query_string += "CREATE SCHEMA nb_provenance;"
#     query_string += "CREATE TABLE nb_provenance.code_dict (code VARCHAR(1000), cell_id INTEGER);"
#     query_string += "DROP SCHEMA IF EXISTS table_profile CASCADE;"
#     query_string += "CREATE SCHEMA table_profile;"
#     query_string += "DROP SCHEMA IF EXISTS column_index CASCADE;"
#     query_string += "CREATE SCHEMA column_index;"
#     query_string += "DROP SCHEMA IF EXISTS topk_ra_states CASCADE;"
#     query_string += "CREATE SCHEMA IF NOT EXISTS topk_ra_states;"
#     query_string += "DROP SCHEMA IF EXISTS topk_sa_states CASCADE;"
#     query_string += "CREATE SCHEMA IF NOT EXISTS topk_sa_states;"
#     return query_string


def initialize_lshe():
    query_string = 'DROP SCHEMA IF EXISTS test_sig CASCADE;'
    query_string += 'CREATE SCHEMA test_sig;'
    query_string += 'DROP SCHEMA IF EXISTS test_hash CASCADE;'
    query_string += 'CREATE SCHEMA test_hash;'
    query_string += 'DROP SCHEMA IF EXISTS utils CASCADE;'
    query_string += 'CREATE SCHEMA utils;'
    query_string += 'CREATE TABLE utils.view_table ("view_name" varchar, "base_tables" varchar, "join_keys" varchar);'
    query_string += 'CREATE TABLE utils.count_table ("schema_string" varchar, "table_string" varchar, "row_count" integer, "col_count" integer);'
    query_string += 'CREATE TABLE utils.indexed_table ("schema_string" varchar, "table_string" varchar);'
    query_string += 'CREATE TABLE utils.optkl_table ("maxK" integer, "numHash" integer, x integer, q integer, t double precision, "optK" integer, "optL" integer);'
    query_string += 'CREATE TABLE utils.partition_table ("part_idx" integer, "upper" integer, "num_domain" integer);'
    query_string += 'CREATE TABLE utils.transpose_table("_schema" text, "_tbl" text, "_key" text, "_val" text, "_hashed" boolean);'
    query_string += 'CREATE TABLE utils.all_notebooks("name" text);'
    return query_string

def check_initialization_lshe():
    query_string = 'CREATE SCHEMA IF NOT EXISTS test_sig;'
    query_string += 'CREATE SCHEMA IF NOT EXISTS test_hash;'
    query_string += 'CREATE SCHEMA IF NOT EXISTS utils;'
    query_string += 'CREATE TABLE IF NOT EXISTS utils.view_table ("view_name" varchar, "base_tables" varchar, "join_keys" varchar);'
    query_string += 'CREATE TABLE IF NOT EXISTS utils.count_table ("schema_string" varchar, "table_string" varchar, "row_count" integer, "col_count" integer);'
    query_string += 'CREATE TABLE IF NOT EXISTS utils.indexed_table ("schema_string" varchar, "table_string" varchar);'
    query_string += 'CREATE TABLE IF NOT EXISTS utils.optkl_table ("maxK" integer, "numHash" integer, x integer, q integer, t double precision, "optK" integer, "optL" integer);'
    query_string += 'CREATE TABLE IF NOT EXISTS utils.partition_table ("part_idx" integer, "upper" integer, "num_domain" integer);'
    query_string += 'CREATE TABLE IF NOT EXISTS utils.transpose_table("_schema" text, "_tbl" text, "_key" text, "_val" text, "_hashed" boolean);'
    query_string += 'CREATE TABLE IF NOT EXISTS utils.all_notebooks("name" text);'
    return query_string


try:
    with engine.begin() as psql_connection:
        #query = initialize_general()
        #psql_connection.execute(query)
        #query = initialize_lshe()

        query = check_initialization_lshe()
        psql_connection.execute(query)

        print("----POSTGRES INITIALIZATION IS SUCCESSFUL----")
except Exception:
    print("Connecting Failed!\n")
    print(sys.exc_info())

# graph = Graph("http://neo4j:yizhang@localhost:7474/db/data")
# graph = connection.graph_engine()
# graph.delete_all()
# print("----NEO4J INITIALIZATION IS SUCCESSFUL----")
