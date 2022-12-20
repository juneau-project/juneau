import networkx as nx
import numpy as np
from sqlalchemy import create_engine
from data_extension.store_prov import Store_Lineage
import data_extension.config as cfg


class SearchMatrix:
    type_mappings = {"int": "integer", "float": "double precision", "str": "text", "int64": "bigint", "str_": "text"}

    def __init__(self, psql_engine):
        self.psql_engine = psql_engine

    def store(self, sql_schema, matrix_name: str, matrix: np.ndarray):
        elem_postgres_type = self.type_mappings[type(matrix[0][0]).__name__]
        dimension, num_ele = matrix.shape

        table_schema = ", ".join(f'd{i} {elem_postgres_type}' for i in range(num_ele))
        create_table_string = f'CREATE TABLE IF NOT EXISTS {sql_schema}.{matrix_name} ({table_schema});'

        insertion_string = ''
        for row in matrix:
            if elem_postgres_type == 'text':
                insertion_string += 'INSERT INTO VALUES ('
                insertion_string += ', '.join(f"'{ele}'" for ele in row)
                insertion_string += ');'
            else:
                insertion_string += f"INSERT INTO VALUES ({', '.join(str(ele) for ele in row)});"

        with self.psql_engine.connect() as connection:
            connection.excute(create_table_string + insertion_string)
        connection.close()

    def query(self, matrix_name, matrix, dependen, line2cid, nb_name, var_types):
        dimension, num_ele = matrix.shape

        if dimension > 2:
            raise Exception('Error: Matrices beyond 2D cannot be queried')

        origin_df = []

        # run DFS on the variable dependency graph to trace back the origin data frames
        store_prov_db_class = Store_Lineage(self.psql_engine)
        g, _ = store_prov_db_class.generate_graph_from_sql(dependen, line2cid, nb_name)
        g_dfs = nx.dfs_tree(g, source=matrix_name)
        for node in list(g_dfs.nodes):
            if g_dfs.out_degree[node] == 0 and var_types.get(g.nodes[node]['var']) == 'DataFrame':
                var_name = g.nodes[node]['var']
                origin_df.append(var_name)

        if not len(origin_df):
            self.store('some_schema', matrix_name, matrix)
            return


conn_string = f"postgresql://{cfg.sql_username}:{cfg.sql_password}@{cfg.sql_host}/{cfg.sql_dbname}"
engine = create_engine(conn_string)
arr = np.array([['hello', 'world', 'hello', 'world'], ['hello', 'world', 'hello', 'world']])
sm = SearchMatrix(engine)
sm.store("matrix_table", arr)
