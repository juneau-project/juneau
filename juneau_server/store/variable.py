from juneau_server.utils.utils import clean_notebook_name

class Var_Info:
    def __init__(self, var_name, var_cid, nb_name, var_code_str):

        self.var_name = var_name
        self.cid = var_cid

        self.nb = clean_notebook_name(nb_name)
        self.store_name = f"{self.cid}_{self.var_name}_{self.nb}"
        self.code_str = var_code_str.strip("\\n#\\n")
        self.code_list = self.code_str.split("\\n#\\n")
        self.neo4j_prev_node = None

    def get_prev_node(self, nb_cell_id_node):

        for cid in range(self.cid - 1, -1, -1):
            if cid in nb_cell_id_node[self.nb]:
                self.neo4j_prev_node = nb_cell_id_node[self.nb][cid]
                break

    def get_value(self, output):
        self.value = output