
class Var_Info:
    def __init__(self, var_name, var_cid, var_store_name, nb_name, var_value, var_code_str, var_code_list, prev_node):
        self.var_name = var_name
        self.store_name = var_store_name
        self.code_str = var_code_str
        self.code_list = var_code_list
        self.value = var_value
        self.neo4j_prev_node = prev_node
        self.cid = var_cid
        self.nb = nb_name

