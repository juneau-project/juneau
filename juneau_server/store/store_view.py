import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import data_extension.config as cfg

def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if from_seen:
            if is_subselect(item):
                yield from extract_from_part(item)
            elif item.ttype is Keyword:
                return
            else:
                yield item
        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                yield identifier.get_name()
        elif isinstance(item, Identifier):
            yield item.get_name()
        # It's a bug to check for Keyword here, but in the example
        # above some tables names are identified as keywords...
        elif item.ttype is Keyword:
            yield item.value

def extract_tables(sql):
    stream = extract_from_part(sqlparse.parse(sql)[0])
    return list(extract_table_identifiers(stream))

class Store_View():

    def __init__(self, code_list, var_table_list, var_name_list, var_info_list):
        self.code_list = code_list
        self.table_name_list = var_table_list
        self.var_name_list = var_name_list
        self.var_info_list = var_info_list

    # support for pd.merge and pd.join
    @staticmethod
    def check_pd_dot(res, line, pattern, var_name_list, var_info_list):
        print(res, var_name_list)
        if len(res) == 1:
            res_string = res[0]
            str_contains_view_name = line.split(res_string)[0]

            if "=" in str_contains_view_name:
                view_name_str = str_contains_view_name.split("=")[0]
                if view_name_str in var_name_list:

                    view_name = view_name_str
                    view_id = view_name_str.index(view_name)

                    src_tables = []
                    src_index = []
                    res_string = res_string[len(pattern) + 1:-1]
                    res_string_list = res_string.split(",")
                    for c_tab in res_string_list:

                        if len(src_tables) == 2:
                            break

                        if c_tab == view_name:
                            continue
                        if c_tab in var_name_list:
                            src_tables.append(c_tab)
                            src_index.append(var_name_list.index(c_tab))
                        elif "[[" in c_tab:
                            c_tab = c_tab.split("[[")[0]
                            if c_tab in var_name_list:
                                src_tables.append(c_tab)
                                src_index.append(var_name_list.index(c_tab))
                        else:
                            continue
                    left_right_join_key = []
                    if len(src_tables) == 2:
                        left_table_schema = var_info_list[src_index[0]].value.columns.tolist()
                        right_table_schema = var_info_list[src_index[1]].value.columns.tolist()

                        if 'left_on' in res_string and 'right_on' in res_string:
                            left_key = re.findall(r"left_on=\'\.*?'", res_string)
                            right_key = re.findall(r"right_on=\'.*?\'", res_string)
                            left_keys = re.findall(r"left_on=\[.*?\]", res_string)
                            right_keys = re.findall(r"right_on=\[.*?\]", res_string)

                            if len(left_key) == 1 and len(right_key) == 1:
                                left_key_name = left_key[0].split("'")[1]
                                right_key_name = right_key[0].split("'")[1]
                                if left_key_name in left_table_schema and right_key_name in right_table_schema:
                                    left_right_join_key.append({left_key_name:right_key_name})

                            elif len(left_keys) == 1 and len(right_keys) == 1:
                                left_part = left_keys.split("'")[1:-1]
                                left_part = [l for l in left_part if l != ","]
                                right_part = right_keys.split("'")[1:-1]
                                right_part = [r for r in right_part if r != ","]
                                if len(left_part) == len(right_part):
                                    for lid in range(len(left_part)):
                                        if left_part[lid] in left_table_schema and right_part[lid] in right_table_schema:
                                            left_right_join_key.append({left_part[lid]: right_part[lid]})
                        elif 'on' in res_string:
                            key = re.findall(r"on=\'\.*?'", res_string)
                            keys = re.findall(r"on=\[.*?\]", res_string)
                            print(key)
                            if len(key) == 1:
                                print(key)
                                key_name = key[0].split("'")[1]
                                if key_name in left_table_schema and key_name in right_table_schema:
                                    left_right_join_key.append({key_name:key_name})
                            elif len(keys) == 1:
                                parts = keys.split("'")[1:-1]
                                keys = [l for l in parts if l != ","]
                                for lid in range(len(keys)):
                                    if keys[lid] in left_table_schema and keys[lid] in right_table_schema:
                                        left_right_join_key.append({keys[lid]: keys[lid]})
                            else:
                                intersection = [k for k in left_table_schema if k in right_table_schema]
                                for k in intersection:
                                    left_right_join_key.append({k:k})
                        return view_id, src_index, left_right_join_key, 'python'
        return -1, -1, [], 'python'

    # support for df.merge and df.join
    @staticmethod
    def check_dot(res, line, pattern, var_name_list, var_info_list):

        if len(res) == 1:
            res_string = res[0]
            str_contains_view_name = line.split(res_string)[0]

            if "=" in str_contains_view_name:
                view_name_str = str_contains_view_name.split("=")[0]
                if view_name_str in var_name_list:

                    view_name = view_name_str
                    view_id = view_name_str.index(view_name)

                    src_tables = []
                    src_index = []
                    left_table = line.split(res_string)[0].split("=")[1]
                    if left_table in var_name_list:
                        src_tables.append(left_table)
                        src_index.append(var_name_list.index(left_table))

                        res_string = res_string[len(pattern) + 1:-1]
                        res_string_list = res_string.split(",")
                        for c_tab in res_string_list:

                            if len(src_tables) == 2:
                                break

                            if c_tab == view_name:
                                continue
                            if c_tab in var_name_list:
                                src_tables.append(c_tab)
                                src_index.append(var_name_list.index(c_tab))
                            elif "[[" in c_tab:
                                c_tab = c_tab.split("[[")[0]
                                if c_tab in var_name_list:
                                    src_tables.append(c_tab)
                                    src_index.append(var_name_list.index(c_tab))
                            else:
                                continue
                        left_right_join_key = []
                        if len(src_tables) == 2:
                            left_table_schema = var_info_list[src_index[0]].value.columns.tolist()
                            right_table_schema = var_info_list[src_index[1]].value.columns.tolist()

                            if 'left_on' in res_string and 'right_on' in res_string:
                                left_key = re.findall(r"left_on=\'\.*?'", res_string)
                                right_key = re.findall(r"right_on=\'.*?\'", res_string)
                                left_keys = re.findall(r"left_on=\[.*?\]", res_string)
                                right_keys = re.findall(r"right_on=\[.*?\]", res_string)

                                if len(left_key) == 1 and len(right_key) == 1:
                                    left_key_name = left_key[0].split("'")[1]
                                    right_key_name = right_key[0].split("'")[1]
                                    if left_key_name in left_table_schema and right_key_name in right_table_schema:
                                        left_right_join_key.append({left_key_name: right_key_name})

                                elif len(left_keys) == 1 and len(right_keys) == 1:
                                    left_part = left_keys.split("'")[1:-1]
                                    left_part = [l for l in left_part if l != ","]
                                    right_part = right_keys.split("'")[1:-1]
                                    right_part = [r for r in right_part if r != ","]
                                    if len(left_part) == len(right_part):
                                        for lid in range(len(left_part)):
                                            if left_part[lid] in left_table_schema and right_part[
                                                lid] in right_table_schema:
                                                left_right_join_key.append({left_part[lid]: right_part[lid]})
                            elif 'on' in res_string:
                                key = re.findall(r"on=\'\.*?'", res_string)
                                keys = re.findall(r"on=\[.*?\]", res_string)
                                if len(key) == 1:
                                    key_name = key[0].split("'")[1]
                                    if key_name in left_table_schema and key_name in right_table_schema:
                                        left_right_join_key.append({key_name: key_name})
                                elif len(keys) == 1:
                                    parts = keys.split("'")[1:-1]
                                    keys = [l for l in parts if l != ","]
                                    for lid in range(len(keys)):
                                        if keys[lid] in left_table_schema and keys[lid] in right_table_schema:
                                            left_right_join_key.append({keys[lid]: keys[lid]})
                                else:
                                    intersection = [k for k in left_table_schema if k in right_table_schema]
                                    for k in intersection:
                                        left_right_join_key.append({k: k})
                            return view_id, src_index, left_right_join_key, 'python'
        return -1, -1, [], 'python'

    @staticmethod
    def check_sql(res, line, pattern, var_name_list, var_info_list):
        sql_query = res[0].split(pattern)[1][1:-1]
        src_tables = extract_tables(sql_query)
        src_index = []
        src_cnt = 0

        flg = True
        for tname in src_tables:
            if tname not in var_name_list:
                flg = False
                break
            else:
                src_index.append(var_name_list.index(tname))
                src_cnt += 1
        if not flg or src_cnt <= 1:
            return -1, -1, []

        res_string = res[0]
        str_contains_view_name = line.split(res_string)[0]

        view_id = -1
        if "=" in str_contains_view_name:
            view_name_str = str_contains_view_name.split("=")[0]
            if view_name_str in var_name_list:
                view_name = view_name_str
                view_id = view_name_str.index(view_name)
        if view_id == -1:
            return -1, -1, [], 'sql'
        else:
            return view_id, src_index, sql_query, 'sql'


    def detect_view_from_code(self):
        code_list = [line.strip("\n").strip(" ") for line in self.code_list]
        views = []
        for line in code_list:
            line = line.replace(" ", "")

            res1 = re.findall(r"pd.merge\(.*?\)", line)
            res2 = re.findall(r"pd.join\(.*?\)", line)
            res3 = re.findall(r".merge\(.*?\)", line)
            res4 = re.findall(r".join\(.*?\)", line)

            if len(res1) == 1:
                view_id, src_ids, join_keys, type = self.check_pd_dot(res1, line, "pd.merge", self.var_name_list, self.var_info_list)
                if view_id != -1:
                    views.append([view_id, src_ids, join_keys, type])
            elif len(res2) == 1:
                view_id, src_ids, join_keys, type = self.check_pd_dot(res2, line, "pd.join", self.var_name_list, self.var_info_list)
                if view_id != -1:
                    views.append([view_id, src_ids, join_keys, type])
            elif len(res3) == 1:
                view_id, src_ids, join_keys, type = self.check_dot(res3, line, ".merge", self.var_name_list, self.var_info_list)
                if view_id != -1:
                    views.append([view_id, src_ids, join_keys, type])
            elif len(res4) == 1:
                view_id, src_ids, join_keys, type = self.check_dot(res4, line, ".join", self.var_name_list, self.var_info_list)
                if view_id != -1:
                    views.append([view_id, src_ids, join_keys, type])
            else:
                continue
        return views

    def store_views(self, view_lists):
        view_statement_to_store = []
        for view in view_lists:
            view_id = view[0]
            src_ids = view[1]
            join_keys = view[2]
            p_type = view[3]

            view_statement = "CREATE VIEW " + cfg.sql_views + ".rtable" + self.table_name_list[view_id] + " AS "

            if p_type == 'python':
                query_statement = "SELECT * FROM "
                for tid in src_ids:
                    query_statement += cfg.sql_dbs + ".rtable" + self.table_name_list[tid] + ", "
                query_statement = query_statement.strip(", ") + " "
                query_statement += "WHERE "
                for key in join_keys:
                    key_pair = list(key.items())[0]
                    query_statement += cfg.sql_dbs + ".rtable" + self.table_name_list[src_ids[0]] + "." + key_pair[0] + \
                        " = " + cfg.sql_dbs + ".rtable" + self.table_name_list[src_ids[1]] + "." + key_pair[1] + " AND "
                query_statement = query_statement.strip(" AND ") + ";"
            else:
                for tid in src_ids:
                    join_keys = join_keys.replace(self.var_name_list[tid], self.table_name_list[tid])
                query_statement = join_keys

            view_statement_to_store.append(view_statement + query_statement)

        return view_statement_to_store













