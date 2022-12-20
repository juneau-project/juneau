import random
import math
import numpy as np
import ast
import logging
from data_extension.funclister import FuncLister


def sigmoid(x):
  return 1 / (1 + math.exp(-x))

def merge_add_dictionary(x,y):
    for v in x.keys():
        if v in y:
            x[v] = x[v] + y[v]
    for v in y.keys():
        if v not in x:
            x[v] = y[v]
    return x

def merge_two_lists(x,y):
    all_dict = {}
    for cid, col in enumerate(x[1]):
        all_dict[col] = x[0][cid]

    for cid, col in enumerate(y[1]):
        if col in all_dict and all_dict[col] != y[0][cid]:
            return None, None
        all_dict[col] = y[0][cid]

    sorted_keys = sorted(list(all_dict.keys()))
    sorted_values = []
    for ck in sorted_keys:
        sorted_values.append(all_dict[ck])
    return sorted_values, sorted_keys

def handle_superlong_nbname(nb_name):
    nb_name = nb_name.replace("-","")
    nb_name = nb_name.replace("_", "")
    nb_name = nb_name.replace(".ipynb", "")
    nb_name = nb_name.split("/")
    if len(nb_name) > 2:
        nb_name = nb_name[-2:]
    nb_name = "".join(nb_name)
    if len(nb_name) > 25:
        nb_name = nb_name[-25:]
    return nb_name

def handle_superlong_varname(var_name):
    nb_name = var_name.replace("-","")
    nb_name = nb_name.replace("_", "")
    if len(nb_name) > 25:
        nb_name = nb_name[:25]
    return nb_name

def containment_score(colA, colB):
    if len(colB) > 1000:
        colB = random.sample(colB, 1000)

    inter = np.intersect1d(colA, colB)
    if len(colA) == 0:
        return 0
    else:
        cscore = float(len(inter))/float(len(colA))
        return cscore

def unique_score(colA):
    return len(set(colA))/len(colA)

def jaccard_similarity(colA, colB):

    if min(len(colA), len(colB)) == 0:
        return 0

    if type(colA) == list:
        colA = np.array(colA)
    if type(colB) == list:
        colB = np.array(colB)
    union = len(np.union1d(colA, colB))
    inter = len(np.intersect1d(colA, colB))
    return float(inter)/float(union)


def sigmoid(x):
    return 1 / (1 + math.exp(-x))


def last_line_var(varname, code):
    ret = 0
    code = code.split("\n")

    for id, i in enumerate(code):
        if '=' not in i:
            continue

        j = i.split('=')
        j = [t.strip(" ") for t in j]

        if varname in j[0]:
            if varname == j[0][-len(varname):]:
                ret = id + 1
    return ret


def parse_code(code_list):

    test = FuncLister()
    all_code = ""
    line2cid = {}

    lid = 1
    fflg = False

    for cid, cell in enumerate(code_list):
        # logging.info(cid)
        # logging.info(cell)
        cell = cell.replace("\\n", "\n")

        if "\n" in cell:
            codes = cell.split("\n")
        else:
            codes = [cell]

        # logging.info(codes)

        new_codes = []
        for code in codes:

            if code[:3].lower() == 'def':
                fflg = True
                continue

            temp_code = code.strip(" ")
            temp_code = temp_code.strip("\t")

            if temp_code[:6].lower() == 'return':
                fflg = False
                continue

            code = code.strip("\n")
            code = code.strip(" ")
            code = code.split("\"")
            code = "'".join(code)
            code = code.split("\\")
            code = "".join(code)

            if len(code) == 0:
                continue
            if code[0] == '%':
                continue
            if code[0] == '#':
                continue
            if code[0] == " ":
                continue
            if code == "":
                continue
            if code == "\n":
                continue

            try:
                ast.parse(code)
                if fflg == False:
                    new_codes.append(code)
                    line2cid[lid] = cid
                    lid = lid + 1
            except:
                logging.info("error with " + code)

        all_code = all_code + '\n'.join(new_codes) + '\n'

    all_code = all_code.strip("\n")
    all_code = all_code.split("\n")
    all_code = [t for t in all_code if t != ""]
    all_code = "\n".join(all_code)

    tree = ast.parse(all_code)
    test.visit(tree)
    return test.dependency, line2cid, all_code

