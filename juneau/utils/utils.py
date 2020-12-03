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

"""
A series of utility functions used throughout Juneau.
"""

from sys import getsizeof
import ast
import logging
import math
import numpy as np
from .funclister import FuncLister
import random

def clean_notebook_name(nb_name):
    """
    Cleans the notebook name by removing the .ipynb extension, removing hyphens,
    and removing underscores.
    Example:
        >>> nb = "My-Awesome-Notebook.ipynb"
        >>> # Receive a PUT with `nb`
        >>> print(clean_notebook_name(nb))
        >>> # prints "MyAwesomeNotebook"
    Returns:
        A string that is cleaned per the requirements above.
    """
    nb_name = nb_name.replace(".ipynb", "").replace("-", "").replace("_", "")
    nb_name = nb_name.split("/")
    if len(nb_name) > 2:
        nb_name = nb_name[-2:]
    nb_name = "".join(nb_name)
    return nb_name[-25:]


def _getsizeof(x):
    """
    Gets the size of a variable `x`. Amended version of sys.getsizeof
    which also supports ndarray, Series and DataFrame.
    """
    if type(x).__name__ in ["ndarray", "Series"]:
        return x.nbytes
    elif type(x).__name__ == "DataFrame":
        return x.memory_usage().sum()
    else:
        return getsizeof(x)

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
        #logging.info(cid)
        #logging.info(cell)
        cell = cell.replace("\\n", "\n")

        if "\n" in cell:
            codes = cell.split("\n")
        else:
            codes = [cell]

        #logging.info(codes)

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
                logging.info("error with "  + code)



        all_code = all_code + '\n'.join(new_codes) + '\n'

    all_code = all_code.strip("\n")
    all_code = all_code.split("\n")
    all_code = [t for t in all_code if t != ""]
    all_code = "\n".join(all_code)

    tree = ast.parse(all_code)
    test.visit(tree)
    return test.dependency, line2cid, all_code


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

def containment_score(colA, colB):
    if len(colB) > 1000:
        colB = random.sample(colB, 1000)

    inter = np.intersect1d(colA, colB)
    if len(colA) == 0:
        return 0
    else:
        cscore = float(len(inter))/float(len(colA))
        return cscore


def sigmoid(x):
  return 1 / (1 + math.exp(-x))