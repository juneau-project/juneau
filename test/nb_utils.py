import sys
import subprocess
import pandas as pd
import ast
import pickle
import logging
import json
import numpy as np
import random
from jupyter_client import find_connection_file
from jupyter_client import MultiKernelManager, BlockingKernelClient, KernelClient
from data_extension.jupyter import exec_code, exec_ipython, request_var
import re
from sqlalchemy import create_engine
from data_extension.config import sql_dbs, sql_dbname, sql_password, sql_name, sql_host
from data_extension.store.store_list import StoreList


def subprocess_cmd(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    proc_stdout = process.communicate()  # [0].strip()
    return proc_stdout
#
# def run_cell_first(kid, code, stdflag):
#
#     pre_code = "%reset -f"
#     exec_code(kid, None, pre_code)
#     print("finish reset")
#     #print(code)
#     return run_cell(kid, code, stdflag)
#
#
# def run_cell(kid, code, stdflag):
#     return exec_code(kid, None, code)


# def find_variables(self, search_var, kernel_id):
#         # Make sure we have an engine connection in case we want to read
#     if kernel_id not in self.done:
#         o2, err = exec_ipython( \
#             kernel_id, search_var, 'connect_psql')
#             # o2, err = data_extension.jupyter.connect_psql(kernel_id, search_var)
#         self.done[kernel_id] = {}
#         logging.info(o2)
#         logging.info(err)
#
#     logging.info('Looking up variable ' + search_var)
#
#
#     output, error = request_var(kernel_id, search_var)
#         #output, error = data_extension.jupyter.exec_ipython(kernel_id, search_var, 'print_var')
#     logging.info('Returned with variable value.')
#
#     if error != "" or output == "" or output is None:
#         sta = False
#         return sta, error
#     else:
#         try:
#             var_obj = pd.read_json(output, orient='split')
#             sta = True
#         except:
#             logging.info('Parsing: ' + output)
#             sta = False
#             var_obj = None
#
#     return sta, var_obj


def run_cell_first(kid, code, stdflag):
    # if stdflag == True:
    # print("Running following code:\n")
    # print(code)

    msg_id = subprocess.Popen(['python', 'run_cell_reset.py', str(kid), code], stderr=subprocess.PIPE,
                              stdout=subprocess.PIPE)
    # end_time = timeit.timeit()
    output, error = msg_id.communicate()
    # print(code)
    # output, error = subprocess_cmd('python' + ' run_cell_reset.py ' + str(kid) + ' ' + code)

    if sys.version[0] == '3':
        output = output.decode('utf-8')
        error = error.decode('utf-8')

    if stdflag:
        print("\n*** THE OUTPUT ***")
        print(output)
        print(error)
        print("******************\n")

    msg_id.stdout.close()

    if error != "":
        return False, error  # end_time - start_time)

    else:
        output = output.split('time:')[1]
        return True, float(output)  # end_time - start_time)


def run_cell(kid, code, stdflag):
    # if stdflag:
    #    print("Running following code:\n")
    #    print(code)

    # start_time = timeit.timeit()
    msg_id = subprocess.Popen(['python', 'run_cell.py', str(kid), code], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    # end_time = timeit.timeit()

    output, error = msg_id.communicate()
    # output, error = subprocess_cmd('python' + ' run_cell_reset.py ' + str(kid) + ' ' + code)

    if sys.version[0] == '3':
        output = output.decode('utf-8')
        error = error.decode('utf-8')

    if stdflag:
        print("\n*** THE OUTPUT ***")
        print(output)
        print(error)
        print("******************\n")

    msg_id.stdout.close()

    if error != "":
        return False, error  # , end_time - start_time)
    else:
        output = output.split('time:')[1]
        return True, float(output)  # , end_time - start_time)


def print_variable_value(kid, tn):
    msg_id = subprocess.Popen(['python', 'print_var.py', str(kid), tn],
                              stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    output, error = msg_id.communicate()

    if sys.version[0] == '3':
        output = output.decode('utf-8')
        error = error.decode('utf-8')

    msg_id.stdout.close()

    if error != "":
        sta = False
        print(error)
        return sta, error
    else:
        sta = True
        return sta, output


def find_variables(kid, stdflag):
    if stdflag:
        print("Finding Variables:\n")

    msg_id = subprocess.Popen(['python', 'get_var_cell.py', str(kid)],
                              stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    output, error = msg_id.communicate()

    if sys.version[0] == '3':
        output = output.decode("utf-8")
        error = error.decode("utf-8")

    # os.kill(msg_id.pid)

    if stdflag:
        print("*** THE OUTPUT ***")
        #print(output)
        print(error)
        #print(error == "")

    msg_id.stdout.close()

    if error != "":
        sta = False
        return sta, error
    else:
        sta = True
        output = output.split('\n')
        del output[1]

        df_var = []
        nd_var = []
        ls_var = []

        output = [v.split() for v in output]
        output = [v for v in output if len(v) > 2]
        output_schema = output[0]
        output_temp = []

        for idi, i in enumerate(output):
            if i[1] == 'DataFrame':
                df_var.append(i[0])
            elif i[1] == 'ndarray':
                nd_var.append(i[0])
            elif i[1] == 'list':
                ls_var.append(i[0])
            elif i[1] == 'dict':
                ls_var.append(i[0])
            else:
                continue
#                output_temp.append(i)

        output = output_temp

        for var in df_var:
            msg_id = subprocess.Popen(['python', 'print_var.py', str(kid), var], stderr=subprocess.PIPE,
                                      stdout=subprocess.PIPE)
            opt, error = msg_id.communicate()
            # p_status = msg_id.wait()
            msg_id.stdout.close()
            if error.decode("utf-8") == "":
                try:
                    var_obj = pd.read_json(opt.decode("utf-8"), orient="split")
                    output.append([var, 'DataFrame', var_obj, np.nan])
                except:
                    logging.info('error 1')
                    logging.info(sys.exc_info())
                    # exit()
            else:
                logging.info("error 2")
                logging.info(error)
            # print(var_obj)

        for var in nd_var:
            msg_id = subprocess.Popen(['python', 'print_var.py', str(kid), var], stderr=subprocess.PIPE,
                                      stdout=subprocess.PIPE)
            opt, error = msg_id.communicate()
            # p_status = msg_id.wait()
            msg_id.stdout.close()
            if error.decode("utf-8") == "":
                try:
                    var_obj = pd.DataFrame(json.loads(opt.decode("utf-8")))
                    var_obj.rename(str, axis='columns', inplace=True)
                    output.append([var, 'Matrix', var_obj, np.nan])
                except:
                    logging.info(sys.exc_info())

            # var_obj = pd.read_sql_table(var, con = eng, schema = sql_dbs)
            # var_obj = pd.read_csv('/Users/yizhang/PycharmProjects/new_juneau/notebook_data_extension/data_extension/test/var_dir/' + var + ".csv")

            else:
                logging.info(error)
            # print(var_obj)

        for var in ls_var:
            msg_id = subprocess.Popen(['python', 'print_var.py', str(kid), var], stderr=subprocess.PIPE,
                                      stdout=subprocess.PIPE)
            opt, error = msg_id.communicate()
            # p_status = msg_id.wait()
            msg_id.stdout.close()
            if error.decode("utf-8") == "":
                try:
                    var_obj = json.loads(opt.decode("utf-8"))
                    if type(var_obj) is dict:
                        var_obj = [var_obj]

                    # if the list is not comprised of dictionaries
                    if not type(var_obj[0]).__name__ == 'dict':
                        var_obj = pd.DataFrame(var_obj)
                        var_obj.rename(str, axis='columns', inplace=True)
                        output.append([var, 'List', var_obj, np.nan])
                        continue

                    sl = StoreList()

                    with open(f"/data2/temp_storage4tables/{var}_{random.randint(0,100)}.json", "w") as outfp:
                        json.dump(var_obj, outfp)
                        outfp.close()

                    logging.info("Parsing Json Started!")
                    parsed_tables, join_data = sl.parse_tables_json_list(var_obj, 'root')
                    logging.info("Parsing Json Ended")
                    # append each view
                    output.append([var, 'View', join_data, parsed_tables])
                except:
                    logging.info("error occurs when parsing list")
                    logging.info(sys.exc_info())
                    pickle.dump(var_obj, open(f"error_store/error_list_{random.randint(0, 10000)}.json","wb"))

                    # exit()
            else:
                logging.info(error)

        # eng.close()

        output = [v for v in output if len(v) == 4]

        if len(output) > 0:
            output_df = pd.DataFrame(output)
 #       if len(output_df.columns.values.tolist()) != 0:
#           print(output_df.columns)
#            print(output_schema)
            output_df.columns = output_schema + ['Base Tables']
        else:
            output_df = None
        return sta, output_df


def analyze_cell(kernel_id, codes, runflag, stdflag, var_type, eflg):
    if runflag:
        res, out = run_cell_first(kernel_id, codes, stdflag)
    else:
        res, out = run_cell(kernel_id, codes, stdflag)

    if not eflg:
        if not res:
            print("Error in the cell!\n")
            print("Code: ", codes)
            print(out)
            return None
        else:
            varflag, var_df = find_variables(kernel_id, stdflag)
            if var_df is None:
                print("No tables detected!\n")
                return None
            if not varflag:
                print("Error happened when searching variables!\n")
                return None
            elif len(var_df.columns.values.tolist()) == 0:
                return None
            else:
                query_var = var_df.loc[var_df['Type'].isin(var_type)]['Variable'].values.tolist()
                cell_df = []
                # print query_var
                # print(var_df)
                for idv, row in var_df.iterrows():
                    # for idv, var in enumerate(query_var):
                    #    if var_df['Type'].iloc[idv] == 'DataFrame':
                    if row['Variable'] not in query_var:
                        continue
                    if row['Type'] == 'DataFrame':
                        cell_df.append((row['Variable'], row['Data/Info'], row['Base Tables'], out, codes))
                        continue
                    if row['Type'] == 'Matrix' or row['Type'] == 'List':
                        cell_df.append((row['Variable'], row['Data/Info'], row['Base Tables'], out, codes))
                        continue
                    if row['Type'] == 'View':
                        cell_df.append((row['Variable'], row['Data/Info'], row['Base Tables'], out, codes))

                    # flg, val = print_variable_value(kernel_id, row['Variable'])
                    # if not flg:
                    #     print("Error happened when searching variable value!\n")
                    #     return None
                    # else:
                    #     try:
                    #         val_df = pd.DataFrame(ast.literal_eval(val))
                    #         val_df.columns = ['col' + str(t) for t in val_df.columns.tolist()]
                    #         cell_df.append((row['Variable'], val_df, out, codes))
                    #     except:
                    #         continue

                if len(cell_df) > 0:
                    cell_df = pd.DataFrame(cell_df)
                    # print(cell_df)
                    cell_df.columns = ['Variable', 'DataFrame', 'Base Tables', 'RunningTime', 'Code']
                    return cell_df
                else:
                    return None