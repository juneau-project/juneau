import codecs
import re
import usaddress
import numpy as np
import os
import pickle

this_dir, this_filename = os.path.split(__file__)

def check_last_name(input_str, input_list, thres = 0.8):
    if input_list.dtype != object:
        return False

    if str(input_str) == "surname" or str(input_str) == "last name":
        return True

    with codecs.open(this_dir + "/data/last_name_List100.p", "rb") as infp:
        data = pickle.load(infp)
        infp.close()
        #data = [t.strip("\n").lower() for t in data]

    #logging.info(input_list)
    cint = 0
    input_list = input_list.dropna().str.lower().tolist()
    for val in data:
        if val in input_list:
            cint += 1
    #input_list = [t.lower() for t in input_list.dropna().tolist()]

    #cint = 0
    #for val in input_list:
    #    if val in data:
    #        cint += 1

    if len(input_list) == 0:
        return False

    cscore = float(cint)/float(len(input_list))


    if cscore > thres:
        return True
    else:
        return False

def check_first_name(input_str, input_list, thres = 0.8):
    if input_list.dtype != object:
        return False

    if str(input_str) == "given name" or str(input_str) == "first name":
        return True

    with codecs.open(this_dir + "/data/first_name_List600.p", "rb") as infp:
        data = pickle.load(infp)
        infp.close()


    cint = 0
    input_list = input_list.dropna().str.lower().tolist()
    for val in data:
        if val in input_list:
            cint += 1

    #input_list = [t.lower() for t in input_list.dropna().tolist()]

#    cint = 0
#    for val in input_list:
#        if val in data:
#            cint += 1

    if len(input_list) == 0:
        return False

    cscore = float(cint)/float(len(input_list))

    if cscore > thres:
        return True
    else:
        return False

def check_name(input_str, input_list, thres = 0.64):
    if input_list.dtype != object:
        return False

    if str(input_str).lower() == "name":
        return True

    with codecs.open(this_dir + "/data/last_name_List100.p", "rb") as infp:
        data = pickle.load(infp)
        infp.close()
        last_name = data

    with codecs.open(this_dir + "/data/first_Name_List600.p", "rb") as infp:
        data = pickle.load(infp)
        infp.close()
        first_name = data

    input_List = [re.split(" |\.",t.strip("\n")) for t in input_list.dropna().tolist()]
    #logging.info(input_List)
    split_List = [t for t in input_list if t!= None and len(t) == 2]

    cint = 0
    for fval, lval in split_List:
        if fval.lower() == "mr" or fval.lower() == "miss" or fval.lower() == "mrs" or fval.lower() in first_name:
            if lval.lower() in last_name:
                cint += 1

    if len(split_List) == 0:
        return False

    cscore = float(cint) / float(len(split_List))

    if cscore > thres:
        return True
    else:
        return False

def check_gender(input_str, input_list, thres = 1):
    if input_list.dtype != object:
        return False

    if str(input_str).lower() == "gender" or str(input_str).lower() == "sex":
        return True

    input_set = set(input_list.tolist())
    gender_set1 = set(['m','f'])
    gender_set2 = set(['male', 'female'])
    #logging.info(input_list.dtype)
    if len(input_set.difference(gender_set1)) == 0 or len(input_set.difference(gender_set2)) == 0:
        return True
    else:
        return False

def check_age(input_str, input_list, thres = 20):
    if input_list.dtype != np.int64:
        return False

    if str(input_str).lower() == "age":
        return True

    if len(set(input_list.tolist())) < thres:
        return False

    for val in input_list.tolist():
        if type(val) is not int:
            return False

    max_int = max(input_list.tolist())
    min_int = min(input_list.tolist())

    if max_int > 120:
        return False
    if min_int <= 1:
        return False


    return True

def check_email(input_str, input_list, thres = 0.8):

    if input_list.dtype != object:
        return False

    if str(input_str).lower() == "email":
        return True

    cint = 0

    regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'

    for val in input_list.tolist():
        if "@" not in str(val):
            continue
        if (re.search(regex, str(val))):
            cint += 1

    if len(input_list) == 0:
        return False

    cscore = float(cint)/float(len(input_list))

    if cscore > thres:
        return True
    else:
        return False

def check_ssn(input_str, input_list, thres = 0.8):
    if input_list.dtype != object:
        return False

    if str(input_str).lower() == "ssn":
        return True

    cint = 0
    for val in input_list.tolist():
        if re.match(r"^(?!000|666)[0-8][0-9]{2}-(?!00)[0-9]{2}-(?!0000)[0-9]{4}$", str(val)):
            cint += 1

    if len(input_list) == 0:
        return False

    cscore = float(cint)/float(len(input_list))

    if cscore > thres:
        return True
    else:
        return False

def check_address(input_str, input_list, thres = 0.8):
    if input_list.dtype != object:
        return False

    if str(input_str) == "address":
        return True

    cint = 0
    for val in input_list.tolist():
        if len(str(val)) > 200:
            continue
        parse_res = usaddress.parse(str(val))
        pdict = {}
        for a,b in parse_res:
            pdict[b] = a
        if 'AddressNumber' in pdict and 'StreetName' in pdict and 'StateName' in pdict:
            cint += 1

    if len(input_list) == 0:
        return False

    cscore = float(cint)/float(len(input_list))

    if cscore > thres:
        return True
    else:
        return False
