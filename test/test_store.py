import logging
from os import listdir
from data_extension.test.store_func import StoreNotebooks, remote_flag
from data_extension.config import sql_dbname
from data_extension.connect.connection import Connection
from data_extension.store.store_lshe import StoreLSHE

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)  # configure root logger

#common_dir = '/Users/peterchan/Desktop/dataset/'
store_var_type = ['DataFrame', 'Matrix', 'List', 'View']



if True:
    common_dir = '/data/juneau_data/'
else:
    common_dir = '/Users/yizhang/PycharmProjects/notebook_management/evaluation/related_table_search/'

#common_dir='/Users/yizhang/PycharmProjects/juneau21/notebook_data_extension/data_extension/test/'
kernel_id = 2897965

# store_var_type = ['DataFrame','ndarray','List']
# std_flag = False

def get_notebooks(input_dirs):
    nb = []

    for dir in input_dirs:
        full_dir = common_dir + dir + '/notebooks'
        for file_name in listdir(full_dir):
            if file_name.endswith('.ipynb'):
                nb.append(f'{full_dir}/{file_name}')

    return nb


dir = [
       common_dir + 'add_data/default/',
       common_dir + 'add_data/copy1/',
       common_dir + 'add_data/copy2/',
       common_dir + 'add_data/copy3/',
       common_dir + 'add_data/copy4/',  # 4

       common_dir + 'data_clen/default/',
       common_dir + 'data_clen/copy1/',
       common_dir + 'data_clen/copy2/',
       common_dir + 'data_clen/copy3/',
       common_dir + 'data_clen/copy4/',  # 9

       common_dir + 'feat_alter/default/',
       common_dir + 'feat_alter/copy1/',
       common_dir + 'feat_alter/copy2/',
       common_dir + 'feat_alter/copy3/',
       common_dir + 'feat_alter/copy4/',#14

       common_dir + 'medical_cost/',
       common_dir + 'ds/',
       common_dir + 'mental_health/',
       common_dir + 'titantic/',
       common_dir + 'ml/',
       common_dir + "other/",
       common_dir + 'joinable/', #21

       common_dir + "airbnb/default/",
       common_dir + "airbnb/copy1/",
       common_dir + "airbnb/copy2/",
       common_dir + "airbnb/copy3/",
       common_dir + "airbnb/copy4/", #26

        common_dir + "olympics/default/", #27
        common_dir + "olympics/copy1/",
        common_dir + "olympics/copy2/",
        common_dir + "olympics/copy3/",
        common_dir + "olympics/copy4/", #31

        common_dir + "ufc/default/", #32
        common_dir + "ufc/copy1/",
        common_dir + "ufc/copy2/",
        common_dir + "ufc/copy3/",
        common_dir + "ufc/copy4/",#36,

        common_dir + "wuhan_coronavirus/default/", #37
        common_dir + "wuhan_coronavirus/copy1/",
        common_dir + "wuhan_coronavirus/copy2/",
        common_dir + "wuhan_coronavirus/copy3/",
        common_dir + "wuhan_coronavirus/copy4/" #41
]

def store_tables(spid):
#     nb_name1 = dir[0] + 'sentiment-analysis_solu.ipynb'#0
#     nb_name2 = dir[0] + 'sentiment-analysis_temp.ipynb'#1
#     nb_name3 = dir[1] + 'sentiment-analysis_solu.ipynb'#0
#     nb_name4 = dir[1] + 'sentiment-analysis_temp.ipynb'#1
#     nb_name5 = dir[2] + 'sentiment-analysis_solu.ipynb'#0
#     nb_name6 = dir[2] + 'sentiment-analysis_temp.ipynb'#1
#     nb_name7 = dir[3] + 'sentiment-analysis_solu.ipynb'#0
#     nb_name8 = dir[3] + 'sentiment-analysis_temp.ipynb'#1
#     nb_name9 = dir[4] + 'sentiment-analysis_solu.ipynb'#0
#     nb_name10 = dir[4] + 'sentiment-analysis_temp.ipynb'#1
#
#     nb0 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5, nb_name6, nb_name7, nb_name8, nb_name9, nb_name10] #10
#     #nb0 = [nb_name1]

    nb_name1 = dir[5] + 'houseprice-data-cleaning-solu.ipynb'#2
    nb_name2 = dir[5] + 'houseprice-data-cleaning-temp.ipynb'#3
    nb_name3 = dir[6] + 'houseprice-data-cleaning-solu.ipynb'#2
    nb_name4 = dir[6] + 'houseprice-data-cleaning-temp.ipynb'#3
    nb_name5 = dir[7] + 'houseprice-data-cleaning-solu.ipynb'#2
    nb_name6 = dir[7] + 'houseprice-data-cleaning-temp.ipynb'#3
    nb_name7 = dir[8] + 'houseprice-data-cleaning-solu.ipynb'#2
    nb_name8 = dir[8] + 'houseprice-data-cleaning-temp.ipynb'#3
    nb_name9 = dir[9] + 'houseprice-data-cleaning-solu.ipynb'#2
    nb_name10 = dir[9] + 'houseprice-data-cleaning-temp.ipynb'#3
    #nb1 = [nb_name1]
    nb1 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5, nb_name6, nb_name7, nb_name8, nb_name9, nb_name10] #10

    nb_name1 = dir[10] + 'titanic_good_solu.ipynb'#4
    nb_name2 = dir[10] + 'titanic_query_temp.ipynb'#5
    nb_name3 = dir[11] + 'titanic_good_solu.ipynb'#4
    nb_name4 = dir[11] + 'titanic_query_temp.ipynb'#5
    nb_name5 = dir[12] + 'titanic_good_solu.ipynb'#4
    nb_name6 = dir[12] + 'titanic_query_temp.ipynb'#5
    nb_name7 = dir[13] + 'titanic_good_solu.ipynb'#4
    nb_name8 = dir[13] + 'titanic_query_temp.ipynb'#5
    nb_name9 = dir[14] + 'titanic_good_solu.ipynb'#4
    nb_name10 = dir[14] + 'titanic_query_temp.ipynb'#5

    nb2 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5, nb_name6, nb_name7, nb_name8, nb_name9, nb_name10]

    nb_name1 = dir[15] + "kernel10.ipynb" #6
    nb_name2 = dir[15] + "kernel11.ipynb" #7
    nb_name3 = dir[15] + "kernel20.ipynb" #8
    nb_name4 = dir[15] + "kernel21.ipynb" #9
    nb_name5 = dir[15] + "kernel30.ipynb" #10
    nb_name6 = dir[15] + "kernel31.ipynb" #11
#    nb3 = [nb_name1]
    nb3 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5, nb_name6]
#
    nb_name1 = dir[16] + "kernel10.ipynb" #12
    nb_name2 = dir[16] + "kernel11.ipynb" #13
    nb_name3 = dir[16] + "kernel20.ipynb" #14
    nb_name4 = dir[16] + "kernel21.ipynb" #15
    nb_name5 = dir[16] + "kernel30.ipynb" #16
    nb_name6 = dir[16] + "kernel31.ipynb" #17
    nb4 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5, nb_name6]
    #nb4 = [nb_name1]
#
#     nb_name1 = dir[17] + "kernel10.ipynb" #18
#     #nb_name2 = dir[5] + "kernel11.ipynb" #19
# #    nb_name3 = dir[5] + "kernel20.ipynb" #20
# #    nb_name4 = dir[5] + "kernel21.ipynb" #21
#     nb_name5 = dir[17] + "kernel30.ipynb" #19
#     #nb_name6 = dir[5] + "kernel31.ipynb" #23
#     nb5 = [nb_name1, nb_name5]
#     #nb5 = [nb_name6]
#     #nb5 = [nb_name1]
#
#     #nb5 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5, nb_name6]
#
#     nb_name1 = dir[18] + "kernel10.ipynb" #20
#     nb_name2 = dir[18] + "kernel11.ipynb" #21
#     nb_name3 = dir[18] + "kernel20.ipynb" #22
#     nb_name4 = dir[18] + "kernel21.ipynb" #23
#     nb_name5 = dir[18] + "kernel30.ipynb" #24
#     nb_name6 = dir[18] + "kernel31.ipynb" #25
#     nb6 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5, nb_name6]
# #    nb6 = [nb_name1]
#
    nb_name1 = dir[19] + "kernel10.ipynb" #26
    nb_name2 = dir[19] + "kernel11.ipynb" #27
    nb_name3 = dir[19] + "kernel20.ipynb" #28
    nb_name4 = dir[19] + "kernel21.ipynb" #29
    nb_name5 = dir[19] + "kernel30.ipynb" #30
    nb_name6 = dir[19] + "kernel31.ipynb" #31

    #nb7 = [nb_name1]
    nb7 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5, nb_name6]
#
#     nb_name1 = dir[20] + "stacked-regressions1.ipynb" #32
#     nb_name2 = dir[20] + "stacked-regressions2.ipynb" #33
#     nb_name3 = dir[20] + "stacked-regressions3.ipynb" #34
#
# #    nb8 = [nb_name1]
#     nb8 = [nb_name1, nb_name2, nb_name3]
#
#     nb_name1 = dir[21] + "instacart0.ipynb"#35
#     nb_name2 = dir[21] + "instacart1.ipynb"#36
#     #nb9 = [nb_name1]
#     nb9 = [nb_name1, nb_name2]
#
    nb_name1 = dir[22] + "airbnb_kernel1.ipynb"
    nb_name2 = dir[22] + "airbnb_kernel2.ipynb"
    nb_name3 = dir[22] + "airbnb_kernel3.ipynb"
    nb_name4 = dir[22] + "airbnb-model_kernel1.ipynb"
    nb_name5 = dir[22] + "airbnb-model_kernel2.ipynb"
    nb_name6 = dir[22] + "airbnb-model_kernel3.ipynb"
    nb_name7 = dir[22] + "airbnb-exploration_kernel1.ipynb"
    nb_name8 = dir[22] + "airbnb-exploration_kernel2.ipynb"
    nb_name9 = dir[22] + "airbnb-exploration_kernel3.ipynb"

    nb_name10 = dir[23] + "airbnb_kernel1.ipynb"
    nb_name11 = dir[23] + "airbnb_kernel2.ipynb"
    nb_name12 = dir[23] + "airbnb_kernel3.ipynb"
    nb_name13 = dir[23] + "airbnb-model_kernel1.ipynb"
    nb_name14 = dir[23] + "airbnb-model_kernel2.ipynb"
    nb_name15 = dir[23] + "airbnb-model_kernel3.ipynb"
    nb_name16 = dir[23] + "airbnb-exploration_kernel1.ipynb"
    nb_name17 = dir[23] + "airbnb-exploration_kernel2.ipynb"
    nb_name18 = dir[23] + "airbnb-exploration_kernel3.ipynb"

    nb_name19 = dir[24] + "airbnb_kernel1.ipynb"
    nb_name20 = dir[24] + "airbnb_kernel2.ipynb"
    nb_name21 = dir[24] + "airbnb_kernel3.ipynb"
    nb_name22 = dir[24] + "airbnb-model_kernel1.ipynb"
    nb_name23 = dir[24] + "airbnb-model_kernel2.ipynb"
    nb_name24 = dir[24] + "airbnb-model_kernel3.ipynb"
    nb_name25 = dir[24] + "airbnb-exploration_kernel1.ipynb"
    nb_name26 = dir[24] + "airbnb-exploration_kernel2.ipynb"
    nb_name27 = dir[24] + "airbnb-exploration_kernel3.ipynb"

    nb_name28 = dir[25] + "airbnb_kernel1.ipynb"
    nb_name29 = dir[25] + "airbnb_kernel2.ipynb"
    nb_name30 = dir[25] + "airbnb_kernel3.ipynb"
    nb_name31 = dir[25] + "airbnb-model_kernel1.ipynb"
    nb_name32 = dir[25] + "airbnb-model_kernel2.ipynb"
    nb_name33 = dir[25] + "airbnb-model_kernel3.ipynb"
    nb_name34 = dir[25] + "airbnb-exploration_kernel1.ipynb"
    nb_name35 = dir[25] + "airbnb-exploration_kernel2.ipynb"
    nb_name36 = dir[25] + "airbnb-exploration_kernel3.ipynb"

    nb_name37 = dir[26] + "airbnb_kernel1.ipynb"
    nb_name38 = dir[26] + "airbnb_kernel2.ipynb"
    nb_name39 = dir[26] + "airbnb_kernel3.ipynb"
    nb_name40 = dir[26] + "airbnb-model_kernel1.ipynb"
    nb_name41 = dir[26] + "airbnb-model_kernel2.ipynb"
    nb_name42 = dir[26] + "airbnb-model_kernel3.ipynb"
    nb_name43 = dir[26] + "airbnb-exploration_kernel1.ipynb"
    nb_name44 = dir[26] + "airbnb-exploration_kernel2.ipynb"
    nb_name45 = dir[26] + "airbnb-exploration_kernel3.ipynb"

    nb10 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5,
            nb_name6, nb_name7, nb_name8, nb_name9, nb_name10,
            nb_name11, nb_name12, nb_name13, nb_name14, nb_name15,
            nb_name16, nb_name17, nb_name18, nb_name19, nb_name20,
            nb_name21, nb_name22, nb_name23, nb_name24, nb_name25,
            nb_name26, nb_name27, nb_name28, nb_name29, nb_name30,
            nb_name31, nb_name32, nb_name33, nb_name34, nb_name35,
            nb_name36, nb_name37, nb_name38, nb_name39, nb_name40,
            nb_name41, nb_name42, nb_name43, nb_name44, nb_name45]

    nb_name1 = dir[27] + "basic_kernel1.ipynb"
    nb_name2 = dir[27] + "basic_kernel2.ipynb"
    nb_name3 = dir[27] + "turkish_kernel1.ipynb"
    nb_name4 = dir[27] + "turkish_kernel2.ipynb"

    nb_name5 = dir[28] + "basic_kernel1.ipynb"
    nb_name6 = dir[28] + "basic_kernel2.ipynb"
    nb_name7 = dir[28] + "turkish_kernel1.ipynb"
    nb_name8 = dir[28] + "turkish_kernel2.ipynb"

    nb_name9 = dir[29] + "basic_kernel1.ipynb"
    nb_name10 = dir[29] + "basic_kernel2.ipynb"
    nb_name11 = dir[29] + "turkish_kernel1.ipynb"
    nb_name12 = dir[29] + "turkish_kernel2.ipynb"

    nb_name13 = dir[30] + "basic_kernel1.ipynb"
    nb_name14 = dir[30] + "basic_kernel2.ipynb"
    nb_name15 = dir[30] + "turkish_kernel1.ipynb"
    nb_name16 = dir[30] + "turkish_kernel2.ipynb"

    nb_name17 = dir[31] + "basic_kernel1.ipynb"
    nb_name18 = dir[31] + "basic_kernel2.ipynb"
    nb_name19 = dir[31] + "turkish_kernel1.ipynb"
    nb_name20 = dir[31] + "turkish_kernel2.ipynb"

    nb11 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5,
            nb_name6, nb_name7, nb_name8, nb_name9, nb_name10,
            nb_name11, nb_name12, nb_name13, nb_name14, nb_name15,
            nb_name16, nb_name17, nb_name18, nb_name19, nb_name20]

    nb_name1 = dir[32] + "efficiency_kernel1.ipynb"
    nb_name2 = dir[32] + "efficiency_kernel2.ipynb"
    nb_name3 = dir[32] + "efficiency_kernel3.ipynb"
    nb_name4 = dir[32] + "ufc-eda_kernel1.ipynb"
    nb_name5 = dir[32] + "ufc-eda_kernel2.ipynb"
    nb_name6 = dir[32] + "ufc-eda_kernel3.ipynb"
    nb_name7 = dir[32] + "ufcb_kernel1.ipynb"
    nb_name8 = dir[32] + "ufcb_kernel2.ipynb"
    nb_name9 = dir[32] + "ufcb_kernel3.ipynb"

    nb_name10 = dir[33] + "efficiency_kernel1.ipynb"
    nb_name11 = dir[33] + "efficiency_kernel2.ipynb"
    nb_name12 = dir[33] + "efficiency_kernel3.ipynb"
    nb_name13 = dir[33] + "ufc-eda_kernel1.ipynb"
    nb_name14 = dir[33] + "ufc-eda_kernel2.ipynb"
    nb_name15 = dir[33] + "ufc-eda_kernel3.ipynb"
    nb_name16 = dir[33] + "ufcb_kernel1.ipynb"
    nb_name17 = dir[33] + "ufcb_kernel2.ipynb"
    nb_name18 = dir[33] + "ufcb_kernel3.ipynb"

    nb_name19 = dir[34] + "efficiency_kernel1.ipynb"
    nb_name20 = dir[34] + "efficiency_kernel2.ipynb"
    nb_name21 = dir[34] + "efficiency_kernel3.ipynb"
    nb_name22 = dir[34] + "ufc-eda_kernel1.ipynb"
    nb_name23 = dir[34] + "ufc-eda_kernel2.ipynb"
    nb_name24 = dir[34] + "ufc-eda_kernel3.ipynb"
    nb_name25 = dir[34] + "ufcb_kernel1.ipynb"
    nb_name26 = dir[34] + "ufcb_kernel2.ipynb"
    nb_name27 = dir[34] + "ufcb_kernel3.ipynb"

    nb_name28 = dir[35] + "efficiency_kernel1.ipynb"
    nb_name29 = dir[35] + "efficiency_kernel2.ipynb"
    nb_name30 = dir[35] + "efficiency_kernel3.ipynb"
    nb_name31 = dir[35] + "ufc-eda_kernel1.ipynb"
    nb_name32 = dir[35] + "ufc-eda_kernel2.ipynb"
    nb_name33 = dir[35] + "ufc-eda_kernel3.ipynb"
    nb_name34 = dir[35] + "ufcb_kernel1.ipynb"
    nb_name35 = dir[35] + "ufcb_kernel2.ipynb"
    nb_name36 = dir[35] + "ufcb_kernel3.ipynb"

    nb_name37 = dir[36] + "efficiency_kernel1.ipynb"
    nb_name38 = dir[36] + "efficiency_kernel2.ipynb"
    nb_name39 = dir[36] + "efficiency_kernel3.ipynb"
    nb_name40 = dir[36] + "ufc-eda_kernel1.ipynb"
    nb_name41 = dir[36] + "ufc-eda_kernel2.ipynb"
    nb_name42 = dir[36] + "ufc-eda_kernel3.ipynb"
    nb_name43 = dir[36] + "ufcb_kernel1.ipynb"
    nb_name44 = dir[36] + "ufcb_kernel2.ipynb"
    nb_name45 = dir[36] + "ufcb_kernel3.ipynb"

    nb12 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5,
           nb_name6, nb_name7, nb_name8, nb_name9, nb_name10,
           nb_name11, nb_name12, nb_name13, nb_name14, nb_name15,
           nb_name16, nb_name17, nb_name18, nb_name19, nb_name20,
           nb_name21, nb_name22, nb_name23, nb_name24, nb_name25,
           nb_name26, nb_name27, nb_name28, nb_name29, nb_name30,
           nb_name31, nb_name32, nb_name33, nb_name34, nb_name35,
           nb_name36, nb_name37, nb_name38, nb_name39, nb_name40,
           nb_name41, nb_name42, nb_name43, nb_name44, nb_name45]

#
#
    nb_name1 = dir[37] + "ncov_kernel1.ipynb"
    nb_name2 = dir[37] + "ncov_kernel2.ipynb"
    nb_name3 = dir[37] + "ncov_kernel3.ipynb"
    nb_name4 = dir[37] + "wuhan_kernel1.ipynb"
    nb_name5 = dir[37] + "wuhan_kernel2.ipynb"
    nb_name6 = dir[37] + "wuhan_kernel3.ipynb"

    nb_name7 = dir[38] + "ncov_kernel1.ipynb"
    nb_name8 = dir[38] + "ncov_kernel2.ipynb"
    nb_name9 = dir[38] + "ncov_kernel3.ipynb"
    nb_name10 = dir[38] + "wuhan_kernel1.ipynb"
    nb_name11 = dir[38] + "wuhan_kernel2.ipynb"
    nb_name12 = dir[38] + "wuhan_kernel3.ipynb"

    nb_name13 = dir[39] + "ncov_kernel1.ipynb"
    nb_name14 = dir[39] + "ncov_kernel2.ipynb"
    nb_name15 = dir[39] + "ncov_kernel3.ipynb"
    nb_name16 = dir[39] + "wuhan_kernel1.ipynb"
    nb_name17 = dir[39] + "wuhan_kernel2.ipynb"
    nb_name18 = dir[39] + "wuhan_kernel3.ipynb"

    nb_name19 = dir[40] + "ncov_kernel1.ipynb"
    nb_name20 = dir[40] + "ncov_kernel2.ipynb"
    nb_name21 = dir[40] + "ncov_kernel3.ipynb"
    nb_name22 = dir[40] + "wuhan_kernel1.ipynb"
    nb_name23 = dir[40] + "wuhan_kernel2.ipynb"
    nb_name24 = dir[40] + "wuhan_kernel3.ipynb"

    nb_name25 = dir[41] + "ncov_kernel1.ipynb"
    nb_name26 = dir[41] + "ncov_kernel2.ipynb"
    nb_name27 = dir[41] + "ncov_kernel3.ipynb"
    nb_name28 = dir[41] + "wuhan_kernel1.ipynb"
    nb_name29 = dir[41] + "wuhan_kernel2.ipynb"
    nb_name30 = dir[41] + "wuhan_kernel3.ipynb"

    nb13 = [nb_name1, nb_name2, nb_name3, nb_name4, nb_name5,
        nb_name6, nb_name7, nb_name8, nb_name9, nb_name10,
        nb_name11, nb_name12, nb_name13, nb_name14, nb_name15,
        nb_name16, nb_name17, nb_name18, nb_name19, nb_name20,
        nb_name21, nb_name22, nb_name23, nb_name24, nb_name25,
        nb_name26, nb_name27, nb_name28, nb_name29, nb_name30]
#
#
#     # nb13 = [nb_name25, nb_name26, nb_name27, nb_name28, nb_name29, nb_name30]
#
#     # nb = nb2 + nb7 + nb3 + nb4 + nb5 + nb6 + nb8 + nb9
#     # nb = nb12 + nb11 + nb10 + nb0 + nb1 + nb2 + nb7 + nb3 + nb4 + nb5 + nb6 + nb8 + nb9
#     # nb = nb13 + nb12 + nb11 + nb10 + nb0 + nb1 + nb2 + nb7 + nb3 + nb4 + nb5 + nb6 + nb8 + nb9
#
#     nb = nb0
#     # more notebooks
#     nb = nb1
#     #nb13 = [nb_name25, nb_name26, nb_name27, nb_name28, nb_name29, nb_name30]
#
    #nb = nb10 + nb2 + nb1 + nb11 + nb12 + nb13
    #nb =  nb0 + nb1 + nb2 + nb7 + nb3 + nb4 + nb5 + nb6 + nb8 + nb9
#     #nb = nb12 + nb11 + nb10 + nb0 + nb1 + nb2 + nb7 + nb3 + nb4 + nb5 + nb6 + nb8 + nb9
#     #nb = nb13 + nb12 + nb11 + nb10 + nb0 + nb1 + nb2 + nb7 + nb3 + nb4 + nb5 + nb6 + nb8 + nb9

    #nb = get_notebooks(['dblp'])#['nyc/NYC_Property_Sales'])
    #print(nb[0])
    #exit()
    #nb = get_notebooks(['flight', 'airbnb_open', 'dblp'])
    #nb = get_notebooks(['flight'])

    #nb = get_notebooks(['dblp'])
    #nb = get_notebooks(['nyc/NYC_PARKING_TICKETS', 'nyc/PASSNYC', 'nyc/Vehicle_Collisions'])
    nb = get_notebooks(['world_bank', 'stocks'])
    #nb = get_notebooks(['nyc/PASSNYC'])
    #nb = get_notebooks(['temp_data'])
    #nb = nb[3:]

    if len(nb) == 1:
        test2_seper(nb[0], True, None, 0)
    else:
        test_class = test2_seper(nb[0], False, None, 0)
        for i in range(1, len(nb) - 1):
             test_class = test2_seper(nb[i], False, test_class, i)
        test_class = test2_seper(nb[-1], True, test_class, len(nb))


def test2_seper(nb_name, cflg, test_class, vid):
    if test_class == None:
        test2 = StoreNotebooks(kernel_id, store_var_type, sql_dbname)
    else:
        test2 = test_class

    logging.info("Handling " + nb_name)
    test2.analyze_and_store(nb_name)

    if cflg == True:
        test2.close_dbconnection()
    return test2


# store_tables(2)

# conn = Connection()
# store_lshe = StoreLSHE(conn.psql_engine())
# store_lshe.store()
store_tables(2)

# conn = Connection()
# store_lshe = StoreLSHE(conn.psql_engine())
# store_lshe.store()
