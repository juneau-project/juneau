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

import logging
from difflib import SequenceMatcher

import numpy as np

TUNING_PARAM = 0.9
SAMPLE_SIZE = 1000


def evaluate_key(vta, vtb, vint, tableA, tableB):
    selectA = float(len(vint)) / float(len(vta))
    selectB = float(len(vint)) / float(len(vtb))

    dicta_temp = np.unique(vta, return_counts=True)
    dicta = {}
    for idx, i in enumerate(dicta_temp[0]):
        dicta[i] = dicta_temp[1][idx]

    dictb_temp = np.unique(vtb, return_counts=True)
    dictb = {}
    for idx, i in enumerate(dictb_temp[0]):
        dictb[i] = dictb_temp[1][idx]

    scorea = 0
    for key in dicta.keys():
        if key in dictb:
            scorea = scorea + float(dictb[key]) * float(dicta[key]) / float(len(vta))

    scoreb = 0
    for key in dictb.keys():
        if key in dicta:
            scoreb = scoreb + float(dicta[key]) * float(dictb[key]) / float(len(vtb))

    sizeab = 0
    for v in vint:
        sizeab = sizeab + len(v)

    sizea = float(compute_table_size(tableA) * selectA)
    sizeb = float(compute_table_size(tableB) * selectB)
    sizeab = sizea + sizeb

    size_intb = 0
    for v in vtb:
        size_intb = size_intb + len(v)

    lower_bound = (
        float(sizeab)
        - float(sizea * scoreb)
        - float(sizeb * scorea)
        + float(selectB * scorea * size_intb)
    )
    return lower_bound


def return_jkpair4tables(tableA, tableB):
    matching = []
    Mpair = {}
    MpairR = {}

    shmal = len(tableA.columns.values)
    shmbl = len(tableB.columns.values)

    # Should improve how to sample
    for i in range(shmal):
        colA = tableA[tableA.columns.values.tolist()[i]].fillna(0).values

        if tableA.shape[0] > SAMPLE_SIZE:
            colA = tableA[tableA.columns.values.tolist()[i]].sample(SAMPLE_SIZE).values

        nameA = tableA.columns.values.tolist()[i].split("_")[0].lower()

        if "index" in nameA:
            continue

        for j in range(shmbl):

            nameB = tableB.columns.values.tolist()[j].split("_")[0].lower()

            if "index" in nameB:
                continue

            if SequenceMatcher(None, nameA, nameB).ratio() < 0.5:
                continue

            colB = tableB[tableB.columns.values.tolist()[j]].fillna(0).values

            if tableB.shape[0] > SAMPLE_SIZE:
                colB = (
                    tableB[tableB.columns.values.tolist()[j]].sample(SAMPLE_SIZE).values
                )

            interAB = np.intersect1d(colA, colB)

            if min(len(colA), len(colB)) == 0:
                matching.append((i, j, float(0)))
            else:
                score_match = float(len(interAB)) / float(
                    min(len(set(colA)), len(set(colB)))
                )
                matching.append((i, j, score_match))

    matching = sorted(matching, key=lambda d: d[2], reverse=True)

    for i in range(len(matching)):
        if matching[i][2] < 0.5:
            break
        else:
            if (
                tableA.columns.values.tolist()[matching[i][0]] not in Mpair
                and tableB.columns.values.tolist()[matching[i][1]] not in MpairR
            ):
                Mpair[
                    tableA.columns.values.tolist()[matching[i][0]]
                ] = tableB.columns.values.tolist()[matching[i][1]]
                MpairR[
                    tableB.columns.values.tolist()[matching[i][1]]
                ] = tableA.columns.values.tolist()[matching[i][0]]

    vta = [",".join(map(str, itm)) for itm in tableA[Mpair.keys()].values]
    vtb = [",".join(map(str, itm)) for itm in tableB[Mpair.values()].values]
    vint = [t for t in vta if t in vtb]

    if min(len(vta), len(vtb)) == 0:
        Fvalue = 0
    else:
        Fvalue = evaluate_key(vta, vtb, vint, tableA, tableB)

    return Mpair, Fvalue


def return_union_row(tableA, tableB):
    union_flag = True
    tableA_strA = []
    tva = tableA.columns.values.tolist()
    index_flg = True
    for i in tva:
        if "index" in i:
            index_flg = False
            break

    for idx, row in tableA.iterrows():
        if not index_flg:
            row_str = ",".join(map(str, row.values.tolist()[1:]))
        else:
            row_str = ",".join(map(str, row.values.tolist()))

        tableA_strA.append(row_str)
    tableB_strB = []
    for idx, row in tableB.iterrows():
        row_str = ",".join(map(str, row.values.tolist()))
        tableB_strB.append(row_str)

    lenA = len(tableA_strA)
    lenB = len(tableB_strB)

    if lenA >= lenB:  # B is new table
        setA = set(tableA_strA)
        setB = set(tableB_strB)
        for i in setB:
            if i not in setA:
                union_flag = False
                break

        if not union_flag:
            return -1, None
        else:
            return compute_table_size(tableB), -1
    else:
        setA = set(tableA_strA)
        setB = set(tableB_strB)
        for i in setA:
            if i not in setB:
                union_flag = False
                break
        if not union_flag:
            return -1, None
        else:
            return compute_table_size(tableA), 1


def pickup_best_table(tablestorage, tableA, cost_flag):
    if len(tablestorage.items()) == 0:
        return None

    rank_table_to_merge = []
    tablestorage = sorted(tablestorage.items(), key=lambda d: int(d[0].split("_")[0]))
    tablestorage2 = []
    tablename = []
    for idt, table_in in tablestorage:
        tablestorage2.append(table_in)
        tablename.append(idt)
    tablestorage = tablestorage2

    for idt, table_in in enumerate(tablestorage):
        tableB = table_in
        join_key, benefit = return_jkpair4tables(tableB, tableA)
        rank_table_to_merge.append((idt, join_key, benefit))

    rank_table_to_merge = sorted(rank_table_to_merge, key=lambda d: d[2], reverse=True)

    if cost_flag:
        return (
            tablename[rank_table_to_merge[0][0]],
            rank_table_to_merge[0][1],
            rank_table_to_merge[0][2],
        )
    else:
        basetablestore = min(
            [
                100,
                sum(tableA.count()),
                sum(tablestorage[rank_table_to_merge[0][0]].count()),
            ]
        )
        if (
            len(rank_table_to_merge[0][1]) == 0
            or rank_table_to_merge[0][2] < basetablestore
        ):
            return None
        else:
            return (
                tablename[rank_table_to_merge[0][0]],
                rank_table_to_merge[0][1],
                rank_table_to_merge[0][2],
            )


def pickup_best_table_union_row(tablestorage, tableA):
    rank_table_to_merge = []
    tablestorage2 = []
    tablename = []
    for idt, table_in in tablestorage.items():
        tablestorage2.append(table_in)
        tablename.append(idt)
    tablestorage = tablestorage2

    for idt, table_in in enumerate(tablestorage):
        tableB = table_in
        benefit, direct = return_union_row(tableB, tableA)
        rank_table_to_merge.append((idt, benefit, direct))

    rank_table_to_merge = sorted(rank_table_to_merge, key=lambda d: d[1], reverse=True)

    if rank_table_to_merge[0][1] == -1:
        return None, rank_table_to_merge[0][1]
    else:
        if rank_table_to_merge[0][2] == 1:
            return tablename[rank_table_to_merge[0][0]], rank_table_to_merge[0][1]
        else:
            return tablename[rank_table_to_merge[0][0]], -1 * rank_table_to_merge[0][1]


def compute_table_size(table):
    storage_cost = 0
    for rindex, row in table.count().iteritems():
        attr = table[rindex].dtype
        if attr == int:
            storage_cost = storage_cost + float(row * 4)
        elif attr == float:
            storage_cost = storage_cost + float(row * 8)
        elif attr == str:
            for v in table[rindex].values.tolist():
                storage_cost = storage_cost + len(v)
        else:
            for v in table[rindex].values.tolist():
                if type(v) == int:
                    storage_cost = storage_cost + 4
                elif type(v) == float:
                    storage_cost = storage_cost + 8
                elif type(v) == str:
                    storage_cost = storage_cost + len(v)
                elif v is None:
                    continue
                else:
                    storage_cost = storage_cost + len(v)

    return storage_cost


def compute_table_size_unique_each_col(table):
    storage_cost = 0
    for col in table.columns.values.tolist():
        value = np.unique(table[col].values)
        attr = table[col].dtype
        if attr == int:
            storage_cost = storage_cost + float(len(value) * 4)
        elif attr == float:
            storage_cost = storage_cost + float(len(value) * 8)
        elif attr == str:
            for v in value.tolist():
                storage_cost = storage_cost + len(v)
        else:
            for v in value.tolist():
                if type(v) == int:
                    storage_cost = storage_cost + 4
                elif type(v) == float:
                    storage_cost = storage_cost + 8
                else:
                    storage_cost = storage_cost + len(v)
    return storage_cost


def cost_func_store_with_union_row(table, table_df_full):
    table_id, storage_benefit = pickup_best_table_union_row(table_df_full, table)
    query_time = 0.01 * float(table.shape[0])
    if storage_benefit > 0:
        storage_cost = (
            compute_table_size(table) - abs(storage_benefit) + 4 * table.shape[0]
        )
    else:
        storage_cost = (
            compute_table_size(table) - abs(storage_benefit) + 4 * table.shape[0]
        )

    if storage_cost < 0 and table_id is not None:
        logging.info(str(compute_table_size(table)))
        logging.info(str(storage_benefit))
        logging.info(table)
        logging.info(str(table_df_full[table_id]))

    return (
        query_time * float(1 - TUNING_PARAM) + float(storage_cost) * TUNING_PARAM,
        table_id,
        storage_benefit,
    )


def cost_func_store_seperately(table):
    query_time = 0.01 * float(table.shape[0])
    storage_cost = compute_table_size(table)
    return query_time * float(1 - TUNING_PARAM) + float(storage_cost) * TUNING_PARAM


def cost_func_store_lineage(running_time, code):
    query_time = running_time
    storage_cost = len(code)
    return query_time * float(1 - TUNING_PARAM) + float(storage_cost) * TUNING_PARAM


def cost_func_store_with_join_row(table, table_df_full):
    table_id, join_key, storage_benefit = pickup_best_table(table_df_full, table, True)
    query_time = 0.01 * float(table.shape[0])
    storage_cost = compute_table_size(table) - storage_benefit + 4 * table.shape[0]
    return (
        query_time * float(1 - TUNING_PARAM) + float(storage_cost) * TUNING_PARAM,
        table_id,
        join_key,
    )


def cost_func_store_with_join_col(table, table_df_full):
    table_id, join_key, storage_benefit = pickup_best_table(table_df_full, table, True)
    query_time = 0.01 * float(sum(table.count()))
    storage_cost = float(sum(table.count()) * 4) + compute_table_size_unique_each_col(
        table
    )
    return (
        query_time * float(1 - TUNING_PARAM) + float(storage_cost),
        table_id,
        join_key,
    )


def choose_storage_strategy(table, table_df_full, table_view_full, running_time, code):
    if len(table_df_full.items()) == 0:
        return 0, None, None
    cost = [(0, cost_func_store_seperately(table))]
    if code is not None:
        cost.append((1, cost_func_store_lineage(running_time, code)))
    return_by_join_row = cost_func_store_with_join_row(table, table_df_full)
    cost.append((2, return_by_join_row[0]))
    return_by_union_row = cost_func_store_with_union_row(table, table_view_full)
    if return_by_union_row[1] is not None:
        cost.append((3, return_by_union_row[0]))

    cost = sorted(cost, key=lambda d: d[1])
    if cost[0][0] == 0:
        return 0, None, None
    elif cost[0][0] == 1:
        return 1, None, None
    elif cost[0][0] == 2:
        return 2, return_by_join_row[1], return_by_join_row[2]
    elif cost[0][0] == 3:
        return 3, return_by_union_row[1], return_by_union_row[2]
