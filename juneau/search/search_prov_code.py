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
TODO: Explain what this module does.
"""

import os
import pickle
import queue


class SearchProv:
    def __star_dis(self, listA, listB):
        listA = sorted(listA)
        listB = sorted(listB)
        lenA = len(listA)
        lenB = len(listB)
        i = 0
        j = 0
        interset = []
        while i < lenA and j < lenB:
            if listA[i] == listB[j]:
                interset.append(listA[i])
                i += 1
                j += 1
            elif listA[i] < listB[j]:
                i += 1
            elif listA[i] > listB[j]:
                j += 1
            else:
                print(i, j)
        dist = abs(lenA - lenB) + max(lenA, lenB) - len(interset)
        return dist

    def Star_Mapping_Dis(self, query, root):
        mapo = {}
        mapr = {}
        distance_pair = []
        for i in query.keys():
            stara = query[i].values()
            for j in root.keys():
                starb = root[j].values()
                simAB = self.__star_dis(stara, starb)
                distance_pair.append((i, j, simAB))
        distance_pair = sorted(distance_pair, key=lambda d: d[2])

        distance_ret = 0
        for i, j, k in distance_pair:
            if i not in mapo and j not in mapr:
                mapo[i] = j
                mapr[j] = i
                distance_ret = distance_ret + k

        return distance_ret

    def Graph_Edit_Dis(self, query, k):
        distance_rank = []
        for i in self.Graphs_Dependencies.keys():
            for j in self.Graphs_Dependencies[i].keys():
                dist = self.Star_Mapping_Dis(query, self.Graphs_Dependencies[i][j])
                distance_rank.append((j, dist))
        distance_rank = sorted(distance_rank, key=lambda d: d[1])
        if k > len(distance_rank):
            k = len(distance_rank)
        res = []
        for i, j in distance_rank[:k]:
            res.append(i)
        return res

    def __get_pre_depedencies4node(self, graph, qnode):
        node_list = {}
        q = queue.Queue()
        q.put(qnode)
        while not q.empty():
            temp_node = q.get()
            if temp_node not in node_list:
                node_list[temp_node] = {}
            predecessors = graph.successors(temp_node)
            for n in predecessors:
                q.put(n)
                node_list[temp_node][n] = "+" + graph[temp_node][n]["label"]
                successors = graph.predecessors(n)
                for s in successors:
                    if s in node_list:
                        if n not in node_list:
                            node_list[n] = {}
                        node_list[n][s] = "-" + graph[s][n]["label"]
        return node_list

    def __get_all_node_dependencies(self):
        gnodes_subgraph = {}
        for g in self.Graphs.keys():
            graph = self.Graphs[g]
            if graph and "nodes" in graph:
                gnodes_subgraph[g] = {}
                gnodes = list(graph.nodes)
                for n in gnodes:
                    gnodes_subgraph[g][n] = self.__get_pre_depedencies4node(graph, n)
        return gnodes_subgraph

    def __read_models(self):
        files = os.listdir(self.model_address)
        Graphs = {}
        for i in files:
            G = pickle.load(open(self.model_address + "/" + i, "rb"))
            Graphs[i] = G
        return Graphs

    def __init__(self, graphs):
        # self.Graphs = graphs
        self.Graphs_Dependencies = graphs  # self.__get_all_node_dependencies()

    def search_topk(self, query, k):
        return self.Graph_Edit_Dis(query, k)

    def search_score_rank(self, query):
        distance_rank = []
        for i in self.Graphs_Dependencies.keys():
            # for j in self.Graphs_Dependencies[i].keys():
            dist = self.Star_Mapping_Dis(query, self.Graphs_Dependencies[i])
            distance_rank.append((i, dist))
        distance_rank = sorted(distance_rank, key=lambda d: d[1])
        return distance_rank
