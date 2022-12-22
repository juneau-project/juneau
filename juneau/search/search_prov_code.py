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
Performs provenance search in tables by using the star distance
between rows.
"""


class ProvenanceSearch:

    def __init__(self, graphs):
        self.graphs_dependencies = graphs

    @staticmethod
    def star_distance(listA, listB):
        listA, listB = sorted(listA), sorted(listB)
        lenA, lenB = len(listA), len(listB)
        i, j = 0, 0
        intersection = []
        while i < lenA and j < lenB:
            if listA[i] == listB[j]:
                intersection.append(listA[i])
                i += 1
                j += 1
            elif listA[i] < listB[j]:
                i += 1
            elif listA[i] > listB[j]:
                j += 1
        dist = abs(lenA - lenB) + max(lenA, lenB) - len(intersection)
        return dist

    @staticmethod
    def star_mapping_distance(query, root):
        mapo, mapr = {}, {}
        distance_pair = []
        for i in query.keys():
            stara = query[i].values()
            for j in root.keys():
                starb = root[j].values()
                simAB = ProvenanceSearch.star_distance(stara, starb)
                distance_pair.append((i, j, simAB))

        distance_pair = sorted(distance_pair, key=lambda d: d[2])
        distance_ret = 0
        for i, j, k in distance_pair:
            if i not in mapo and j not in mapr:
                mapo[i] = j
                mapr[j] = i
                distance_ret += k

        return distance_ret

    def graph_edit_distance(self, query, k):
        distance_rank = []
        for i in self.graphs_dependencies.keys():
            for j in self.graphs_dependencies[i].keys():
                dist = ProvenanceSearch.star_mapping_distance(query, self.graphs_dependencies[i][j])
                distance_rank.append((j, dist))
        distance_rank = sorted(distance_rank, key=lambda d: d[1])
        k = min(k, len(distance_rank))
        return [i for i, _ in distance_rank[:k]]

    def search_top_k(self, query, k):
        return self.graph_edit_distance(query, k)

    def search_score_rank(self, query):
        distance_rank = []
        for i in self.graphs_dependencies.keys():
            dist = ProvenanceSearch.star_mapping_distance(query, self.graphs_dependencies[i])
            distance_rank.append((i, dist))
        return sorted(distance_rank, key=lambda d: d[1])
