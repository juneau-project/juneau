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

import ast


class FuncLister(ast.NodeVisitor):
    """
    A node visitor base class that walks the abstract syntax tree
    and calls a visitor function for every node found. This is used
    in Juneau to TODO: why is FuncLister used in Juneau?
    """

    def __init__(self):
        self.dependency = {}

    def visit_Attribute(self, node):
        return_node = []
        if "id" in node.value.__dict__:
            return_node.append((node.value.id, node.attr))
        else:
            ret = self.visit(node.value)
            if ret:
                return_node = return_node + ret

        return return_node

    def visit_Index(self, node):
        return_node = []
        if "id" in node.value.__dict__:
            return_node.append((node.value.id, "Index"))
        else:
            ret = self.visit(node.value)
            if ret:
                return_node = return_node + ret
        return return_node

    def visit_Subscript(self, node):

        return_node = []

        if "id" in node.value.__dict__:
            return_node.append((node.value.id, "Index"))
        else:
            ret = self.visit(node.value)
            if ret:
                return_node = return_node + ret

        ret = self.visit(node.slice)
        if ret:
            return_node = return_node + ret

        return return_node

    def visit_UnaryOp(self, node):
        return_node = []
        if "id" in node.operand.__dict__:
            return_node.append((node.operand.id, str(node.op)))
        else:
            ret = self.visit(node.operand)
            if ret is not None:
                return_node = return_node + ret
        return return_node

    def visit_BinOp(self, node):

        return_node = []
        if "id" in node.left.__dict__:
            return_node.append((node.left.id, str(node.op)))
        else:
            ret = self.visit(node.left)
            if ret is not None:
                return_node = return_node + ret

        if "id" in node.right.__dict__:
            return_node.append((node.right.id, str(node.op)))
        else:
            ret = self.visit(node.right)
            if ret is not None:
                return_node = return_node + ret

        return return_node

    def visit_BoolOp(self, node):
        return_node = []
        for nv in node.values:
            if "id" in nv.__dict__:
                return_node.append((nv.id, str(node.op)))
            else:
                ret = self.visit(nv)
                if ret is not None:
                    return_node = return_node + ret

        return return_node

    def visit_Compare(self, node):
        return_node = []
        if "id" in node.left.__dict__:
            return_node.append((node.left.id, str(node.ops)))
        else:
            ret = self.visit(node.left)
            if ret:
                return_node = return_node + ret

        for nc in node.comparators:
            if "id" in nc.__dict__:
                return_node.append((nc.id, str(node.ops)))
            else:
                ret = self.visit(nc)
                if ret:
                    return_node = return_node + ret
        return return_node

    def visit_List(self, node):
        return_node = []
        if "elts" in node.__dict__:
            for ele in node.elts:
                if "id" in ele.__dict__:
                    return_node.append((ele.id, "List"))
                else:
                    ret = self.visit(ele)
                    if ret is not None:
                        return_node = return_node + ret
        return return_node

    def visit_Tuple(self, node):
        return_node = []
        if "elts" in node.__dict__:
            for ele in node.elts:
                if "id" in ele.__dict__:
                    return_node.append((ele.id, "Tuple"))
                else:
                    ret = self.visit(ele)
                    if ret is not None:
                        return_node = return_node + ret
        return return_node

    def visit_Call(self, node):

        return_node = []

        ret = self.visit(node.func)
        if ret:
            return_node = return_node + ret

        for na in node.args:
            if "id" in na.__dict__:
                if "id" in node.func.__dict__:
                    return_node.append((na.id, node.func.id))
                else:
                    return_node.append((na.id, node.func.attr))
            else:
                if "id" in node.func.__dict__:
                    fname = node.func.id
                else:
                    fname = node.func.attr

                ret = self.visit(na)
                if ret is not None:
                    for ri, rj in ret:
                        return_node.append((ri, fname))

        for nk in node.keywords:
            if "value" in nk.__dict__:
                if "id" in nk.value.__dict__:
                    if "id" in node.func.__dict__:
                        return_node.append((nk.value.id, node.func.id))
                    else:
                        return_node.append((nk.value.id, node.func.attr))
                else:
                    if "id" in node.func.__dict__:
                        fname = node.func.id
                    else:
                        fname = node.func.attr

                    ret = self.visit(nk.value)
                    if ret is not None:
                        for ri, rj in ret:
                            return_node.append((ri, fname))
        return return_node

    def visit_Assign(self, node):
        left_array = []
        for nd in node.targets:
            if "id" in nd.__dict__:
                left_array.append(nd.id)
            else:
                ret = self.visit(nd)
                if ret:
                    left_array = left_array + ret

        right_array = []
        if "id" in node.value.__dict__:
            right_array.append((node.value.id, "Assign"))
        else:
            ret = self.visit(node.value)
            if ret is not None:
                right_array = right_array + ret

        self.dependency[node.lineno] = (left_array, right_array)
