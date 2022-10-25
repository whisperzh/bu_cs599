from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function

import argparse
from matplotlib import pyplot as plt
import csv
import logging
import sys
from enum import Enum
from typing import List, Tuple
import uuid

import ray

# Note (john): Make sure you use Python's logger to log
#              information about your program
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()


# Partition strategy enum
class PartitionStrategy(Enum):
    RR = "Round_Robin"
    HASH = "Hash_Based"


# Custom tuple class with optional metadata
class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """

    def __init__(self, tuple, metadata=None, operator=None):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator

    # Returns the lineage of self
    def lineage(self) -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        return self.operator.lineage([self])
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(self, att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Returns the How-provenance of self
    def how(self) -> str:
        return self.metadata['how']

        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs(self) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        pass

    def __hash__(self):
        str_val = ''
        for i in self.tuple:
            str_val = str_val + '_' + str(i)
        return str_val


# Data operator
class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    def __init__(self,
                 id=None,
                 name=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        self.pull = pull
        self.partition_strategy = partition_strategy
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    # NOTE (john): Must be implemented by the subclasses
    def get_next(self) -> List[ATuple]:
        logger.error("Method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def apply(self, tuples: List[ATuple]) -> bool:
        logger.error("Apply method is not implemented!")


# Scan operator
class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    # Initializes scan operator
    def __init__(self,
                 filepath,
                 outputs: List[Operator],
                 filter=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 isleft=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Scan, self).__init__(name="Scan",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.filepath = filepath
        self.batch_size = 2000
        if outputs is not None:
            self.pushNxt = outputs[0]
        self.keys = []
        self.isLeft = isleft
        self.batches = []
        self.batch_generator = self.reader()
        self.track_prov = track_prov
        if track_prov:
            self.l_map = {}
        self.row_idx = 1

        pass

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        data = []
        try:
            data = next(self.batch_generator)
        except:
            data = None
        ans = [ATuple(self.keys), data]

        if data is None:
            return []
        return ans
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        ans = []
        for t in tuples:
            item = self.l_map.get(t.__hash__(), None)
            ans.append(item)
        return ans
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Starts the process of reading tuples (only for push-based evaluation)
    def start(self):

        try:
            while True:
                tag = 'L'
                if self.isLeft == False:
                    tag = 'R'
                tuples = next(self.batch_generator)
                data = [ATuple(self.keys), tuples, tag]
                self.pushNxt.apply(data)
        except Exception:
            self.pushNxt.apply([ATuple(self.keys)])

        pass

    def reader(self):
        with open(self.filepath, 'r') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=' ')
            self.keys = csv_reader.__next__()[1:]
            count = 0
            bat = []

            for row in csv_reader:
                if count == self.batch_size:
                    yield bat
                    count = 1
                    Arow = ATuple(tuple(row))

                    # lineage
                    if self.track_prov:
                        self.l_map[Arow.__hash__()] = Arow.tuple
                        Arow.operator = self

                    # how provenance
                    if self.propagate_prov:
                        if self.isLeft:
                            Arow.metadata = {'how': 'f' + str(self.row_idx)}
                        else:
                            Arow.metadata = {'how': 'r' + str(self.row_idx)}
                        self.row_idx += 1
                    bat = [Arow]
                else:
                    count += 1
                    Arow = ATuple(tuple(row))

                    # lineage
                    if self.track_prov:
                        self.l_map[Arow.__hash__()] = Arow.tuple
                        Arow.operator = self

                    # how provenance
                    if self.propagate_prov:
                        if self.isLeft:
                            Arow.metadata = {'how': 'f' + str(self.row_idx)}
                        else:
                            Arow.metadata = {'how': 'r' + str(self.row_idx)}
                        self.row_idx += 1

                    bat.append(Arow)

            yield bat

        pass


# Equi-join operator
class Join(Operator):
    """Equi-join operator.

    Attributes:
        left_inputs (List): A list of handles to the instances of the operator
        that produces the left input.

        right_inputs (List):A list of handles to the instances of the operator
        that produces the right input.

        outputs (List): A list of handles to the instances of the next
        operator in the plan.

        left_join_attribute (int): The index of the left join attribute.
        right_join_attribute (int): The index of the right join attribute.

        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    # Initializes join operator
    def __init__(self,
                 left_inputs: List[Operator],
                 right_inputs: List[Operator],
                 outputs: List[Operator],
                 left_join_attribute,
                 right_join_attribute,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Join, self).__init__(name="Join",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.left_attri = left_join_attribute
        self.right_attri = right_join_attribute
        # attributes
        if left_inputs is not None:
            self.next1 = left_inputs[0]
        else:
            self.next1 = None
        if right_inputs is not None:
            self.next2 = right_inputs[0]
        else:
            self.next2 = None
        if outputs is not None:
            self.pushNxt = outputs[0]
        self.track_prov = track_prov
        self.keyMapL = {}
        self.keyMapR = {}
        # push based key

        self.pullMap = {}
        self.titleL = []
        self.titleR = []

        pass

    # Returns next batch of joined tuples (or None if done)

    def get_next(self):
        datal = self.next1.get_next()
        self.titleL = datal[0].tuple
        self.leftTitlemap = {}
        createTitleMap(self.titleL, self.leftTitlemap)
        # left title

        while datal:
            datal = datal[1]
            for d in datal:
                key = d.tuple[self.leftTitlemap[self.left_attri]]
                self.keyMapL[key] = d
            datal = self.next1.get_next()
        # get key and map left

        datar = self.next2.get_next()
        self.titleR = datar[0].tuple
        self.rightTitlemap = {}
        createTitleMap(self.titleR, self.rightTitlemap)
        # right title

        ans = [ATuple(self.titleL + self.titleR), []]
        while datar:
            datar = datar[1]
            for d in datar:
                lef = self.keyMapL.get(d.tuple[self.rightTitlemap[self.right_attri]], None)
                if lef is not None:
                    Arow = ATuple(lef.tuple + d.tuple)
                    # how provenance
                    if self.propagate_prov:
                        Arow.metadata = {}
                        Arow.metadata['how'] = lef.metadata['how'] + '*' + d.metadata['how']

                    if self.track_prov:
                        Arow.operator = self
                    ans[1].append(Arow)

            datar = self.next2.get_next()
        # do matching

        return ans
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        ans = []
        for t in tuples:
            l_t = ATuple(t.tuple[0:len(self.titleL)])
            r_t = ATuple(t.tuple[len(self.titleL):])
            l_lineage = self.next1.lineage([l_t])
            r_lineage = self.next2.lineage([r_t])
            if isinstance(l_lineage, list):
                for l in l_lineage:
                    ans.append(l)
            else:
                ans.append(l_lineage)
            if isinstance(r_lineage, list):
                for r in r_lineage:
                    ans.append(r)
            else:
                ans.append(r_lineage)
        return ans

        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        if len(tuples) == 1 and len(self.keyMapR.keys()) > 0:
            self.pushNxt.apply(tuples)
            return
        if len(tuples) == 1:
            return

        tag = tuples[2]

        self.leftTitlemap = {}
        self.rightTitlemap = {}

        if tag == 'L':
            self.titleL = tuples[0].tuple
            createTitleMap(tuples[0].tuple, self.leftTitlemap)
        else:
            self.titleR = tuples[0].tuple
            createTitleMap(tuples[0].tuple, self.rightTitlemap)

        if tuples[2] == 'L':  # tuple->keys
            if len(self.keyMapR.keys()) == 0:
                self.creatHashMap(tuples[1], self.leftTitlemap, self.left_attri, self.left_attri, self.keyMapL)
            else:
                data = [tuples[0], []]
                # 1.compare left data with right key
                # 2.send data to upper ops
                for left_tuples in tuples[1]:
                    rig = self.keyMapR.get(left_tuples.tuple[self.leftTitlemap[self.left_attri]], None)
                    key = left_tuples.tuple[self.leftTitlemap[self.left_attri]]
                    self.keyMapL[key] = left_tuples.tuple
                    if rig is not None:
                        item = left_tuples.tuple + rig.tuple
                        Arow = ATuple(item)
                        if self.track_prov:
                            Arow.operator = self

                        # how provenance
                        if self.propagate_prov:
                            Arow.metadata = {}
                            Arow.metadata['how'] = left_tuples.metadata['how'] + '*' + rig.metadata['how']

                        data[1].append(Arow)
                    if self.next1 is None:
                        self.next1 = left_tuples.operator
                    if self.next2 is None:
                        self.next2 = rig.operator
                data[0] = ATuple(self.titleL + self.titleR)

                self.pushNxt.apply(data)

        else:
            data = [tuples[0], []]
            for right_tuples in tuples[1]:
                lef = self.keyMapL.get(right_tuples.tuple[self.rightTitlemap[self.right_attri]])
                key = right_tuples.tuple[self.rightTitlemap[self.right_attri]]
                self.keyMapR[key] = right_tuples.tuple
                if lef:
                    item = lef.tuple + right_tuples.tuple
                    Arow = ATuple(item)
                    if self.track_prov:
                        Arow.operator = self

                    # how provenance
                    if self.propagate_prov:
                        Arow.metadata = {}
                        Arow.metadata['how'] = lef.metadata['how'] + '*' + right_tuples.metadata['how']

                    data[1].append(Arow)
                if self.next1 is None:
                    self.next1 = lef.operator
                if self.next2 is None:
                    self.next2 = right_tuples.operator
            data[0] = ATuple(self.titleL + self.titleR)
            self.pushNxt.apply(data)

        pass

    def creatHashMap(self, tuples: List[ATuple], titlemap, titleAttri, field_to_delete, pullMap):
        for t in tuples:
            key = t.tuple[titlemap[titleAttri]]
            pullMap[key] = t
        pass


# Project operator
class Project(Operator):
    """Project operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    # Initializes project operator
    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[None],
                 fields_to_keep=[],
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Project, self).__init__(name="Project",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        # YOUR CODE HERE
        if inputs is not None:
            self.next_opt = inputs[0]
        else:
            self.next_opt = None
        self.fields_to_keep = fields_to_keep
        if outputs is not None:
            self.pushNxt = outputs[0]
        self.track_prov = track_prov
        self.lineage_map = {}
        pass

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        data = self.next_opt.get_next()
        title = data[0].tuple
        ans = [ATuple(self.fields_to_keep)]
        titleMap = {}
        createTitleMap(title, titleMap)

        ans.append([])
        data = data[1]

        for d in data:
            item = []
            for t in self.fields_to_keep:
                item.append(d.tuple[titleMap[t]])
            Arow = ATuple(tuple(item))
            if self.track_prov:
                self.lineage_map[Arow.__hash__()] = d
                Arow.operator = self

            # how provenance
            if self.propagate_prov:
                Arow.metadata = {}
                Arow.metadata['how'] = d.metadata['how']

            ans[1].append(Arow)
        return ans
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        # needtodo tuples-> original tuple
        ans = []
        for t in tuples:
            original_tuple = self.lineage_map.get(t.__hash__(), None)
            tmp = self.next_opt.lineage([original_tuple])
            if isinstance(tmp, list):
                for item in tmp:
                    ans.append(item)
            else:
                ans.append(tmp)
        return ans
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        if len(tuples) == 1:
            self.pushNxt.apply(tuples)
            # do submission
        else:
            title = tuples[0].tuple
            data = tuples[1]
            ans = [[], []]
            titleMap = {}
            if self.fields_to_keep == None:
                self.pushNxt.apply(tuples)
                return
            createTitleMap(title, titleMap)
            ans[0] = ATuple(self.fields_to_keep)
            # schema

            for d in data:
                item = []
                for t in self.fields_to_keep:
                    item.append(d.tuple[titleMap[t]])
                Arow = ATuple(item)
                if self.track_prov:
                    self.lineage_map[Arow.__hash__()] = d
                    Arow.operator = self
                    if self.next_opt is None:
                        self.next_opt = d.operator

                # how provenance
                if self.propagate_prov:
                    Arow.metadata = {}
                    Arow.metadata['how'] = d.metadata['how']

                ans[1].append(Arow)
            self.pushNxt.apply(ans)
        pass


# Group-by operator
class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function (e.g. AVG)
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    # Initializes average operator
    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 key,
                 value,
                 agg_gun,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(GroupBy, self).__init__(name="GroupBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        if inputs is not None:
            self.next_opt = inputs[0]
        else:
            self.next_opt = None
        if outputs is not None:
            self.pushNxt = outputs[0]
        self.key = key
        self.value = value
        self.agg = agg_gun
        self.data = []
        self.track_prov = track_prov
        self.lineage_original_tuples = {}
        # YOUR CODE HERE
        pass

    def AVG(self, data: List[ATuple], key, value):
        dic = {}
        diclen = {}

        if key is not None:
            ans = []
            if self.track_prov:
                self.lineage_original_tuples = {}
            if self.propagate_prov:
                self.how_map = {}
            for d in data:
                if dic.get(d.tuple[key]):
                    dic[d.tuple[key]] += int(d.tuple[value])
                    diclen[d.tuple[key]] += 1

                    # how provenance
                    if self.propagate_prov:
                        how_p = '('+d.metadata['how'] + '@' + d.tuple[value]+'),'
                        self.how_map[d.tuple[key]] += how_p

                    # lineage
                    if self.track_prov:
                        self.lineage_original_tuples[d.tuple[key]].append(d)
                else:
                    dic[d.tuple[key]] = int(d.tuple[value])
                    diclen[d.tuple[key]] = 1
                    if self.track_prov:
                        self.lineage_original_tuples[d.tuple[key]] = [d]

                    # how provenance
                    if self.propagate_prov:
                        how_p = '('+d.metadata['how'] + '@' + d.tuple[value]+'),'
                        self.how_map[d.tuple[key]] = 'AVG ('+ how_p

            for k in dic.keys():
                dic[k] /= diclen[k]
                Arow = ATuple([str(k), dic[k]])
                if self.track_prov:
                    Arow.operator = self

                # how provenance
                if self.propagate_prov:
                    Arow.metadata = {}
                    Arow.metadata['how'] = self.how_map[k][:-1]+')'

                ans.append(Arow)
            return ans

        else:
            if len(data) is not 0:
                sum = 0
                if self.propagate_prov:
                    how ='AVG ('
                for d in data:
                    sum += int(d.tuple[0])
                    how += '('+d.metadata['how'] + '@' + d.tuple[0]+'),'
                ans = sum / len(data)
                Arow = ATuple([str(ans)])
                if self.track_prov:
                    self.lineage_original_tuples[str(ans)] = data
                    Arow.operator = self

                if self.propagate_prov:
                    Arow.metadata = {}
                    Arow.metadata['how'] = how[:-1]+')'

                return [Arow]
            else:
                return []

        pass

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        data = self.next_opt.get_next()
        self.title = data[0].tuple
        data = data[1]
        ans = [ATuple(self.title)]
        self.titleMap = {}
        createTitleMap(self.title, self.titleMap)
        if self.agg == "AVG":
            ans.append(self.AVG(data, self.titleMap.get(self.key, None), self.titleMap[self.value]))
        if self.key is None:
            ans[0] = ATuple(["Average"])
        return ans
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        ans = []
        for t in tuples:
            key = 0
            if self.key:
                key = self.titleMap[self.key]
            for i in self.lineage_original_tuples[t.tuple[key]]:
                tmp = self.next_opt.lineage([i])
                if isinstance(tmp, list):
                    for item in tmp:
                        ans.append(item)
                else:
                    ans.append(tmp)
        return ans
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        if len(tuples) == 1:
            ans = [ATuple(self.title), []]
            if self.agg == "AVG":
                ans[1] = self.AVG(self.data, self.titleMap.get(self.key, None), self.titleMap[self.value])
            self.pushNxt.apply(ans)
            return
        # end of file

        else:
            self.title = tuples[0].tuple
            self.titleMap = {}
            createTitleMap(self.title, self.titleMap)
            for d in tuples[1]:
                if self.track_prov:
                    if self.next_opt is None:
                        self.next_opt = d.operator
                self.data.append(d)
        return

        pass


# Custom histogram operator
class Histogram(Operator):
    """Histogram operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    # Initializes histogram operator
    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 key=0,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Histogram, self).__init__(name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov,
                                        pull=pull,
                                        partition_strategy=partition_strategy)
        # YOUR CODE HERE
        if inputs is not None:
            self.next_opt = inputs[0]
        else:
            self.next_opt = None
        if outputs is not None:
            self.pushNxt = outputs[0]
        self.ans = []
        self.data = []
        self.track_prov = track_prov
        if self.track_prov:
            self.lineage_map = {}
        pass

    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        ans = []
        for t in tuples:
            original_tuples = self.lineage_map[t.tuple[0]]
            lin = self.next_opt.lineage(original_tuples)
            if isinstance(lin, list):
                for item in lin:
                    ans.append(item)

        return ans
        pass

    # Returns histogram (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        data = self.next_opt.get_next()
        dic = {}

        for d in data[1]:
            key = d.tuple[0]
            val = dic.get(key, 0)
            if self.track_prov:
                if val == 0:
                    self.lineage_map[key] = [d]
                else:
                    self.lineage_map[key].append(d)
            val += 1
            dic[key] = val
        tp1 = []
        for k in dic.keys():
            Arow = ATuple([k, dic[k]])
            Arow.operator = self
            tp1.append(Arow)
        data[1] = tp1
        data[0] = ATuple(["Rating", "count"])
        return data
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        # Creating histogram
        if len(tuples) == 1:
            # submit
            dic = {}
            for d in self.data:
                key = d.tuple[0]
                val = dic.get(key, 0)
                val += 1
                dic[key] = val
                if self.track_prov:
                    if self.next_opt is None:
                        self.next_opt = d.operator
                    if val == 1:
                        self.lineage_map[key] = [d]
                    else:
                        self.lineage_map[key].append(d)
            tp1 = []
            for k in dic.keys():
                Arow = ATuple([k, dic[k]])
                if self.track_prov:
                    Arow.operator = self
                tp1.append(Arow)
            ans = [ATuple(["Ratings", "amount"]), tp1]
            self.pushNxt.apply(ans)
        else:
            for d in tuples[1]:
                self.data.append(d)
        pass


# Order by operator
class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        ASC (bool): True if sorting in ascending order, False otherwise.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    # Initializes order-by operator
    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 comparator,
                 ASC=True,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(OrderBy, self).__init__(name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov,
                                      pull=pull,
                                      partition_strategy=partition_strategy)
        if inputs is not None:
            self.next_opt = inputs[0]
        else:
            self.next_opt = None
        if outputs is not None:
            self.pushNxt = outputs[0]
        self.comp = comparator
        self.track_prov = track_prov
        self.Asc = ASC
        self.data = []
        # YOUR CODE HERE
        pass

    # Returns the sorted input (or None if done)
    def get_next(self):
        data = self.next_opt.get_next()
        title = data[0].tuple
        data = data[1]
        titleMap = {}
        createTitleMap(title, titleMap)

        comp = titleMap[self.comp]
        if self.track_prov:
            for d in data:
                d.operator = self

        data.sort(key=lambda x: x.tuple[comp], reverse=not self.Asc)
        return [ATuple(title), data]
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        ans = []
        for t in tuples:
            tmp = self.next_opt.lineage([t])
            if isinstance(tmp, list):
                for item in tmp:
                    ans.append(item)
            else:
                ans.append(tmp)
        return ans
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        if len(tuples) == 1:
            comp = self.titleMap[self.comp]
            self.data.sort(key=lambda x: x.tuple[comp], reverse=not self.Asc)
            self.pushNxt.apply([ATuple(self.title), self.data])
            self.pushNxt.apply(tuples)
            return
        else:
            self.title = tuples[0].tuple
            data_raw = tuples[1]
            self.titleMap = {}
            createTitleMap(self.title, self.titleMap)
            for d in data_raw:
                if self.track_prov:
                    if self.next_opt is None:
                        self.next_opt = d.operator
                    d.operator = self
                self.data.append(d)

            comp = self.titleMap[self.comp]
            # self.data.sort(key=lambda x: x.tuple[comp], reverse=not self.Asc)
            # self.pushNxt.apply([ATuple(self.title), self.data])
        pass


# Top-k operator
class TopK(Operator):
    """TopK operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    # Initializes top-k operator
    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 k=None,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(TopK, self).__init__(name="TopK",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        if inputs is not None:
            self.next_opts = inputs[0]
        else:
            self.next_opts = None
        if outputs is not None:
            self.pushNxt = outputs[0]
        self.k = k
        self.track_prov = track_prov
        # YOUR CODE HERE
        pass

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        data = self.next_opts.get_next()
        title = data[0].tuple
        data = data[1]
        data = data[0:self.k]
        if self.track_prov:
            for d in data:
                d.operator = self
        return [ATuple(title), data]
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        return self.next_opts.lineage(tuples)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        if len(tuples) == 1:
            self.pushNxt.apply(tuples)
        else:
            data = tuples[1][0:self.k]
            if self.track_prov:
                for d in data:
                    if self.next_opts is None:
                        self.next_opts = d.operator
                    d.operator = self
            self.pushNxt.apply([tuples[0], data])
        pass


# Top-k operator
class Sink(Operator):
    """TopK operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    # Initializes top-k operator
    def __init__(self,
                 inputs,
                 outputs,
                 filepath,
                 track_prov=False,
                 block=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Sink, self).__init__(name="Sink",
                                   track_prov=track_prov,
                                   propagate_prov=propagate_prov,
                                   pull=pull,
                                   partition_strategy=partition_strategy)
        # YOUR CODE HERE
        self.fileoutput = filepath
        if inputs is not None:
            self.next_opt = inputs[0]
        if outputs is not None:
            self.pushNxt = outputs[0]
        self.track_prov = track_prov
        self.pull = pull
        self.output = [[], []]
        self.block = block
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        if self.pull:
            return self.next_opt.lineage(tuples)
        return self.pushNxt.lineage(tuples)
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    def get_next(self) -> List[ATuple]:
        self.output = self.next_opt.get_next()
        if self.track_prov:
            for t in self.output[1]:
                t.operator = self
        self.saveAsCsv()
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        if self.block is False:
            if len(tuples) > 1:
                self.output = tuples
                self.saveAsCsv()
        else:
            if len(tuples) > 1:
                self.output[0] = tuples[0]
                for t in tuples[1]:
                    self.output[1].append(t)
            else:
                self.saveAsCsv()
        pass

    def saveAsCsv(self):
        f = open(self.fileoutput, 'w', newline='')

        # create the csv writer
        writer = csv.writer(f, delimiter=',')
        writer.writerow(self.output[0].tuple)
        for row in self.output[1]:
            # write a row to the csv file
            writer.writerow(row.tuple)

        # close the file
        f.close()


# Filter operator
class Select(Operator):
    """Select operator.

    Attributes:
        inputs (List): A list of handles to the instances of the previous
        operator in the plan.
        outputs (List): A list of handles to the instances of the next
        operator in the plan.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
        pull (bool): Defines whether to use pull-based (True) vs
        push-based (False) evaluation.
        partition_strategy (Enum): Defines the output partitioning
        strategy.
    """

    # Initializes select operator
    def __init__(self,
                 inputs: List[Operator],
                 outputs: List[Operator],
                 predicate,
                 track_prov=False,
                 propagate_prov=False,
                 pull=True,
                 partition_strategy: PartitionStrategy = PartitionStrategy.RR):
        super(Select, self).__init__(name="Select",
                                     track_prov=track_prov,
                                     propagate_prov=propagate_prov,
                                     pull=pull,
                                     partition_strategy=partition_strategy)
        if inputs is not None:
            self.next_opt = inputs[0]
        else:
            self.next_opt = None
        self.predicate = predicate
        if outputs is not None:
            self.pushNxt = outputs[0]
        else:
            self.pushNxt = None
        if track_prov:
            self.track_prov = track_prov
        # YOUR CODE HERE
        pass

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        data = self.next_opt.get_next()
        if len(data) == 0:
            return []

        title = data[0].tuple
        ans = [ATuple(title), []]
        if self.predicate is None:
            ans[1] = data[1]
            if self.track_prov:
                for a in ans[1]:
                    a.operator = self
            return ans
        map = {}
        createTitleMap(title, map)
        data = data[1]
        for d in data:
            for k in self.predicate.keys():
                if d.tuple[map[k]] == str(self.predicate[k]):
                    if self.track_prov:
                        d.operator = self
                    ans[1].append(d)
        return ans
        pass

    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        ans = []
        for t in tuples:
            lin = self.next_opt.lineage([t])
            if isinstance(lin, list):
                for l in lin:
                    ans.append(l)
        return ans
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        if len(tuples) == 1:
            self.pushNxt.apply(tuples)
            return
        map = {}
        createTitleMap(tuples[0].tuple, map)
        data = tuples[1]
        ans = [tuples[0], [], tuples[2]]
        if self.predicate is None:
            ans[1] = data
            return self.pushNxt.apply(ans)

        for d in data:
            for k in dict(self.predicate).keys():
                if d.tuple[map[k]] == str(self.predicate[k]):
                    ans[1].append(d)
        self.pushNxt.apply(ans)
        pass


def createTitleMap(title, titleMap):
    for i in range(len(title)):
        titleMap[title[i]] = i


def query1(pull, pathf, pathr, uid, mid, resPath, track_prov=0, how=0, where=False):

    if track_prov is 0:
        track_prov = False
    else:
        track_prov = True

    if how is 0:
        how = False
    else:
        how = True

    if pull == 1:
        sf = Scan(filepath=pathf, outputs=None, track_prov=track_prov, propagate_prov=how)
        sr = Scan(filepath=pathr, outputs=None, track_prov=track_prov, propagate_prov=how)
        se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None, track_prov=track_prov, propagate_prov=how)
        se2 = Select(inputs=[sr], predicate={"MID": mid}, outputs=None, track_prov=track_prov, propagate_prov=how)
        join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
                    right_join_attribute="UID", track_prov=track_prov, propagate_prov=how)
        proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"], track_prov=track_prov, propagate_prov=how)
        groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG", track_prov=track_prov, propagate_prov=how)
        sink = Sink(inputs=[groupby], outputs=None, filepath=resPath, track_prov=track_prov, propagate_prov=how)
        sink.get_next()
    else:
        sink = Sink(inputs=None, outputs=None, filepath=resPath, track_prov=track_prov, propagate_prov=how)
        groupby = GroupBy(inputs=None, outputs=[sink], key="", value="Rating", agg_gun="AVG", track_prov=track_prov, propagate_prov=how)
        proj = Project(inputs=None, outputs=[groupby], fields_to_keep=["Rating"], track_prov=track_prov, propagate_prov=how)
        join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                    right_join_attribute="UID", track_prov=track_prov, propagate_prov=how)
        se1 = Select(inputs=None, predicate={"UID1": uid}, outputs=[join], track_prov=track_prov, propagate_prov=how)
        sf = Scan(filepath=pathf, outputs=[se1], track_prov=track_prov, propagate_prov=how)
        se2 = Select(inputs=None, predicate={"MID": mid}, outputs=[join], track_prov=track_prov, propagate_prov=how)
        sr = Scan(filepath=pathr, isleft=False, outputs=[se2], track_prov=track_prov, propagate_prov=how)
        sf.start()
        sr.start()
    pass


def query2(pull, pathf, pathr, uid, mid, resPath, track_prov=0, how=0, where=False):
    if track_prov is 0:
        track_prov=False
    else:
        track_prov=True

    if how is 0:
        how=False
    else:
        how=True

    if pull == 1:
        sf = Scan(filepath=pathf, outputs=None, track_prov=track_prov, propagate_prov=how)
        sr = Scan(filepath=pathr, outputs=None, track_prov=track_prov, propagate_prov=how)
        se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None, track_prov=track_prov, propagate_prov=how)
        se2 = Select(inputs=[sr], predicate=None, outputs=None, track_prov=track_prov, propagate_prov=how)
        join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
                    right_join_attribute="UID", track_prov=track_prov, propagate_prov=how)
        proj = Project(inputs=[join], outputs=None, fields_to_keep=["MID", "Rating"], track_prov=track_prov,
                       propagate_prov=how)
        groupby = GroupBy(inputs=[proj], outputs=None, key="MID", value="Rating", agg_gun="AVG", track_prov=track_prov,
                          propagate_prov=how)
        orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", ASC=False, track_prov=track_prov,
                          propagate_prov=how)
        topk = TopK(inputs=[orderby], outputs=None, k=1, track_prov=track_prov, propagate_prov=how)
        pj = Project(inputs=[topk], outputs=None, fields_to_keep=["MID"], track_prov=track_prov, propagate_prov=how)
        sink = Sink(inputs=[pj], outputs=None, filepath=resPath, track_prov=track_prov, propagate_prov=how)
        sink.get_next()
    else:
        sink = Sink(inputs=None, outputs=None, filepath=resPath, track_prov=track_prov, propagate_prov=how)
        pj = Project(inputs=None, outputs=[sink], fields_to_keep=["MID"], track_prov=track_prov, propagate_prov=how)
        topk = TopK(inputs=None, outputs=[pj], k=1, track_prov=track_prov, propagate_prov=how)
        orderby = OrderBy(inputs=None, outputs=[topk], comparator="Rating", ASC=False, track_prov=track_prov,
                          propagate_prov=how)
        gb = GroupBy(inputs=None, outputs=[orderby], key="MID", value="Rating", agg_gun="AVG", track_prov=track_prov,
                     propagate_prov=how)
        proj = Project(inputs=None, outputs=[gb], fields_to_keep=["MID", "Rating"], track_prov=track_prov,
                       propagate_prov=how)
        join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                    right_join_attribute="UID", track_prov=track_prov, propagate_prov=how)
        se1 = Select(inputs=None, predicate={"UID1": uid}, outputs=[join], track_prov=track_prov, propagate_prov=how)
        sf = Scan(filepath=pathf, outputs=[se1], track_prov=track_prov, propagate_prov=how)
        se2 = Select(inputs=None, predicate=None, outputs=[join], track_prov=track_prov, propagate_prov=how)
        sr = Scan(filepath=pathr, isleft=False, outputs=[se2], track_prov=track_prov, propagate_prov=how)
        sf.start()
        sr.start()
        for output in sink.output[1]:
            print(output.how())
    pass


def query3(pull, pathf, pathr, uid, mid, resPath, track_prov=0, how=0, where=False):

    if track_prov is 0:
        track_prov = False
    else:
        track_prov = True

    if how is 0:
        how = False
    else:
        how = True

    if pull == 1:
        sf = Scan(filepath=pathf, outputs=None)
        sr = Scan(filepath=pathr, outputs=None)
        se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
        se2 = Select(inputs=[sr], predicate={"MID": mid}, outputs=None)
        join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
                    right_join_attribute="UID")
        proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
        hist = Histogram(inputs=[proj], outputs=None)
        sink = Sink(inputs=[hist], outputs=None, filepath=resPath)
        sink.get_next()
    else:
        sink = Sink(inputs=None, outputs=None, filepath=resPath)
        hist = Histogram(inputs=None, outputs=[sink])
        proj = Project(inputs=None, outputs=[hist], fields_to_keep=["Rating"])
        join = Join(left_inputs=None, right_inputs=None, outputs=[proj], left_join_attribute="UID2",
                    right_join_attribute="UID")
        se1 = Select(inputs=None, predicate={"UID1": uid}, outputs=[join])
        sf = Scan(filepath=pathf, outputs=[se1])
        se2 = Select(inputs=None, predicate={"MID": mid}, outputs=[join])
        sr = Scan(filepath=pathr, isleft=False, outputs=[se2])
        sf.start()
        sr.start()
    pass



if __name__ == "__main__":
    logger.info("Assignment #1")

    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE

    # query1(True,"../data/friends.txt","../data/movie_ratings.txt",10,3,"../data/res.txt")
    # query2(True,"../data/friends.txt","../data/movie_ratings.txt",5,None,"../data/res.txt")
    # query3(True,"../data/friends.txt","../data/movie_ratings.txt",1190,16015,"../data/res.txt")

    parser = argparse.ArgumentParser()

    parser.add_argument("-q", "--query", type=int, help="task number",default=2)
    parser.add_argument("-f", "--ff", help="filepath", default='../data/lin_f.txt')
    parser.add_argument("-m", "--mf", help="filepath", default='../data/lin_m.txt')
    parser.add_argument("-uid", "--uid", help="uid",default=0)
    parser.add_argument("-mid", "--mid", help="mid")
    parser.add_argument("-p", "--pull", type=int, default=0, help="pull")
    parser.add_argument("-o", "--output", help="filepath", default='../data/res.txt')

    parser.add_argument("-l", "--lineage", help="lineage",type=int)
    parser.add_argument("-h", "--how", help="how provenance", type=int)
    parser.add_argument("-r", "--responsibility", help="responsibility", type=int)
    args = parser.parse_args()

    if args.query == 1:
        query1(args.pull, args.ff, args.mf, args.uid, args.mid, args.output,args.lineage,args.how)
    elif args.query == 2:
        query2(args.pull, args.ff, args.mf, args.uid, args.mid, args.output,args.lineage,args.how)
    elif args.query == 3:
        query3(args.pull, args.ff, args.mf, args.uid, args.mid, args.output,args.lineage,args.how)

    # TASK 2: Implement recommendation query for User A
    #
    # SELECT R.MID
    # FROM ( SELECT R.MID, AVG(R.Rating) as score
    #        FROM Friends as F, Ratings as R
    #        WHERE F.UID2 = R.UID
    #              AND F.UID1 = 'A'
    #        GROUP BY R.MID
    #        ORDER BY score DESC
    #        LIMIT 1 )

    # YOUR CODE HERE

    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE

    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'

    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE

    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE

    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE

    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
