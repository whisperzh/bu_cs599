from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function

import argparse
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
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(self, att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Returns the How-provenance of self
    def how(self) -> str:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs(self) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        pass


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
        self.csv_reader = None
        self.batch_size = 2000
        self.batch_index = 0
        self.pushNxt = outputs[0]
        self.keys = []
        self.isLeft=isleft
        self.batches = []
        self.title = None
        pass

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        if len(self.batches) == 0:
            self.prepare_data()
        ans = [ATuple(self.keys),[]]
        data = self.batches[self.batch_index * self.batch_size:(self.batch_index + 1) * self.batch_size]
        ans[1]=data
        self.batch_index += 1
        if len(data) == 0:
            return []
        return ans
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Starts the process of reading tuples (only for push-based evaluation)
    def start(self):
        if len(self.batches) == 0:
            self.prepare_data()
        ans = [ATuple(self.keys), []]
        for i in range(0, len(self.batches), self.batch_size):
            # tuples=[ATuple(x) for x in self.batches[i:i + self.batch_size]]
            tuples = self.batches[i:i + self.batch_size]
            tag='L'
            if self.isLeft == False:
                tag='R'
            data = [ATuple(self.keys), tuples,tag]
            self.pushNxt.apply(data)

        pass

    def prepare_data(self):
        if self.csv_reader == None:
            with open(self.filepath, 'r') as csvfile:
                self.csv_reader = csv.reader(csvfile, delimiter=' ')
                self.keys = self.csv_reader.__next__()[1:]
                for row in self.csv_reader:
                    self.batches.append(ATuple(row))
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
        #attributes
        self.next1 = left_inputs[0]
        self.next2 = right_inputs[0]
        self.pushNxt = outputs[0]

        self.keyMapL={}
        self.keyMapR={}
        #push based key

        self.outTitleLeft=[]
        self.outTitleRight=[]
        self.pullMap = {}
        pass

    # Returns next batch of joined tuples (or None if done)

    def get_next(self):
        datal = self.next1.get_next()
        titleLeft = datal[0].tuple
        leftTitlemap = {}
        createTitleMap(datal[0].tuple,leftTitlemap)
        keystoBeProcess = []

        while datal:
            datal = datal[1]
            for d in datal:
                keystoBeProcess.append(d)
            datal = self.next1.get_next()

        datar = self.next2.get_next()
        titleRight = datar[0].tuple
        valuestoBeProcess = []
        while datar:
            datar = datar[1]
            for d in datar:
                valuestoBeProcess.append(d)
            datar = self.next2.get_next()

        rightTitlemap = {}
        createTitleMap(titleRight,rightTitlemap)
        self.creatHashMap(keystoBeProcess, self.leftTitlemap,self.left_attri,self.left_attri,self.keyMapL)
        del titleLeft[leftTitlemap[self.left_attri]]
        del titleRight[rightTitlemap[self.right_attri]]
        ans=[ATuple(titleLeft + titleRight),[]]

        for t in valuestoBeProcess:
            lef = self.keyMapL.get(t.tuple[rightTitlemap[self.right_attri]])
            tmp = []
            if lef:
                tmp = t.tuple
                del tmp[rightTitlemap[self.right_attri]]
                ans[1].append(ATuple(lef + tmp))
        # YOUR CODE HERE

        return ans
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        tag=tuples[2]
        self.leftTitlemap = {}

        self.rightTitlemap = {}
        if tag=='L':
            createTitleMap(tuples[0].tuple,self.leftTitlemap)
            self.outTitleLeft=tuples[0].tuple
            del self.outTitleLeft[self.leftTitlemap[self.left_attri]]
        else:
            createTitleMap(tuples[0].tuple,self.rightTitlemap)
            self.outTitleRight = tuples[0].tuple
            del self.outTitleRight[self.rightTitlemap[self.right_attri]]


        if tuples[2]=='L':                #tuple->keys
            if len(self.keyMapR.keys())==0:
                for left_tuples in tuples[1].tuple:
                    key = left_tuples[self.leftTitlemap[self.left_attri]]
                    del left_tuples[self.leftTitlemap[self.left_attri]]
                    self.keyMapL[key] = left_tuples

            else:
                data=[tuples[0],[]]
                #1.compare left data with right key
                #2.send data to upper ops
                for left_tuples in tuples[1].tuple:
                    if self.keyMapR.get(left_tuples[self.leftTitlemap[self.left_attri]]) is not None:
                        key=left_tuples[self.leftTitlemap[self.left_attri]]
                        del left_tuples[self.leftTitlemap[self.left_attri]]
                        self.keyMapL[key]=left_tuples
                        item=left_tuples+self.keyMapR[key]
                        data[1].append(item)
                data[0]=ATuple(self.outTitleLeft+self.outTitleRight)
                self.pushNxt.apply(data)

        else:
            data = [tuples[0], []]
            for right_tuples in tuples[1].tuple:
                if self.keyMapL.get(right_tuples[self.rightTitlemap[self.right_attri]]) is not None:
                    key = right_tuples[self.rightTitlemap[self.right_attri]]
                    del right_tuples[self.rightTitlemap[self.right_attri]]
                    self.keyMapR[key] = right_tuples
                    item = right_tuples + self.keyMapL[key]
                    data[1].append(item)
            data[0] = ATuple(self.outTitleLeft + self.outTitleRight)
            self.pushNxt.apply(data)

        pass


    def creatHashMap(self, tuples: List[ATuple], titlemap,titleAttri, field_to_delete,pullMap):
        for t in tuples:
            key = t.tuple[titlemap[titleAttri]]
            del t.tuple[titlemap[field_to_delete]]
            pullMap[key] = t.tuple
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
        self.next_opt = inputs[0]
        self.fields_to_keep = fields_to_keep
        self.pushNxt=outputs

        pass

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        data = self.next_opt.get_next()
        title = data[0].tuple
        ans = [ATuple(self.fields_to_keep)]
        titleMap = {}
        for i in range(len(title)):
            titleMap[title[i]] = i
        ans.append([])
        data = data[1]
        for d in data:
            item = []
            for t in self.fields_to_keep:
                item.append(d.tuple[titleMap[t]])
            ans[1].append(ATuple(item))
        return ans
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        title=tuples[0].tuple
        data=tuples[1]
        ans=[tuples[0],[]]
        titleMap = {}
        if self.fields_to_keep==None:
            ans[1]=tuples[1]
            self.pushNxt.apply(ans)
            return

        for i in range(len(title)):
            titleMap[title[i]] = i
        for d in data:
            item = []
            for t in self.fields_to_keep:
                item.append(d.tuple[titleMap[t]])
            ans[1].append(ATuple(item))
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
        if outputs is not None:
            self.pushNxt=outputs[0]
        self.key = key
        self.value = value
        self.agg = agg_gun
        # YOUR CODE HERE
        pass

    def AVG(self, data: List[ATuple], key, value):
        dic = {}
        diclen = {}

        if key is not None:
            ans = []
            for d in data:
                if dic.get(d.tuple[key]):
                    dic[d.tuple[key]] += int(d.tuple[value])
                    diclen[d.tuple[key]] += 1
                else:
                    dic[d.tuple[key]] = int(d.tuple[value])
                    diclen[d.tuple[key]] = 1
            for k in dic.keys():
                dic[k] /= diclen[k]
                ans.append(ATuple([k, dic[k]]))
            return ans

        else:
            sum = 0
            for d in data:
                sum += int(d.tuple[0])
            ans = sum / len(data)
            return [ATuple(ans)]

        pass

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        data = self.next_opt.get_next()
        title = data[0].tuple
        data = data[1]
        ans = [ATuple(title)]
        titleMap = {}
        createTitleMap(title,titleMap)
        if self.agg == "AVG":
            ans.append(self.AVG(data, titleMap[self.key], titleMap[self.value]))

        return ans
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        title=tuples[0].tuple
        titleMap={}
        createTitleMap(title,titleMap)
        ans=[tuples[0]]
        if self.agg == "AVG":
            ans.append(self.AVG(tuples[1], titleMap[self.key], titleMap[self.value]))
        self.pushNxt.apply(ans)
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
        pass

    # Returns histogram (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
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
        if outputs is not None:
            self.pushNxt = outputs[0]
        self.comp = comparator
        self.Asc = ASC
        # YOUR CODE HERE
        pass

    # Returns the sorted input (or None if done)
    def get_next(self):
        data = self.next_opt.get_next()
        title = data[0].tuple
        data = data[1]
        titleMap = {}
        for i in range(len((title))):
            titleMap[title[i]] = i
        comp = titleMap[self.comp]

        data.sort(key=lambda x: x.tuple[comp], reverse=not self.Asc)
        return [ATuple(title), data]
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        title = tuples[0].tuple
        data = tuples[1]
        titleMap = {}
        createTitleMap(title,titleMap)
        comp = titleMap[self.comp]
        data.sort(key=lambda x: x.tuple[comp], reverse=not self.Asc)
        self.pushNxt.apply([ATuple(title), data])
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
        self.next_opts = inputs[0]
        self.pushNxt=outputs[0]
        self.k = k
        # YOUR CODE HERE
        pass

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        data = self.next_opts.get_next()
        title = data[0].tuple
        data = data[1]
        data = data[0:self.k]
        return [ATuple(title), data]
        # YOUR CODE HERE
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        self.pushNxt.apply([tuples[0],tuples[1][0:self.k]])
        pass


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
        # YOUR CODE HERE
        pass

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        data = self.next_opt.get_next()
        if len(data) == 0:
            return []

        title = data[0].tuple
        ans = [data[0], []]
        if self.predicate is None:
            ans[1] = data[1]
            return ans
        map = {}
        for i in range(len(tuple(title.tuple))):
            map[title.tuple[i]] = i
        data = data[1]
        for d in data:
            for k in dict(self.predicate).keys():
                if d.tuple[map[k]] == self.predicate[k]:
                    ans[1].append(d)
        return ans
        pass

    # Applies the operator logic to the given list of tuples
    def apply(self, tuples: List[ATuple]):
        map = {}
        createTitleMap(tuples[0].tuple,map)
        data = tuples[1]
        ans = [tuples[0], [],tuples[2]]
        if self.predicate is None:
            ans[1] = data
            return self.pushNxt.apply(ans)

        for d in data:
            for k in dict(self.predicate).keys():
                if d.tuple[map[k]] == self.predicate[k]:
                    ans[1].append(d)
        self.pushNxt.apply(ans)
        pass

def createTitleMap(title,titleMap):
    for i in range(len(title)):
        titleMap[title[i]]=i


def query1(pathf, pathr, uid, mid):
    sf = Scan(filepath=pathf, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
    se2 = Select(inputs=[sr], predicate={"MID": mid}, outputs=None)
    join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG")
    groupby.get_next()
    pass


def query2(pathf, pathr, uid, mid):
    sf = Scan(filepath=pathf, outputs=None)
    sr = Scan(filepath=pathr, outputs=None)
    se1 = Select(inputs=[sf], predicate={"UID1": uid}, outputs=None)
    se2 = Select(inputs=[sr], predicate={"MID": mid}, outputs=None)
    join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2",
                right_join_attribute="UID")
    proj = Project(inputs=[join], outputs=None, fields_to_keep=["Rating"])
    groupby = GroupBy(inputs=[proj], outputs=None, key="", value="Rating", agg_gun="AVG")
    groupby.get_next()
    pass

join=Join(left_inputs=None, right_inputs=None, outputs=None, left_join_attribute="UID2", right_join_attribute="UID")
se1 = Select(inputs=[join], predicate={"UID1": '1190'}, outputs=None)
sf = Scan(filepath="../data/friends.txt", outputs=[se1])

sf.start()


sf = Scan(filepath="../data/friends.txt", outputs=None)
sr = Scan(filepath="../data/movie_ratings.txt", outputs=None)
se1 = Select(inputs=[sf], predicate={"UID1": '1190'}, outputs=None)
se2 = Select(inputs=[sr], predicate=None, outputs=None)
join = Join(left_inputs=[se1], right_inputs=[se2], outputs=None, left_join_attribute="UID2", right_join_attribute="UID")
proj = Project(inputs=[join], outputs=None, fields_to_keep=["MID", "Rating"])
groupby = GroupBy(inputs=[proj], outputs=None, key="MID", value="Rating", agg_gun="AVG")
orderby = OrderBy(inputs=[groupby], outputs=None, comparator="Rating", ASC=False)
topk = TopK(inputs=[orderby], outputs=None, k=1)
topk.get_next()

# if __name__ == "__main__":
#     logger.info("Assignment #1")
#
#     # TASK 1: Implement 'likeness' prediction query for User A and Movie M
#     #
#     # SELECT AVG(R.Rating)
#     # FROM Friends as F, Ratings as R
#     # WHERE F.UID2 = R.UID
#     #       AND F.UID1 = 'A'
#     #       AND R.MID = 'M'
#
#     # YOUR CODE HERE
#     query1("../data/friends.txt","../data/movie_ratings.txt","A","M")
#     parser = argparse.ArgumentParser()
#
#     parser.add_argument("-q", "--query", help="task number")
#     parser.add_argument("-f", "--ff", help="filepath")
#     parser.add_argument("-m", "--mf", help="filepath")
#     parser.add_argument("-uid", "--uid", help="uid")
#     parser.add_argument("-mid", "--mid", help="mid")
#     parser.add_argument("-p", "--pull", help="pull")
#     parser.add_argument("-o", "--output", help="filepath")
#     args = parser.parse_args()
#
#
#     # TASK 2: Implement recommendation query for User A
#     #
#     # SELECT R.MID
#     # FROM ( SELECT R.MID, AVG(R.Rating) as score
#     #        FROM Friends as F, Ratings as R
#     #        WHERE F.UID2 = R.UID
#     #              AND F.UID1 = 'A'
#     #        GROUP BY R.MID
#     #        ORDER BY score DESC
#     #        LIMIT 1 )
#
#     # YOUR CODE HERE
#
#     # TASK 3: Implement explanation query for User A and Movie M
#     #
#     # SELECT HIST(R.Rating) as explanation
#     # FROM Friends as F, Ratings as R
#     # WHERE F.UID2 = R.UID
#     #       AND F.UID1 = 'A'
#     #       AND R.MID = 'M'
#
#     # YOUR CODE HERE
#
#     # TASK 4: Turn your data operators into Ray actors
#     #
#     # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'
#
#     logger.info("Assignment #2")
#
#     # TASK 1: Implement lineage query for movie recommendation
#
#     # YOUR CODE HERE
#
#     # TASK 2: Implement where-provenance query for 'likeness' prediction
#
#     # YOUR CODE HERE
#
#     # TASK 3: Implement how-provenance query for movie recommendation
#
#     # YOUR CODE HERE
#
#     # TASK 4: Retrieve most responsible tuples for movie recommendation
#
#     # YOUR CODE HERE
