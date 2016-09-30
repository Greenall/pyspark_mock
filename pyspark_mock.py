#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. 
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
'''
This module mocks the functionality of pyspark to enable rapid test-driven development

@author : John Greenall
'''
import os
import mock
from collections import defaultdict
from operator import add
from copy import deepcopy
import pyspark
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from pyspark.resultiterable import ResultIterable

def _partition_data(data, numSlices):
    '''
    function used internally for partitioning data
    '''
    numSlices = min(numSlices, len(data))
    if len(data) == 1:
        return [data]
    slices = range(0, len(data)-1, max(1, (numSlices-1) / (len(data)-1))) + [len(data)]
    partitioned = [data[st:nd] for st,nd in zip(slices[:-1], slices[1:])]    
    return partitioned

class SparkContextMock:
    '''
    This class mocks key functionality of a SparkContext.
    The simplest way to leverage this in your unittests without changing your application 
    code is to use the mock module as follows.
    
    Example:
    
        >>> import mock
        ... import pyspark_mock
        ... with mock.patch('pyspark.SparkContext', new=pyspark_mock.SparkContextMock):
        ...    # now make additional imports (including pyspark or any modules that themselves import pyspark)
        ...    unittest.main()
    '''
    _jvm = mock.MagicMock()
    environment = dict()
    defaultParallelizm = 10
    def __init__(self, *args, **kwargs):
        pyspark.SparkFiles._sc = self
        
    def __enter__(self, *args, **kwargs):
        '''
        No setup or teardown required with this Mock class but these methods required
        to make the class compatible with 'with' statement.
        '''
        return self
    
    def __exit__(self, *args, **kwargs):
        '''
        No setup or teardown required with this Mock class but these methods required
        to make the class compatible with 'with' statement.
        '''        
    
    def broadcast(self, value):
        '''
        Wrap supplied value in a BroadcastMock instance
        
        Returns:
            var (BroadcastMock) : broadcast variable
        '''
        return BroadcastMock(value)
    
    def parallelize(self, data, numSlices=None):
        '''
        Create an RDDMock object from the supplied data. 
        
        Args:
            data (iterable) : elements of the iterable become the elements of the RDD
            numSlices (int) : number of partitions to use for the data
            
        Returns:
            rdd (RDDMock) : the distributed dataset
            
        '''
        if numSlices is None:
            numSlices = self.defaultParallelizm

        return RDDMock(jrdd=_partition_data(data, numSlices), ctx=self)
    
    def addFile(self, path):
        '''
        Test for existence of supplied file
        
        Args:
            path (str): absolute path to file
        '''
        assert(os.path.exists(path))
    
    def addPyFile(self, path):
        '''
        Test for existence of supplied file
        
        Args:
            path (str): absolute path to file
        '''        
        assert(os.path.exists(path))
    
    @classmethod
    def setSystemProperty(cls, *args, **kwargs):
        '''
        dummy method - doesn't do anything
        '''

class RDDMock:
    '''
    This class mocks all the key functionality of a SparkRDD to allow rapid 
    prototyping and unittesting
    '''
    def __init__(self, jrdd, ctx, jrdd_deserializer=AutoBatchedSerializer(PickleSerializer())):
        self._jrdd = jrdd
        self.is_cached = False
        self.is_checkpointed = False
        self.ctx = ctx
        self._jrdd_deserializer = jrdd_deserializer   
    
    def map(self, f, preservesPartitioning=False):
        '''
        Create a new RDD by applying f to each element of this RDD.
        The values returned from those function calls will be the
        elements in the new RDD (one-one mapping)
        
        Args:
            f (function) : function to be applied to each element
            preservesPartitioning (bool) : should new RDD have identical
            partitions to this one? If false repartition with default.
        '''
        if preservesPartitioning:
            numSlices = len(self._jrdd)
        else:
            numSlices = SparkContextMock.defaultParallelizm
            
        data = _partition_data([f(x) for x in reduce(add, self._jrdd)], numSlices)
        return RDDMock(data, ctx=self.ctx)
    
    def flatMap(self, f, preservesPartitioning=False):
        '''
        Create a new RDD by instantiating an Iterator, f on each element 
        of this RDD and adding all elements returned by that Iterator
        to the new RDD. The mapping is potentially one-many.
        
        Args:
            f (function) : Iterator to be instantiated on each element
            preservesPartitioning (bool) : should new RDD have identical
            partitions to this one? If false repartition with default.
        '''        
        if preservesPartitioning:
            numSlices = len(self._jrdd)
        else:
            numSlices = SparkContextMock.defaultParallelizm
            
        data = _partition_data([x for d in reduce(add, self._jrdd) for x in f(d)], numSlices)        
        return RDDMock(data, ctx=self.ctx)
    
    def collect(self):
        '''
        Returns:
            data (list) : All elements of the RDD as a list
        '''
        return deepcopy(reduce(add, self._jrdd))
    
    def filter(self, f):
        '''
        Create a new RDD by evaluating f on each element of this RDD and
        dropping all elements that do not return True
        
        Args:
            f (function) : filter function that should return boolean
        '''
        nPartitions = len(self._jrdd)
        flat = reduce(add, self._jrdd)
        res = _partition_data(filter(f, flat), nPartitions) 
        return RDDMock(res, self.ctx)
    
    def take(self, n):
        '''
        Return a limited number of elements as a list
        
        Args:
            n (int) : maximum number of elements to return
        Returns:
            data (list) : min(n, |RDD|) elements of the RDD as a list
        '''        
        return deepcopy(reduce(add, self._jrdd)[:n])
    
    def groupByKey(self, numPartitions=None):
        '''
        Aggregate elements with matching keys, wrapping them in a ResultIterable
        
        Args:
            numPartitions (int) : number of partitions to use for the output data
        '''        
        res = defaultdict(list)
        for part in self._jrdd:
            for k,v in part:
                res[k].append(v)
        
        if numPartitions is None:
            numPartitions = SparkContextMock.defaultParallelizm
                            
        data = _partition_data([(k, ResultIterable(v)) for k, v in res.iteritems()], numPartitions)        
        return RDDMock(data, ctx=self.ctx)
    
    def reduceByKey(self, func, numPartitions=None):
        '''
        Aggregate elements with matching keys through repeated application of 
        the aggregation function func.
        
        Args:
            func (function) : function that must take two elements as input and return one element
            of the same type.
            numPartitions (int) : number of partitions to use for the output data
        '''
        return self.combineByKey(lambda x: x, func, func, numPartitions=numPartitions)
    
    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numPartitions=None):
        '''
        Aggregate elements with matching keys using a Combiner construct.
        
        Args:
            createCombiner (function) : function that takes as input an RDD element and instantiates
            a combiner instance
            mergeValue (function) : function that takes a combiner and value as input and returns
            a modified combiner
            mergeCombiner (function) : function that takes two combiners as input and returns
            the resultant combiner
        '''
        res = []
        for part in self._jrdd:
            r = dict()
            for k, v in part:
                if k in r:
                    r[k] = mergeValue(r[k], v)
                else:
                    r[k] = createCombiner(v)
                    
            res.append(r)
            
        final = dict()
        for r in res:
            for k, v in r.iteritems():
                if k in final:
                    final[k] = mergeCombiners(final[k], v)
                else:
                    final[k] = v
                    
        if numPartitions is None:
            numPartitions = SparkContextMock.defaultParallelizm
            
        data = _partition_data([(k, v) for k,v in final.iteritems()], numPartitions)
        return RDDMock(data, self.ctx)
    
    
class BroadcastMock:
    '''
    very simple wrapper to mock a Broadcast Variable
    '''
    def __init__(self, data):
        self.value = data
        
