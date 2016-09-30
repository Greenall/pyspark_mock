Rapid Test Driven Development With Pyspark
=================================================

This module was created in April 2015 during a project completed within AIG.
Whilst pyspark was in the main a pleasure to work with, I quickly found a big niggle that I didn't see addressed in any of the pyspark resources.

The project that I was implementing was not just a piece of one-off analysis that involved chaining a few simple map/ reduce operations. The task was complex and the software had to be kept live and actively maintained. Therefore a good level of test coverage was a necessity. 

For testing map functions, this is straightforward. It's easy to create a test taking an input record, and apply the map function before checking output matches the desired output for that record.

Where things get a little more painful is in testing, e.g. the `mergeCombiner`, `mergeValue` functions to be used by a `combineByKey` method. 
This necessitates creation of test data in terms of the intermediate quantities that would be passed to the `mergeCombiner`, `mergeValue` function within the `combineByKey` method. As we probably want to test several edge cases, this can add up to quite a lot of effort. 
A faster approach could be to test the `mergeCombiner`, `mergeValue`, functions together do what they should i.e. mapping from input RDD to desired output RDD via application of `combineByKey`. Though this has to be classified as functional test rather than a proper unit test, writing such tests may be more intuitive since the input RDD and output RDD are more likely to be meaningful quantities rather than intermediates. 

Can be illustrated with the following simple example, adapted from from [Bill Bejeck's blog](http://codingjunkie.net/spark-combine-by-key/):
```Python
from collections import namedtuple
from datetime import datetime
import unittest
import pyspark

CreditRecord = namedtuple('CreditRecord', ('firstName', 'surname', 'score', 'date'))

def map_credit_score(record):
    '''
    map the full name and score as key value pair
    
    Args:
        record  CreditRecord
    Returns:
        (fullname, score)
    '''
    return ('{} {}'.format(record.firstName, record.surname), record.score)
```

Now first I'll quickly write a test for the map function (which is so trivial it's almost not worth doing).


```Python

class SparkExperimentTester(unittest.TestCase):
    def test_map_credit_score(self):
        # check appropriate value returned
        s1 =  CreditRecord(firstName='Chester', surname='Cheeto', score=57, 
                            date=datetime(year=2015, month=1, day=1))
        self.assertEqual(('Chester Cheeto', 57), map_credit_score(s1))
    
if __name__=='__main__':
    unittest.main()
```
Running this test produces the following output:
```
.
----------------------------------------------------------------------
Ran 1 test in 0.004s

OK
```

Now let's add some `combineByKey` functionality. I would like to log a count, sum and squared sum of all scores associated with each person. This involves writing 3 simple functions:

```Python
def createScoreCombiner(score):
    '''
    create count, sum and squared sum
    '''
    return (1, score, score**2)

def scoreCombiner(collector, score):
    '''
    update count, sum and squared sum from single reading
    '''
    return (collector[0] + 1, collector[1] + score, collector[2] + score**2)

def scoreMerger(collector1, collector2):
    '''
    merge two sets of scores by summing
    '''
    return tuple([c1 + c2 for c1, c2 in zip(collector1, collector2)])
```

Rather than testing each function individually, I'd rather write one functional test that makes sure that the combination of all three functions do what they're supposed to do when applied to an RDD with `combineByKey`. Let's try something like this:

```Python
class SparkExperimentTester(unittest.TestCase):
    @classmethod
    def setUpClass(this):
        this.sc = pyspark.SparkContext()
        
    def test_squared_sum_count(self):
        records = [('Chester Cheeto', i) for i in xrange(5)] + [('Jimmy Mallow', 5)]
        rdd = self.sc.parallelize(records)
        res = rdd.collect()
        res = rdd.combineByKey(createScoreCombiner, scoreValueMerger, scoreCombinerMerger).collect()
        res = sorted(res, key=lambda x:x[0])
        self.assertEqual(res, [('Chester Cheeto', (5, sum(xrange(5)), sum(i**2 for i in xrange(5)))),
                               ('Jimmy Mallow', (1, 5, 25))])
    
if __name__=='__main__':
    unittest.main()
```

Note now that I've had to initialize a SparkContext. I've done that in the setUpClass method to minimize impact so I only have to do it once regardless of how many tests I'm going to write. Still, the output of this incredibly simple test now looks like this:

```
tput: No value for $TERM and no -T specified
Spark assembly has been built with Hive, including Datanucleus jars on classpath

16/09/16 11:17:57 INFO ui.SparkUI: Started SparkUI at http://mymachineurl:4040
16/09/16 11:17:57 INFO executor.Executor: Starting executor ID <driver> on host localhost
...
<LOTS MORE OUTPUT FROM SPARK>
...
16/09/16 11:17:59 INFO scheduler.DAGScheduler: Job 1 finished: collect at /data/SmartAIG/code_python/pyspark_mock_example.py:53, took 0.864428 s
.
----------------------------------------------------------------------
Ran 2 tests in 6.202s

OK
```
Six seconds! That's the cost of firing up a Spark Context. Doesn't sound like much but it becomes a real pain when you're making incremental changes to debug a more complex function. 

I wrote pyspark_mock to reduce this startup overhead at test time. It's a minimal mock implementation of a pyspark Context and can be easily used to speed up the above test. No changes are required to the tests, all we need to do is patch the SparkContext using the mock module as follows:
```Python
if __name__=='__main__':
    import pyspark_mock
    with mock.patch('pyspark.SparkContext', pyspark_mock.SparkContextMock):
        import pyspark
        unittest.main()
```

Now our console output looks like this:

```
..
----------------------------------------------------------------------
Ran 2 tests in 0.001s

OK
```
We got six seconds of our life back! Hope this guide provides useful to others.

### References
pyspark_mock implements the most commonly used methods of the SparkContext (further methods may be added as required). In between writing pyspark_mock and posting to Github, Sven Kreiss released a more comprehensive python implementation of the SparkContext called [pysparkling](https://github.com/svenkreiss/pysparkling).

### License
This code is supplied under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

