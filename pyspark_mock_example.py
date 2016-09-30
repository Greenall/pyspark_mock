from collections import namedtuple
from datetime import datetime
import unittest
import mock

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


def createScoreCombiner(score):
    '''
    create count, sum and squared sum
    '''
    return (1, score, score**2)

def scoreValueMerger(collector, score):
    '''
    update count, sum and squared sum from single reading
    '''
    return (collector[0] + 1, collector[1] + score, collector[2] + score**2)

def scoreCombinerMerger(collector1, collector2):
    '''
    merge two sets of scores by summing
    '''
    return tuple([c1 + c2 for c1, c2 in zip(collector1, collector2)])

class SparkExperimentTester(unittest.TestCase):
    @classmethod
    def setUpClass(this):
        this.sc = pyspark.SparkContext()
        
    def test_map_credit_score(self):
        # check appropriate value returned
        s1 =  CreditRecord(firstName='Chester', surname='Cheeto', score=57, 
                            date=datetime(year=2015, month=1, day=1))
        self.assertEqual(('Chester Cheeto', 57), map_credit_score(s1))
        
    def test_squared_sum_count(self):
        records = [('Chester Cheeto', i) for i in xrange(5)] + [('Jimmy Mallow', 5)]
        rdd = self.sc.parallelize(records)
        res = rdd.collect()
        res = rdd.combineByKey(createScoreCombiner, scoreValueMerger, scoreCombinerMerger).collect()
        res = sorted(res, key=lambda x:x[0])
        self.assertEqual(res, [('Chester Cheeto', (5, sum(xrange(5)), sum(i**2 for i in xrange(5)))),
                               ('Jimmy Mallow', (1, 5, 25))])
                         
        
    
if __name__=='__main__':
    import pyspark_mock
    with mock.patch('pyspark.SparkContext', pyspark_mock.SparkContextMock):
        import pyspark
        unittest.main()