import unittest
import mock
import pyspark_mock

class PysparkMockTester(unittest.TestCase):
    def test_SparkContextMock(self):
        sc = pyspark.SparkContext()
        self.assertIsInstance(sc, pyspark_mock.SparkContextMock)
        
        #check paralellization of a dataset works okay
        rdd = sc.parallelize(range(100))
        self.assertIsInstance(rdd, pyspark_mock.RDDMock)
        
    def test_SparkRDDMock(self):
        sc = pyspark.SparkContext()
        rdd = sc.parallelize(range(5))
        self.assertListEqual(range(5), rdd.collect())
        # test map
        m1 = rdd.map(lambda x: (x % 2, range(x)))
        self.assertEqual([(0, []), 
                          (1, [0]),
                          (0, [0,1]),
                          (1, [0,1,2]),
                          (0, [0,1,2,3])], m1.collect())
        # test flatMap
        m2 = m1.flatMap(lambda x: iter(x[1]))
        
        
        # test groupByKey
        
    
        
        
        
if __name__=='__main__':
    with mock.patch('pyspark.SparkContext', new=pyspark_mock.SparkContextMock):
        import pyspark
        unittest.main()
