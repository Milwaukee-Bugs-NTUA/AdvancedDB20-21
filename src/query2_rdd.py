#!/usr/bin/env python

from pyspark.sql import SparkSession
from itertools import islice
from io import StringIO
import time
import csv

ignore_header = lambda idx, it: islice(it, 1, None) if idx == 0 else it

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def query2():
    spark = SparkSession.builder.appName('query2-sql').getOrCreate()
    sc = spark.sparkContext
    
    start = time.time()
    # percentage = \
    #     sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
    #     mapPartitionsWithIndex(ignore_header). \
    #     map(lambda x: (int(split_complex(x)[0]), (float(split_complex(x)[2]), 1))). \
    #     reduceByKey(lambda x,y: (x[0] + y[0],x[1] + y[1])). \
    #     mapValues(lambda v: v[0]/v[1]). \
    #     map(lambda x: (0,1) if x[1] > 3 else (1,1)). \
    #     reduceByKey(lambda x,y: x + y). \
    #     map(lambda x: (0, (x[0],x[1]))). \
    #     reduceByKey(lambda x,y: (x[1]/(x[1] + y[1]))*100 if x[0] == 0 else (y[1]/(x[1] + y[1]))*100). \
    #     collect()

    percentage = \
        sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[0]), float(split_complex(x)[2]))). \
        aggregateByKey((0,0), lambda a,b: (a[0] + b,    a[1] + 1),lambda a,b: (a[0] + b[0], a[1] + b[1])). \
        mapValues(lambda v: v[0]/v[1]). \
        map(lambda x: (0,1) if x[1] > 3 else (1,1)). \
        reduceByKey(lambda x,y: x + y). \
        map(lambda x: (0, (x[0],x[1]))). \
        reduceByKey(lambda x,y: (x[1]/(x[1] + y[1]))*100 if x[0] == 0 else (y[1]/(x[1] + y[1]))*100). \
        collect()

    for p in percentage:
        print(p[1])
    end = time.time()
    print("Execution time:",end - start,"secs")

    return end - start


if __name__ == "__main__":
    query2()
