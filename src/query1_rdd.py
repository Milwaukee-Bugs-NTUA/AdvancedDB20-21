#!/usr/bin/env python

from pyspark.sql import SparkSession
from itertools import islice
from io import StringIO
import csv
import time

ignore_header = lambda idx, it: islice(it, 1, None) if idx == 0 else it

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def ex_year(date):
    return date.split("-")[0]

def query1():
    spark = SparkSession.builder.appName('query1-sql').getOrCreate()
    sc = spark.sparkContext

    start = time.time()
    table = \
        sc.textFile("hdfs://master:9000/user/data/movies.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (split_complex(x)[0], (ex_year(split_complex(x)[3]), split_complex(x)[5], split_complex(x)[6], split_complex(x)[1]))). \
        filter(lambda x: not x[1][0] == '' and not float(x[1][1]) == 0 and not float(x[1][2]) == 0). \
        filter(lambda x: int(x[1][0]) >= 2000). \
        map(lambda x: (int(x[1][0]), (int(x[0]),x[1][3], ((float(x[1][2]) - float(x[1][1])) / float(x[1][1]))*100))). \
        reduceByKey(lambda x, y: x if x[2] > y[2] else y). \
        sortByKey(ascending=True). \
        collect()

    for i in table:
        print(i)
    end = time.time()
    print("Execution time:",end - start,"secs")

    return end - start

if __name__ == "__main__":
    query1()
