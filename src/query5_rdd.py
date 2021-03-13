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

def query5_rdd():
    spark = SparkSession.builder.appName('query5-sql').getOrCreate()
    sc = spark.sparkContext

    start = time.time()
    genres = \
        sc.textFile("hdfs://master:9000/user/data/movie_genres.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[0]), split_complex(x)[1]))
        
    ratings = \
        sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[1]), int(split_complex(x)[0])))
    
    table = \
        genres.join(ratings). \
        map(lambda x: ((x[1][0],x[1][1]), 1)). \
        reduceByKey(lambda x, y: x + y). \
        map(lambda x: (x[0][0], (x[0][1],x[1]))). \
        reduceByKey(lambda x, y: x if x[1] > y[1] else y). \
        sortByKey(). \
        collect()

    end = time.time()

    for i in table:
        print(i)
    print("Execution time:",end - start,"secs")

    return end - start

if __name__ == "__main__":
    query5_rdd()
