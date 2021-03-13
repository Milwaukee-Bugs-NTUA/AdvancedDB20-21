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

def query3_rdd():
    spark = SparkSession.builder.appName('query3-sql').getOrCreate()
    sc = spark.sparkContext

    start = time.time()
    avg_ratings = \
        sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[1]), (float(split_complex(x)[2]), 1))). \
        reduceByKey(lambda x,y: (x[0] + y[0],x[1] + y[1])). \
        mapValues(lambda v: v[0]/v[1])
    genres = \
        sc.textFile("hdfs://master:9000/user/data/movie_genres.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[0]), split_complex(x)[1])) \
        
    table = \
        genres.join(avg_ratings). \
        map(lambda x: (x[1][0], (x[1][1], 1))). \
        reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])). \
        mapValues(lambda v: (v[0]/v[1], v[1])). \
        sortByKey(). \
        collect()

    end = time.time()

    for i in table:
        print(i)
    print("Execution time:",end - start,"secs")

    return end - start

if __name__ == "__main__":
    query3_rdd()
