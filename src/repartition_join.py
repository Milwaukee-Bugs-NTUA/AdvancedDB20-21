#!/usr/bin/env python

from pyspark.sql import SparkSession
from itertools import islice
from io import StringIO
import csv
import time

ignore_header = lambda idx, it: islice(it, 1, None) if idx == 0 else it

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def broadcast_join(table1, table2):
    pass

if __name__ == "__main__":

    spark = SparkSession.builder.appName('broadcast-join').getOrCreate()
    sc = spark.sparkContext

    movie_genres = \
        sc.textFile("hdfs://master:9000/user/data/movie_genres_100.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(split_complex). \
        map(lambda row:(row[0],row[1]))

    def repartition_join(rdd1, rdd2):
        L = rdd1.mapValues(lambda v: ('l',v))
        R = rdd2.mapValues(lambda v: ('r',v))
        
        def helper(values):
            Bl = []
            Br = []

            for (table,v) in values:
                if table == "l":
                    Bl.append(v)
                elif table == "r":
                    Br.append(v)

            return ((l,r) for l in Bl for r in Br)

        return L.union(R).groupByKey().flatMapValues(lambda x: helper(x))

    ratings = \
        sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(split_complex). \
        map(lambda row: (row[1], (row[0],*row[2:])))

    start = time.time()
    final = repartition_join(ratings,movie_genres).collect()
    end = time.time()

    print("Execution time with repartition join: {} secs".format(end-start))