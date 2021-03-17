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
    
    start = time.time()
    movie_genres = \
        sc.textFile("hdfs://master:9000/user/data/movie_genres_100.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(split_complex). \
        map(lambda row:(row[0],row[1])). \
        groupByKey(). \
        map(lambda row: (row[0],tuple(row[1]))). \
        collect()

    movie_genres_dict = dict(movie_genres)
    sc.broadcast(movie_genres_dict)
    
    def helper(iterator):
        global movie_genres_dict
        res = []
        for row in iterator:
            row = split_complex(row)
            if row[1] in movie_genres_dict:
                res.append(row)
        return iter(res)
    
    def join(iterator):
        global movie_genres_dict
        res = []
        for row in iterator:
            for genre in movie_genres_dict[row[1]]:
                res.append((row[1],(genre, (row[0],row[2],row[3]))))
        return iter(res)

    ratings = \
        sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        mapPartitions(helper). \
        mapPartitions(join). \
        collect()
    end = time.time()

    # for i in ratings:
    #     print(i)
    print("Execution time with broadcast join: {} secs".format(end-start))