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
        sc.textFile("hdfs://master:9000/user/data/movie_genres.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(split_complex). \
        map(lambda row:(row[0],row[1])). \
        groupByKey(). \
        map(lambda row: (row[0],tuple(row[1]))). \
        take(2)

    movie_genres_dict = dict(movie_genres)
    sc.broadcast(movie_genres_dict)

    # Start join
    ratings = \
        sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (split_complex(x)[1], (split_complex(x)[0], split_complex(x)[2], split_complex(x)[3]))). \
        filter(lambda x: x[0] in movie_genres_dict). \
        map(lambda x: (x[0], (movie_genres_dict[x[0]],x[1]))). \
        collect()

    for i in ratings:
        print(i)