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

def words(text):
    return len(text.split(" "))

def period(year):
    perids = {\
        0:"2000 - 2004", \
        1:"2005 - 2009", \
        2:"2010 - 2014", \
        3:"2015 - 2019", \
        4:"2020 - 2024"}
    return (year - 2000) // 5

def query4_rdd():
    spark = SparkSession.builder.appName('query4-sql').getOrCreate()
    sc = spark.sparkContext

    start = time.time()
    movies = \
        sc.textFile("hdfs://master:9000/user/data/movies.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[0]), (ex_year(split_complex(x)[3]), split_complex(x)[2]))). \
        filter(lambda x: not x[1][0] == '' and not x[1][1] == ''). \
        mapValues(lambda v: (int(v[0]),v[1])). \
        filter(lambda x: x[1][0] >= 2000)
    drama = \
        sc.textFile("hdfs://master:9000/user/data/movie_genres.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[0]), split_complex(x)[1])). \
        filter(lambda x: x[1] == "Drama")

    table = \
        drama.join(movies). \
        map(lambda x: (period(x[1][1][0]), (words(x[1][1][1]), 1))). \
        reduceByKey(lambda x, y: (x[0] + y[0],x[1] + y[1])). \
        mapValues(lambda v: v[0]/v[1]). \
        sortByKey(). \
        collect()

    end = time.time()

    for i in table:
        print(i)
    print("Execution time:",end - start,"secs")

    return end - start

if __name__ == "__main__":
    query4_rdd()
