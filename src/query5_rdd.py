#!/usr/bin/env python

from pyspark.sql import SparkSession
from itertools import islice
from io import StringIO
import csv
import time

ignore_header = lambda idx, it: islice(it, 1, None) if idx == 0 else it

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def convert_to_dict(l):
    res = {}
    for t in l:
        res[(t[1][0], t[0])] = t[1][1]
    return res

def query5_rdd():
    spark = SparkSession.builder.appName('query5-sql').getOrCreate()
    sc = spark.sparkContext

    start = time.time()

    genres = \
        sc.textFile("hdfs://master:9000/user/data/movie_genres.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[0]), split_complex(x)[1]))
    # (movie_id, genre)        
    ratings = \
        sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[1]), (int(split_complex(x)[0]), float(split_complex(x)[2]))))
    # (movie_id, (user_id, rating))
    movies = \
        sc.textFile("hdfs://master:9000/user/data/movies.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[0]), (split_complex(x)[1],float(split_complex(x)[-1]))))
    # (movie_id, (title, popularity))

    # join 
    # (movie_id,genre) x (movie_id,(user_id, rating))
    # (movie_id, (genre, (user_id, rating)))
    # map ((genre,user_id), 1)
    # reduce ((genre,user_id),num)
    # map (genre, (user_id,num))
    # special users computed
    special_users_rdd = \
        genres.join(ratings). \
        map(lambda x: ((x[1][0],x[1][1][0]), 1)). \
        reduceByKey(lambda x, y: x + y). \
        map(lambda x: (x[0][0], (x[0][1],x[1]))). \
        reduceByKey(lambda x, y: x if x[1] > y[1] else y)
    
    special_users = \
        special_users. \
        map(lambda x: (x[1][0], (x[0],x[1][1]))). \
        collect()
    
    special_users_rdd = \
        special_users_rdd. \
        map(lambda x: ((x[0],x[1][0]), x[1][1]))

    special_tuples = convert_to_dict(special_users)

    print("Special Users computed") 

    special_ratings = \
        ratings.filter(lambda x: x[1][0] in dict(special_users)). \
        join(genres). \
        mapValues(lambda v: (v[0][0],v[0][1],v[1])). \
        filter(lambda x: (x[1][2],x[1][0]) in special_tuples). \
        join(movies). \
        mapValues(lambda v: (v[0][0],v[0][1],v[0][2],v[1][0],v[1][1])). \
        map(lambda x: ((x[1][2], x[1][0]) ,(x[0],x[1][1],x[1][3],x[1][4]))). \
        reduceByKey(lambda x, y: x if x[1] > y[1] or (x[1] == y[1] and x[3] >= y[3]) else y). \
        collect()

    # join(special_users_rdd). \
    #     mapValues(lambda v: (v[0][0],v[0][1],v[0][2],v[0][3],v[1][0])). \
    print("Special ratings computed")
    end = time.time()

    for i in special_users:
        print(i)
    for i in special_ratings:
        print(i)
    print("Execution time:",end - start,"secs")

    return end - start

if __name__ == "__main__":
    query5_rdd()
