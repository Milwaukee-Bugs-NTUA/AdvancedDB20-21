#!/usr/bin/env python

from pyspark.sql import SparkSession
from itertools import islice
from io import StringIO
import csv
import time

ignore_header = lambda idx, it: islice(it, 1, None) if idx == 0 else it

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

def query5_rdd():
    spark = SparkSession.builder.appName('query5-sql').getOrCreate()
    sc = spark.sparkContext

    start = time.time()

    genres = \
        sc.textFile("hdfs://master:9000/user/data/movie_genres.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[0]), split_complex(x)[1]))
    # (movie_id, genre)
    # dummy_genres = \
    #     sc.textFile("hdfs://master:9000/user/data/movie_genres.csv"). \
    #     mapPartitionsWithIndex(ignore_header). \
    #     map(lambda x: ((int(split_complex(x)[0]),split_complex(x)[1]), ()))
    # # ((movie_id,genre),())
        
    ratings = \
        sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[1]), (int(split_complex(x)[0]), float(split_complex(x)[2]))))
    # (movie_id, (user_id, rating))

    # user_ratings = \
    #     sc.textFile("hdfs://master:9000/user/data/ratings.csv"). \
    #     mapPartitionsWithIndex(ignore_header). \
    #     map(lambda x: (int(split_complex(x)[0]), (int(split_complex(x)[1]), float(split_complex(x)[2]))))
    # # (user_id, (movie_id, rating))
    
    movies = \
        sc.textFile("hdfs://master:9000/user/data/movies.csv"). \
        mapPartitionsWithIndex(ignore_header). \
        map(lambda x: (int(split_complex(x)[0]), (split_complex(x)[1],float(split_complex(x)[-1]))))
    # (movie_id, (title, popularity))

    # ((user_id, genre), (movie_id, rating, title, popularity, genre))

    # map(lambda genre, t: ((genre, t[0]), t[1])). \
    #     join(movies_ratings_genres). \
    #     mapValues(lambda t: t[0] + t[1]). \
    #     max(key=lambda key, values: (values[2],values[4])). \

    # join 
    # (movie_id,genre) x (movie_id,(user_id, rating))
    # (movie_id, (genre, (user_id, rating)))
    # map ((genre,user_id), 1)
    # reduce ((genre,user_id),num)
    # map (genre, (user_id,num))
    # special users ar computed
    # map (user_id, (genre,num))
    # join (user_id, (genre,num)) x (user_id,(movie_id,rating))
    # (user_id, (genre,num),(movie_id, rating))
    # mapvalues (user_id, (genre,num, movie_id, rating))
    # map ((movie_id,genre), (num, user_id,rating))
    # join ((movie_id,genre), (num, user_id,rating)) X ((movie_id,genre),())
    special_users = \
        genres.join(ratings). \
        map(lambda x: ((x[1][0],x[1][1][0]), 1)). \
        reduceByKey(lambda x, y: x + y). \
        map(lambda x: (x[0][0], (x[0][1],x[1]))). \
        reduceByKey(lambda x, y: x if x[1] > y[1] else y). \
        map(lambda x: (x[1][0], (x[0],x[1][1]))). \
        collect()

    print("Special Users computed")
    special = dict(special_users)

    special_ratings = \
        ratings.filter(lambda x: x[1][0] in special). \
        take(10)      
    
    print("Special ratings computed")
        
        
        # map(lambda x: (x[1][0], (x[0],x[1][1]))). \
        # join(user_ratings). \
        # mapValues(lambda t1: t1[0] + t1[1]). \
        # map(lambda x: ((x[1][0],x[1][2]), (x[1][1],x[0],x[1][3]))). \
        # join(dummy_genres). \
        # filter(lambda x: x[0][1] == 'Action'). \
        # take(10)        

    end = time.time()

    for i in special_users:
        print(i)
    for i in special_ratings:
        print(i)
    print("Execution time:",end - start,"secs")

    return end - start

if __name__ == "__main__":
    query5_rdd()
