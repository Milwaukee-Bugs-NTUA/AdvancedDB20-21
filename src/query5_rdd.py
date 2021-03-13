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

# ((u,genre), (num, movie_id, rating, title, popularity))
def min_max(x, y):
    cur_key,  (cur_num, cur_movie_id, cur_rating, cur_title, cur_popularity) = x
    key,  (num, movie_id, rating, title, popularity) = x
    


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

    movies_ratings_genres = \
        ratings.join(movies). \
        mapValues(lambda t: t[0] + t[1]). \
        join(genres). \
        mapValues(lambda t: t[0] + t[1]). \
        map(lambda movie_id, values: ((values[0], values[-1]), (movie_id, values[1:-1])))
    # ((user_id, genre), (movie_id, rating, title, popularity, genre))

    # (genre, (user_id, num_ratings))
    special_users = \
        genres.join(ratings). \
        map(lambda x: ((x[1][0],x[1][1]), 1)). \
        reduceByKey(lambda x, y: x + y). \
        map(lambda x: (x[0][0], (x[0][1],x[1]))). \
        reduceByKey(lambda x, y: x if x[1] > y[1] else y). \
        map(lambda genre, t: ((genre, t[0]), t[1])). \
        join(movies_ratings_genres). \
        mapValues(lambda t: t[0] + t[1]). \
        max(key=lambda key, values: (values[2],values[4])). \
        collect()

        # ((u,genre), (num, movie_id, rating, title, popularity))
    end = time.time()

    for i in special_users:
        print(i)
    print("Execution time:",end - start,"secs")

    return end - start

if __name__ == "__main__":
    query5_rdd()
