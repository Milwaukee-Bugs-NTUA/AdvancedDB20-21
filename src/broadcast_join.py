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

    

    ratings = \
        sc.textFile("hdfs://master:9000/user/data/ratings.csv").mapPartitionsWithIndex(ignore_header)
    


    def broadcast_join(small_rdd, big_rdd, context):
        
        small = small_rdd.groupByKey(). \
        map(lambda row: (row[0],tuple(row[1]))). \
        collect()
        
        small_rdd_hash = dict(small)
        
        def helper(iterator):
            res = []
            for row in iterator:
                row = split_complex(row)
                if row[1] in small_rdd_hash:
                    res.append(row)
            return iter(res)
    
        def join(iterator):
            res = []
            for row in iterator:
                for genre in small_rdd_hash[row[1]]:
                    res.append((row[1],(genre, (row[0],row[2],row[3]))))
            return iter(res)
        
        context.broadcast(small_rdd_hash)
        
        res = big_rdd.mapPartitions(helper). \
        mapPartitions(join)

        return res
    

    start = time.time()
    
    table = broadcast_join(movie_genres, ratings, sc).collect()
    
    end = time.time()
    
    for i in table:
       print(i)
    print()
    print("Execution time with broadcast join: {} secs".format(end-start))
