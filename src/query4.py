#!/usr/bin/env python

from pyspark.sql import SparkSession
import time
import sys

def query4(format,showOutput=True):
    spark = SparkSession.builder.appName('query4-sql').getOrCreate()

    if format == "csv":
        df = spark.read.format("csv").option("header", "true").option("inferSchema","true")
    elif format == "parquet":
        df = spark.read.format("parquet")
    else:
        print("No such format available!")
        exit(-1)

    df1 = df.load("/user/data/movies." + format)
    df1.registerTempTable("movies")
    df2 = df.load("/user/data/movie_genres." + format)
    df2.registerTempTable("movie_genres")

    sqlString = \
    "select (YEAR(m.release_date) - 2000) div 5 as period, AVG(LENGTH(m.summary) - LENGTH(REPLACE(m.summary, ' ', '')) + 1) as mean_summary " + \
    "from movies as m, movie_genres as mg " + \
    "where m.movie_id = mg.movie_id " + \
        "and Year(m.release_date) >= 2000 " + \
        "and mg.genre = 'Drama' " + \
    "group by (YEAR(m.release_date) - 2000) div 5 " + \
    "order by period" 

    # Query
    start = time.time()
    spark.sql(sqlString).show()
    end = time.time()
    print("Execution time:",end - start,"secs")

    return end - start

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide one file format form the above:")
        print("(1) csv, (2) parquet")
        exit(0)
    else:
        query4(sys.argv[1])
