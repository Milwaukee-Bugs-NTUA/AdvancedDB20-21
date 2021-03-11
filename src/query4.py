#!/usr/bin/env python

from pyspark.sql import SparkSession
import sys

def query4(format):
    spark = SparkSession.builder.appName('query1-sql').getOrCreate()

    if format == "csv":
        df = spark.read.format("csv").option("header", "true")
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
    "select AVG(LENGTH(m.summary) - LENGTH(REPLACE(m.summary, ' ', '')) + 1) as mean_summary, (YEAR(m.release_date) - 2000) div 5 as period " + \
    "from movies as m, movie_genres as mg " + \
    "where m.movie_id = mg.movie_id " + \
        "and Year(m.release_date) >= 2000 " + \
        "and mg.genre = 'Drama' " + \
    "group by (YEAR(m.release_date) - 2000) div 5 " + \
    "order by period" 

    # Query
    spark.sql(sqlString).show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide one file format form the above:")
        print("(1) csv, (2) parquet")
        exit(0)
    else:
        query4(sys.argv[1])
