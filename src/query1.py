#!/usr/bin/env python

from pyspark.sql import SparkSession
import sys

def query(format):
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
    df3 = df.load("/user/data/ratings." + format)
    df3.registerTempTable("ratings")

    # Show movies table
    df1.show()
    df2.show()
    df3.show()

    # sqlString = \
    #     "select max(income - cost) as profit, movieId, year" + \
    #     "from movies" + \
    #     "where year > 2000" + \
    #     "group by year"

    # # Query
    # spark.sql(sqlString).show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide one file format form the above:")
        print("(1) csv, (2) parquet")
        exit(0)
    else:
        query(sys.argv[1])
