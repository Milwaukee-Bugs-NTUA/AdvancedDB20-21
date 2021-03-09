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

    # Show movies table
    df1.show()

    sqlString = \
    "select MAX(m2.income - m2.cost) as profit, m1.movie_id, YEAR(m2.release_date) as year " + \
    "from movies as m1, movies as m2 " + \
    "where YEAR(m2.release_date) >= 2000 " + \
    "group by YEAR(m2.release_date)"
    # Query
    spark.sql(sqlString).show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide one file format form the above:")
        print("(1) csv, (2) parquet")
        exit(0)
    else:
        query(sys.argv[1])
