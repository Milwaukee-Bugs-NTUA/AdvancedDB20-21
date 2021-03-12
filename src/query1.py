#!/usr/bin/env python

from pyspark.sql import SparkSession
import time
import sys

def query1(format, showOutput=True):
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

    sqlString = \
    "select first(t.maxprofit) as profit, first(m.movie_id) as movie_id, first(m.title) as title, t.year as year " + \
    "from (" + \
        "select MAX(((income - cost)/cost)*100) as maxprofit,YEAR(release_date) as year " + \
        "from movies " + \
        "where YEAR(release_date) >= 2000 " + \
        "and income != 0 and cost != 0 " + \
        "group by YEAR(release_date) " + \
    ") as t inner join movies as m " + \
    "on YEAR(m.release_date) = t.year and t.maxprofit = (((m.income - m.cost)/m.cost)*100) " + \
    "group by t.year " + \
    "order by t.year"
    # Query
    start = time.time()
    df = spark.sql(sqlString)
    end = time.time()

    if showOutput:
        df.show(df.count(),truncate=False)
        print("Execution time: {} secs".format(end-  start))
    return end - start

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide one file format form the above:")
        print("(1) csv, (2) parquet")
        exit(0)
    else:
        query1(sys.argv[1])
