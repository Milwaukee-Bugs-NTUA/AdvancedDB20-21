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

    sqlString = \
    "select first(m.income - m.cost) as profit, first(m.movie_id), first(m.title), t.year as year " + \
    "from (" + \
    "select MAX(income - cost) as maxprofit,YEAR(release_date) as year " + \
    "from movies " + \
    "where YEAR(release_date) >= 2000 " + \
    "and income != 0 and cost != 0 " + \
    "group by YEAR(release_date) " + \
    ") as t inner join movies as m " + \
    "on YEAR(m.release_date) = t.year and t.maxprofit = (m.income - m.cost) " + \
    "group by t.year " + \
    "order by t.year"
    # Query
    df = spark.sql(sqlString)
    df.show(df.count(),truncate=False)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide one file format form the above:")
        print("(1) csv, (2) parquet")
        exit(0)
    else:
        query(sys.argv[1])
