#!/usr/bin/env python

from pyspark.sql import SparkSession
import sys
import time

def query2(format,showOutput=True):
    spark = SparkSession.builder.appName('query2-sql').getOrCreate()

    if format == "csv":
        df = spark.read.format("csv").option("header", "true")
    elif format == "parquet":
        df = spark.read.format("parquet")
    else:
        print("No such format available!")
        exit(-1)

    df1 = df.load("/user/data/ratings." + format)
    df1.registerTempTable("ratings")

    sqlString = \
    "select first(g3)/count(distinct(r.user_id)) *100 as percentage " + \
	"From (" + \
        "Select count(*) as g3 " + \
        "from " + \
            "(select user_id, AVG(rating) as mean_rating " + \
	        "From ratings " + \
	        "Group by user_id " + \
            "Having AVG(rating) >3)" + \
        ") as t1, ratings as r"
    
    # Query
    start = time.time()
    df = spark.sql(sqlString)
    end = time.time()
    
    if showOutput:
        df.show()
        print("Execution time:",end - start,"secs")

    return end - start

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide one file format form the above:")
        print("(1) csv, (2) parquet")
        exit(0)
    else:
        query2(sys.argv[1])
