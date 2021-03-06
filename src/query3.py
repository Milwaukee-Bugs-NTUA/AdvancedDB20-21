#!/usr/bin/env python

from pyspark.sql import SparkSession
import time
import sys

def query3(format):
    spark = SparkSession.builder.appName('query3-sql').getOrCreate()

    if format == "csv":
        df = spark.read.format("csv").option("header", "true").option("inferSchema","true")
    elif format == "parquet":
        df = spark.read.format("parquet")
    else:
        print("No such format available!")
        exit(-1)

    df1 = df.load("/user/data/ratings." + format)
    df1.registerTempTable("ratings")
    df2 = df.load("/user/data/movie_genres." + format)
    df2.registerTempTable("movie_genres")

    #sqlString = \
    #"select mg.genre, AVG(r.rating) as mean_rating, count(distinct(mg.movie_id)) as movies " + \
	#"From movie_genres as mg, ratings as r " + \
	#"Where mg.movie_id == r.movie_id " + \
    #"Group by mg.genre"

    sqlString = \
    "select mg.genre, AVG(r.average_score) as mean_rating, count(r.movie_id) as movies " + \
	"from movie_genres as mg, " + \
    "(" + \
        "select movie_id, AVG(rating) as average_score " + \
        "from ratings " + \
        "group by movie_id" + \
    ") as r " + \
	"where mg.movie_id = r.movie_id " + \
    "group by mg.genre " + \
    "order by mg.genre"
    
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
        query3(sys.argv[1])
