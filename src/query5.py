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

    df1 = df.load("/user/data/ratings." + format)
    df1.registerTempTable("ratings")
    df2 = df.load("/user/data/movie_genres." + format)
    df2.registerTempTable("movie_genres")

    sqlString = \
    """
    with ratings_per_user_genre(genre,user_id,num) as
        (
            select mg.genre, r.user_id, count(*) as num
            from movie_genres as mg,ratings as r
            where mg.movie_id = r.movie_id
            group by mg.genre,r.user_id 
            grouping sets ((mg.genre,r.user_id))
        )
    select t1.genre,first(t2.user_id) as user_id,first(t1.max_num) as number_of_reviews
    from 
        (
            select genre, max(num) as max_num
            from ratings_per_user_genre
            group by genre
        ) as t1 inner join 
        ratings_per_user_genre as t2
    on t1.genre = t2.genre and t1.max_num = t2.num
    group by t1.genre
    order by t1.genre
    """

    # Query
    spark.sql(sqlString).show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide one file format form the above:")
        print("(1) csv, (2) parquet")
        exit(0)
    else:
        query4(sys.argv[1])
