#!/usr/bin/env python

from pyspark.sql import SparkSession
import time
import sys

def query5(format,showOutput=True):
    spark = SparkSession.builder.appName('query5-sql').getOrCreate()

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
    df3 = df.load("/user/data/movies." + format)
    df3.registerTempTable("movies")

    sqlString = \
    """
    with ratings_per_user_genre(genre,user_id,num) as
        (
            select mg.genre, r.user_id, count(*) as num
            from movie_genres as mg,ratings as r
            where mg.movie_id = r.movie_id
            group by mg.genre,r.user_id 
            grouping sets ((mg.genre,r.user_id))
        ),
    special_users(genre,user_id,max_num) as
    (
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
    ),
    special_users_ratings(genre,user_id,max_num,rating,movie_id,title,popularity) as
    (
        select s.genre,s.user_id,s.max_num, rating, m.movie_id, m.title, m.popularity
        from special_users as s, ratings as r, movies as m, movie_genres as mg
        where s.user_id = r.user_id
            and r.movie_id = m.movie_id
            and m.movie_id = mg.movie_id
            and s.genre = mg.genre
    ),
    max_ratings_table(genre,user_id,max_num,movie_id,title,max_rating,popularity) as
    (
        select t1.genre,t1.user_id,t1.max_num,t2.movie_id,t2.title,t1.max_rating,t2.popularity
        from 
            (
                select genre,user_id,max_num,max(rating) as max_rating
                from special_users_ratings
                group by genre,user_id,max_num
                grouping sets ((genre,user_id,max_num))
            ) as t1 inner join 
            special_users_ratings as t2 on
        t1.genre = t2.genre and
        t1.user_id = t2.user_id and
        t1.max_rating = t2.rating
    ),
    min_ratings_table(genre,user_id,max_num,movie_id,title,min_rating,popularity) as
    (
        select t1.genre,t1.user_id,t1.max_num,t2.movie_id,t2.title,t1.min_rating,t2.popularity
        from 
            (
                select genre,user_id,max_num,min(rating) as min_rating
                from special_users_ratings
                group by genre,user_id,max_num
                grouping sets ((genre,user_id,max_num))
            ) as t1 inner join 
            special_users_ratings as t2 on
        t1.genre = t2.genre and
        t1.user_id = t2.user_id and
        t1.min_rating = t2.rating
    )
    select favourite.genre,favourite.user_id,favourite.max_num,
            favourite.title,favourite.max_rating,least_favourite.title,least_favourite.min_rating
    from
        (
            select t1.genre,t1.user_id,t1.max_num,t2.title,t1.max_rating
            from
                (
                    select genre,user_id,max_num,max_rating,max(popularity) as max_popularity
                    from max_ratings_table
                    group by genre,user_id,max_num,max_rating
                    grouping sets ((genre,user_id,max_num,max_rating))
                ) as t1 inner join
                max_ratings_table as t2 on
            t1.genre = t2.genre and
            t1.user_id = t2.user_id and
            t1.max_popularity = t2.popularity
        ) as favourite,
        (
            select t1.genre,t1.user_id,t1.max_num,t2.title,t1.min_rating
            from
                (
                    select genre,user_id,max_num,min_rating,max(popularity) as max_popularity
                    from min_ratings_table
                    group by genre,user_id,max_num,min_rating
                    grouping sets ((genre,user_id,max_num,min_rating))
                ) as t1 inner join
                min_ratings_table as t2 on
            t1.genre = t2.genre and
            t1.user_id = t2.user_id and
            t1.max_popularity = t2.popularity
        ) as least_favourite
    where favourite.genre = least_favourite.genre
    and favourite.user_id = least_favourite.user_id
    order by favourite.genre
    """

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
        query5(sys.argv[1])
