#!/usr/bin/env python

from pyspark.sql import SparkSession
import sys, time

disabled = sys.argv[1]
spark = SparkSession.builder.appName('query1-sql').getOrCreate()

if disabled == "Y":
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    #set("spark.sql.cbo.enabled", "False")
elif disabled == 'N':
    pass
else:
    raise Exception ("This setting is not available.")

df = spark.read.format("parquet")
df1 = df.load("hdfs://master:9000/user/data/ratings.parquet")
df2 = df.load("hdfs://master:9000/user/data/movie_genres_100.parquet")
df1.registerTempTable("ratings")
df2.registerTempTable("movie_genres")
sqlString = \
    """
    select *
    from 
        (
            SELECT * FROM movie_genres LIMIT 100
        ) as g, ratings as r
    where r.movie_id = g.movie_id
    """

t1 = time.time()
spark.sql(sqlString).show()
t2 = time.time()
spark.sql(sqlString).explain()
print("Time with choosing join type %s is %.4f sec."%("enabled" if
disabled == 'N' else "disabled", t2-t1))
with open("../results/test_joins.txt","a+") as f:
        f.write("{}\n".format(t2 - t1))
