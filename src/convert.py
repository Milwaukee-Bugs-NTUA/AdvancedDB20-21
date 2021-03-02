#!/home/user/.anaconda3/bin/python

import pandas as pd

# movies
df_movies = pd.read_csv('../dataset/movies.csv')
df_movies.to_parquet('../dataset/movies.parquet')
# genres
df_genres = pd.read_csv('../dataset/movie_genres.csv')
df_genres.to_parquet('../dataset/movie_genres.parquet')
# rankings
df_rankings = pd.read_csv('../dataset/ratings.csv')
df_rankings.to_parquet('../dataset/ratings.parquet')
