# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import first

# COMMAND ----------

# MAGIC %md
# MAGIC # Started LIB

# COMMAND ----------

# MAGIC %run /Tools/Lib

# COMMAND ----------

# MAGIC %md 
# MAGIC ## open basic file

# COMMAND ----------

od = OpenData()

# COMMAND ----------

df = od.read_brutes_genre_file()

# COMMAND ----------

display(df)

# COMMAND ----------

dfMovies = df.withColumn('genres', (F.split(df.genres,'\,')))
dfMovies = dfMovies.withColumn('genres', (F.explode(dfMovies.genres)))
dfMovies = dfMovies.replace('\\N', 'None')

# COMMAND ----------

genres = dfMovies.select('genres').distinct()

# COMMAND ----------

genres = genres.withColumn("id", F.monotonically_increasing_id())
genres = genres.withColumn("multiplication", 2 ** genres.id)
genres = genres.drop(genres.id)

# COMMAND ----------

display(genres)

# COMMAND ----------

od.create_write_read_table(genres, "cl_genres")

# COMMAND ----------

movies_col = dfMovies.join(genres, dfMovies.genres == genres.genres)

# COMMAND ----------

display(movies_col)

# COMMAND ----------

val = movies_col.groupBy("tconst").agg(F.sum("multiplication").alias('multiplication'))

display(val)

# COMMAND ----------

display(display(val.select("multiplication").distinct()))

# COMMAND ----------

movies_genres = dfMovies[["tconst","isAdult", "startYear", "runtimeMinutes"]]

# COMMAND ----------

display(movies_genres)

# COMMAND ----------

movies_genres_merge = movies_genres.join(val, "tconst")

# COMMAND ----------

display(movies_genres_merge)

# COMMAND ----------

od.create_write_read_table(movies_genres_merge, "cl_movies_genres")

# COMMAND ----------


