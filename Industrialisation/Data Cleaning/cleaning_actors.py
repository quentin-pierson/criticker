# Databricks notebook source
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

df = od.read_brutes_actors_file(type="spark")

# COMMAND ----------

df = df.replace('\\N', 'None')

# COMMAND ----------

display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC Other file

# COMMAND ----------

df_4 = od.read_brutes_rating_file()

# COMMAND ----------

display(df_4)

# COMMAND ----------

df_1 = od.read_brutes_basic_file()
df_1 = df_1[["nconst", "primaryName"]]
df_2 = od.read_brutes_crew_file()
df_2 = df_2[["tconst","directors"]]
df_4 = od.read_brutes_rating_file()
df_4 = df_4[["tconst","averageRating"]]
df_5 = od.read_clean_movies_titles()

movie_metadata_table = df_1.join(df_2,df_1.nconst==df_2.directors)
full_movie_metadata_table = movie_metadata_table.join(df_4,movie_metadata_table.tconst==df_4.tconst)
full_movie_metadata_table = full_movie_metadata_table.join(df_5,movie_metadata_table.tconst==df_5.tconst)
full_movie_metadata_table = full_movie_metadata_table[["title","averageRating","primaryName"]]
display(full_movie_metadata_table)
