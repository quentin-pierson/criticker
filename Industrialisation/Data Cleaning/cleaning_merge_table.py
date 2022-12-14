# Databricks notebook source
# MAGIC %md
# MAGIC # Merge table

# COMMAND ----------

# MAGIC %run /Tools/Lib

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md 
# MAGIC ## open data

# COMMAND ----------

od = OpenData()

# COMMAND ----------

df1 = od.read_clean_movies_titles()
df2 = od.read_clean_rating()
df3 = od.read_clean_director()
df4 = od.read_clean_actors()
df5 = od.read_clean_movies_genres()

# COMMAND ----------

# MAGIC %md
# MAGIC # DISPLAY DATAFRAME

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df2)

# COMMAND ----------

display(df3)

# COMMAND ----------

display(df5)

# COMMAND ----------

ddf_merge = df1.join(df2, ["tconst"])
ddf_merge = ddf_merge.join(df3, ["tconst"])
ddf_merge = ddf_merge.withColumnRenamed("nconst", "director")
ddf_merge = ddf_merge[["tconst", "title","language", "director", "averageRating","numVotes"]]
ddf_merge = ddf_merge.join(df5, ["tconst"])

# COMMAND ----------

ddf_merge = ddf_merge.drop_duplicates()
ddf_merge = ddf_merge.replace('None', None)
ddf_merge = ddf_merge.withColumn("multiplication",ddf_merge.multiplication.cast('int'))
dff_merge = ddf_merge.select('*').distinct()

# COMMAND ----------

display(ddf_merge)

# COMMAND ----------

ddf_merge.dtypes

# COMMAND ----------

dff = od.create_write_read_table(ddf_merge, "cl_movies_rating")
