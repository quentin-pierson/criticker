# Databricks notebook source
# MAGIC %md
# MAGIC # Started LIB

# COMMAND ----------

# MAGIC %run /Tools/Lib

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import regexp_replace

# COMMAND ----------

# MAGIC %md 
# MAGIC ## open basic file

# COMMAND ----------

od = OpenData()

# COMMAND ----------

df = od.read_brutes_movies_titles_file()

# COMMAND ----------

df.columns

# COMMAND ----------

display(df)

# COMMAND ----------

df_filtered = df.dropDuplicates((['titleId']))
df_filtered = df_filtered.replace('\\N', 'None')
df_filtered = df.filter(df.region == "US")
df_filtered = df_filtered.withColumnRenamed("titleId", "tconst")

qualification_indexer = StringIndexer(inputCol="language", outputCol="language_out")
df_filtered = qualification_indexer.fit(df_filtered).transform(df_filtered)

df_filtered = df_filtered.withColumn("language_out",col("language_out").cast("Integer"))
df_filtered = df_filtered[["tconst","title", "language_out"]]
df_filtered = df_filtered.withColumnRenamed("language_out", "language")

# COMMAND ----------

df_filtered = df_filtered.withColumn("title", regexp_replace(df_filtered["title"], "\W", " "));

# COMMAND ----------

display(df_filtered.sort("title").distinct())

# COMMAND ----------

df_filtered.filter(df_filtered.title == "MinuteNightmare").show(truncate=False)

# COMMAND ----------

df_filtered.select('language').distinct().collect()

# COMMAND ----------

dff = od.create_write_read_table(df_filtered, "cl_movies_titles")

# COMMAND ----------


