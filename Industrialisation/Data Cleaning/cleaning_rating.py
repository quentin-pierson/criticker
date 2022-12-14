# Databricks notebook source
# MAGIC %run /Tools/Lib

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

od = OpenData()

# COMMAND ----------

df = od.read_brutes_rating_file()

# COMMAND ----------

display(df)

# COMMAND ----------

df_clean = df.withColumn("averageRating",col("averageRating").cast("Float"))
df_clean = df_clean.withColumn("numVotes",col("numVotes").cast("Integer"))

# COMMAND ----------

df_clean.dtypes

# COMMAND ----------

display(df_clean)

# COMMAND ----------

od.create_write_read_table(df_clean, "cl_rating")

# COMMAND ----------


