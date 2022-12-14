# Databricks notebook source
# MAGIC %run /Tools/Lib

# COMMAND ----------

od = OpenData()

# COMMAND ----------

df = od.read_brutes_genre_file()

# COMMAND ----------

rfgiuehrufg = pd.get_dummies(df.genres, dummy_na=True)

# COMMAND ----------

display(rfgiuehrufg)

# COMMAND ----------

df = df.replace('\\N', 'None')

# COMMAND ----------

from pyspark.ml.feature import StringIndexer
qualification_indexer = StringIndexer(inputCol="language", outputCol="language_index")
df1 = qualification_indexer.fit(df).transform(df)

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %fs ls mnt/datalake_group_21_brutes/

# COMMAND ----------


