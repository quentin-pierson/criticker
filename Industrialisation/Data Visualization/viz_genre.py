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

df = od.read_brutes_genre_file()

# COMMAND ----------

display(df)

# COMMAND ----------


