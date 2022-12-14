# Databricks notebook source
# MAGIC %md
# MAGIC # Started LIB

# COMMAND ----------

# MAGIC %run /Tools/Lib

# COMMAND ----------

od = OpenData()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## open akas file

# COMMAND ----------

df = od.read_brutes_movies_titles_file()

# COMMAND ----------

display(df)
