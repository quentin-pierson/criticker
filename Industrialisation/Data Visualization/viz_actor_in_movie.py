# Databricks notebook source
# MAGIC %md
# MAGIC # Started LIB

# COMMAND ----------

# MAGIC %run /Tools/Lib

# COMMAND ----------

od = OpenData()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## open basic file

# COMMAND ----------

df = od.read_brutes_actor_in_movie_file()

# COMMAND ----------

display(df)
