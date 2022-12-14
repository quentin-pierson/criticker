# Databricks notebook source
# MAGIC %run /Tools/Lib

# COMMAND ----------

od = OpenData()

# COMMAND ----------

df_1 = od.read_brutes_actors_file()
df_2 = od.read_brutes_crew_file()
df_3 = od.read_brutes_principals_file()
df_4 = od.read_brutes_rating_file()
df_5 = od.read_brutes_movies_titles_file()

# COMMAND ----------

display(df_1)

# COMMAND ----------

display(df_2)

# COMMAND ----------

display(df_3)

# COMMAND ----------

display(df_4)

# COMMAND ----------

display(df_5)

# COMMAND ----------


