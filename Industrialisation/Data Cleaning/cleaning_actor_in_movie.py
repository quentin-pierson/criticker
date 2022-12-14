# Databricks notebook source
# MAGIC %run /Tools/Lib

# COMMAND ----------

od = OpenData()

# COMMAND ----------

df = od.read_brutes_actor_in_movie_file()
df2 = od.read_brutes_actors_file()

# COMMAND ----------

cleaning_category_table = df[["tconst", "nconst", "category"]]

# COMMAND ----------

display(cleaning_category_table)

# COMMAND ----------

cleaning_actor_table = cleaning_category_table.filter(cleaning_category_table.category == "actor") #.collect() #  && cleaning_actor_table.ordering == 1 in [None,1,2,3]

# COMMAND ----------

display(cleaning_actor_table)

# COMMAND ----------

dff = od.create_write_read_table(cleaning_actor_table, "cl_actors")

# COMMAND ----------

cleaning_director_table = cleaning_category_table.filter(cleaning_category_table.category == "director") #.collect() #  && cleaning_actor_table.ordering == 1 in [None,1,2,3]

# COMMAND ----------

dff = od.create_write_read_table(cleaning_director_table, "cl_directors")
