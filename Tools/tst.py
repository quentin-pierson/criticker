# Databricks notebook source
# MAGIC %run Tools/Lib

# COMMAND ----------

# MAGIC %secrets list-scopes

# COMMAND ----------

lib = Lib()
adl_path_key_2 = lib.get_secret("adlgroupe21-blob-container-key")

# COMMAND ----------


