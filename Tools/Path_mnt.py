# Databricks notebook source


# COMMAND ----------

path_name = "fs.azure.account.key.adlgroupe21.blob.core.windows.net"
path_key = "wgBcbt2kEbo6ajPojiRTOJ9ODMc+8ITNlLDy3HuhixQJtAN5+NDO/NapzojIP/hUXZ6JPCXQN4RD+AStqSZt0A=="

# COMMAND ----------

config = {
    path_name: path_key
}

# COMMAND ----------

container_name = "brutes"
mount_name = f"datalake_group_21_{container_name}"
storage_account_name = "adlgroupe21"

dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
  mount_point = f"/mnt/{mount_name}",
  extra_configs=config)

# COMMAND ----------

# MAGIC %fs ls mnt/datalake_group_21_brutes/
