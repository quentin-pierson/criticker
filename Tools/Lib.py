# Databricks notebook source
import pandas as pd
import os

# COMMAND ----------

class Lib:
    def __init__(self):
        self.adl_path_name = "fs.azure.account.key.adlgroupe21.blob.core.windows.net"
        self.adl_path_key = "wgBcbt2kEbo6ajPojiRTOJ9ODMc+8ITNlLDy3HuhixQJtAN5+NDO/NapzojIP/hUXZ6JPCXQN4RD+AStqSZt0A=="
        self.config = {
            self.adl_path_name: self.adl_path_key
        }
        self.scope = self.get_env_var("SECRET_SCOPE")
        
    def get_env_var(self, name):
        return os.getenv(name)
    
    def get_secret(self, name):
        return dbutils.secrets.get(scope=self.scope, key=name)
    
    def get_set_widget(self, name, value):
        self.set_widget(name, value)
        return self.get_widget(name)
    
    def set_widget(self, name, value):
        dbutils.widgets.text(name, value)
        
    def get_widget(self, name):
        return dbutils.widgets.get(name)
        

# COMMAND ----------

class FileManager(Lib):
    def __init__(self):
        super().__init__()
        
    def mnt_path(self, container_name, mount_name, storage_account_name="adlgroupe21"):
        mount_name = f"datalake_group_21_{container_name}"

        dbutils.fs.mount(
          source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
          mount_point = f"/mnt/{mount_name}",
          extra_configs=self.config)
        
    def spark_read_tsv(self, path, header, format, sep, *args):
        return spark.read.format(format).option("header", header).option("sep", sep).load(path)
    
    def pandas_read_tsv(self, path, header, sep, encoding, na_values, *args):
        pd.read_csv(f"/dbfs/{path}", header=header, sep=sep, encoding=encoding, na_values=na_values)
  
    def create_delta_table(self, columns, table_name, database_name="default"):
        data = ""
        for k, v in columns:
            data += f"{k} {v.upper()},\n"
        data = data[:-2]
        ddl_query = f"""CREATE OR REPLACE TABLE {database_name}.{table_name}
                        (
                           {data}
                        )USING DELTA;
                           """
        spark.sql(ddl_query)
    
    def write_table(self, dataframe, table_name, database_name="default", format="delta", mode="overwrite"):
        dataframe.write.format(format).mode(mode).saveAsTable(f"{database_name}.{table_name}")
    
    def read_table(self, table_name):
        df = spark.read.load(f"dbfs:/user/hive/warehouse/{table_name}")
        display(df)
        return df
    
    def create_write_read_table(self, dataframe, table_name, database_name="default"):
        self.create_delta_table(dataframe.dtypes, table_name, database_name)
        return self.write_read_table(dataframe, table_name)
    
    def write_read_table(self, dataframe, table_name, database_name="default", format="delta", mode="overwrite"):
        self.write_table(dataframe, table_name, database_name, format, mode)
        return self.read_table(table_name)
        
    def open_by_types(self, type, path, header, *args):
        if type == "pandas":
            return self.pandas_read_tsv(path, int(header == "true"), *args)
        elif type == "spark":
            return self.spark_read_tsv(path, header, *args)
        else:
            return "ERROR, type doit avoir comme valeur soit: 'spark', 'pandas'"

# COMMAND ----------

class OpenData(FileManager):
    def __init__(self):
        super().__init__()
        self.path_name_basic = "/mnt/datalake_group_21_brutes/name.basics.tsv"
        self.path_name_crew = "/mnt/datalake_group_21_brutes/title.crew.tsv"
        self.path_name_principals = "/mnt/datalake_group_21_brutes/title.principals.tsv"
        self.path_name_rating = "/mnt/datalake_group_21_brutes/title.rating.tsv"
        self.path_name_akas = "/mnt/datalake_group_21_brutes/titles.akas.tsv"
        self.path_name_genre = "/mnt/datalake_group_21_brutes/data.tsv"

    
    def read_brutes_actors_file(self, type="spark", header="true", format="csv", sep="\t", encoding="", na_values="\\N"):
        return self.open_by_types(type, self.path_name_basic, header, format, sep, encoding, na_values)

    def read_brutes_crew_file(self, type="spark", header="true", format="csv", sep="\t", encoding="", na_values="\\N"):
        return self.open_by_types(type, self.path_name_crew, header, format, sep, encoding, na_values)
    
    def read_brutes_actor_in_movie_file(self, type="spark", header="true", format="csv", sep="\t", encoding="", na_values="\\N"):
        return self.open_by_types(type, self.path_name_principals, header, format, sep, encoding, na_values)
    
    def read_brutes_rating_file(self, type="spark", header="true", format="csv", sep="\t", encoding="", na_values="\\N"):
        return self.open_by_types(type, self.path_name_rating, header, format, sep, encoding, na_values)
    
    def read_brutes_movies_titles_file(self, type="spark", header="true", format="csv", sep="\t", encoding="", na_values="\\N"):
        return self.open_by_types(type, self.path_name_akas, header, format, sep, encoding, na_values)
    
    def read_brutes_genre_file(self, type="spark", header="true", format="csv", sep="\t", encoding="", na_values="\\N"):
        return self.open_by_types(type, self.path_name_genre, header, format, sep, encoding, na_values)
    
      
    def read_clean_movies_titles(self):
        return spark.read.load("dbfs:/user/hive/warehouse/cl_movies_titles")
    
    def read_clean_movies_genres(self):
        return spark.read.load("dbfs:/user/hive/warehouse/cl_movies_genres") 
    
    def read_clean_movies_rating(self):
        return spark.read.load("dbfs:/user/hive/warehouse/cl_movies_rating")
    
    def read_clean_rating(self):
        return spark.read.load("dbfs:/user/hive/warehouse/cl_rating")
    
    def read_clean_director(self):
        return spark.read.load("dbfs:/user/hive/warehouse/cl_directors")
    
    def read_clean_actors(self):
        return spark.read.load("dbfs:/user/hive/warehouse/cl_actors") 

    def read_clean_genres(self):
        return spark.read.load("dbfs:/user/hive/warehouse/cl_genres") 

# COMMAND ----------

class Vizualization():
    def __init__(self):
        self.fm = FileManager()
        slef. fm.read_brutes_basic_file()

# COMMAND ----------

class ModelAI():
    def __init__(self):
        pass
