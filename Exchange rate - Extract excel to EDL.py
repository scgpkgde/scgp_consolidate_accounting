# Databricks notebook source
# MAGIC %md
# MAGIC #Extract data from the excel Sheet: 2022

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, substring , split , lit , stack ,  explode, col,array,monotonically_increasing_id
import pandas as pd
import re

SOURCE_PATH = "abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/exchange_rate/"
src_files = dbutils.fs.ls(SOURCE_PATH)
 

excel_file_path = src_files[0].path

# COMMAND ----------


df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", f"'2022'!A3:Y15") \
    .load(excel_file_path)

df_year = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", f"'2022'!A1:A2") \
    .load(excel_file_path)
data_year = df_year.select(col("_c0").substr(-4, 4).alias("data_year")).collect()[0]["data_year"].strip()

df_country = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", f"'2022'!B2:Y15") \
    .load(excel_file_path)
country_column_names = [col_name for col_name in df_country.columns if not col_name.startswith('_c')]

def remove_numbers_from_last_index(column_name):
    return re.sub(r'\d+$', '', column_name)

new_column_names =  ['Month'] + [country + '_' + remove_numbers_from_last_index(name) 
                               for country in country_column_names 
                               for name in df.columns[1:4]]
df_renamed = df.select([col(old).alias(new) for old, new in zip(df.columns, new_column_names)])

def transform_dataframe(df):
    new_df = spark.createDataFrame([], schema="Year INT, Month STRING, Currency STRING, Currency_Type STRING, Conversion_Rate DOUBLE")
    for column_name in df.columns:
        type_rate_split = column_name.replace('\n', '_').split('_')
        if len(type_rate_split) >= 2:
            type_rate = type_rate_split[1]
            currency = column_name.split('_')[0] 
    
            new_df = new_df.union(df.select(
              lit(data_year).alias("Year"),
              expr("`{}`".format(df.columns[0])).alias("Month"), 
              lit(currency).alias("Currency"), 
              lit(type_rate).alias("Currency_Type"), 
              expr("`{}`".format(column_name)).alias("Conversion_Rate")
              ))
    return new_df

transformed_df = transform_dataframe(df_renamed)


# COMMAND ----------

# MAGIC %md
# MAGIC # Create hash column

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, to_date, lpad, current_timestamp

transformed_df = transformed_df.withColumn("scd_active",lit(1))
transformed_df = transformed_df.withColumn("scd_start",current_timestamp())
transformed_df = transformed_df.withColumn("scd_end", to_date(concat(lit("9999"), lit("12"), lit("31")), "yyyyMMdd").cast("timestamp"))

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws

transformed_df = transformed_df.withColumn("src_hash_key", sha2(
  concat_ws("_","Year","Month","Currency","Currency_Type"), 256
  )
)

transformed_df = transformed_df.withColumn("src_hash_diff", sha2(
  concat_ws("_","Conversion_Rate"), 256
  )
)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, DecimalType, BooleanType

transformed_df = transformed_df.withColumn("Year", col("Year").cast("int"))
transformed_df = transformed_df.withColumn("Conversion_Rate", col("Conversion_Rate").cast(DecimalType(20, 4)))
transformed_df = transformed_df.withColumn("scd_active", lit(1).cast(BooleanType()))
transformed_df = transformed_df.withColumn("scd_start",current_timestamp())
transformed_df = transformed_df.withColumn("scd_end", to_date(concat(lit("9999"), lit("12"), lit("31")), "yyyyMMdd").cast("timestamp"))

# COMMAND ----------

transformed_df.cache()

# COMMAND ----------

transformed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Read Data in Table

# COMMAND ----------

path = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_STG/cad/excel_exchange_rate';
existing_df = spark.read.format("delta").load(path)

 

# COMMAND ----------

# MAGIC %md
# MAGIC #Update

# COMMAND ----------

from pyspark.sql.functions import col

transformed_df_alias = transformed_df.alias("t")
existing_df_alias = existing_df.alias("e")

joined_df = transformed_df_alias.join(existing_df_alias, "src_hash_key", "inner")\
                                .filter((col("t.src_hash_diff") != col("e.src_hash_diff")))
df_insert = joined_df.select("t.*")
df_update = joined_df.select("e.*")

# COMMAND ----------

from pyspark.sql.functions import col , current_timestamp

df_update = df_update.withColumn("scd_active", lit(False))\
                     .withColumn("scd_end", current_timestamp())

existing_df_filtered = existing_df_alias.join(df_update.select("src_hash_key"), "src_hash_key", "left_anti")
final_df = existing_df_filtered.union(df_update)
final_df = final_df.union(df_insert) 


final_df.write.format("delta").mode("overwrite").save("abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_STG/cad/excel_exchange_rate")


# COMMAND ----------

# MAGIC %md
# MAGIC #Insert

# COMMAND ----------

from pyspark.sql.functions import col

result_df = transformed_df.select("src_hash_key").exceptAll(existing_df.select("src_hash_key"))
ins_df = transformed_df.join(result_df.select("src_hash_key"), "src_hash_key", "inner")


# COMMAND ----------

ins_df.write.format("delta").mode("append").save("abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_STG/cad/excel_exchange_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC #Remove file in brob

# COMMAND ----------

[dbutils.fs.rm(s.path) for s in src_files]
