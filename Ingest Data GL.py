# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import lit, col

SOURCE_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/data_management/authorization/20240315_11. D09_Financial Ratio_CIP.xlsx'
 
df = spark.read \
.format("com.crealytics.spark.excel") \
.option("header", "true") \
.option("inferSchema", "true") \
.option("dataAddress", "'BPC BS V'!B4:EP286") \
.load(SOURCE_PATH)
 
selected_column = df.select(df['_c0']) 
collected_values = selected_column.collect() 
value_at_index_8 = collected_values[3][0] 
df = df.withColumn("year_month", lit(value_at_index_8)) 

df = df.select("ID", "Description", "A0000 - Statutory Reporting", "year_month") \
               .withColumnRenamed("ID", "gl_account_number") \
               .withColumnRenamed("Description", "gl_account_name") \
               .withColumnRenamed("A0000 - Statutory Reporting", "amount")

# df = df.withColumn("year_month", lit(value_at_index_8))

df.display()



# COMMAND ----------


