# Databricks notebook source
# MAGIC %md
# MAGIC #Extract data from the excel Sheet: 2025

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, substring , split , lit , stack ,  explode, col,array,monotonically_increasing_id
import pandas as pd
import re
from datetime import datetime

SOURCE_PATH = "abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/exchange_rate/"
src_files = dbutils.fs.ls(SOURCE_PATH)


# COMMAND ----------

import os

for f in src_files:
  adls_file_path = f.path
  file_name = os.path.basename(f.path) 
  volume_file_path = f"dbfs:/Volumes/scgp_edl_dev_uat/dev_scgp_edl_landing/tmp_excel_file/{file_name}"
  dbutils.fs.cp(adls_file_path, volume_file_path)




# COMMAND ----------

import glob

VOLUME_PATH = 'dbfs:/Volumes/scgp_edl_dev_uat/dev_scgp_edl_landing/tmp_excel_file/'
volume_files = dbutils.fs.ls(VOLUME_PATH)
current_date = datetime.now()
formatted_date = current_date.strftime("%Y%m%d")
prefix = f"{formatted_date}_Exchange rate"
_files = [file_info for file_info in volume_files if prefix in file_info.name]

print(_files)

# COMMAND ----------

excel_file_path = _files[0].path.replace('dbfs:', '')
print(excel_file_path)

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import lit
import re

# ฟังก์ชันสำหรับลบตัวเลขที่ท้ายชื่อคอลัมน์
def remove_numbers_from_last_index(column_name):
    return re.sub(r'\d+$', '', column_name)

# ดึงปีจากข้อมูล
data_year = str(df_pandas_year.iloc[0, 0])[-4:].strip()
print(data_year)

# อ่านข้อมูลจากไฟล์ Excel ด้วย pandas
df_pandas_main = pd.read_excel(excel_file_path, sheet_name=data_year, skiprows=2)  # อ่านข้อมูลหลัก
df_pandas_main = df_pandas_main.dropna()  # ลบแถวที่มีค่า null


df_pandas_year = pd.read_excel(excel_file_path, sheet_name=data_year, nrows=2, usecols="A")  # อ่านปี
df_pandas_country = pd.read_excel(excel_file_path, sheet_name=data_year, skiprows=1, nrows=1, usecols="B:Y")  # อ่านชื่อประเทศ



# ดึงชื่อประเทศ
country_column_names = [col for col in df_pandas_country.columns if not col.startswith("Unnamed")]
print(country_column_names)

df_pandas_main = df_pandas_main.rename(columns={'Unnamed: 0': 'Month'})
df_pandas_main = df_pandas_main.loc[:, ~df_pandas_main.columns.str.startswith('Unnamed:')]

# สร้างชื่อคอลัมน์ใหม่
new_column_names = ['Month'] + [
    f"{country}_{remove_numbers_from_last_index(col)}"
    for country in country_column_names
    for col in df_pandas_main.columns[1:4]  # เลือกเฉพาะคอลัมน์ที่ต้องการ
]
print(new_column_names)

df_pandas_main.columns = new_column_names

df_spark_main = spark.createDataFrame(df_pandas_main)
# display(df_spark_main)

def transform_dataframe(df, year):
    melted_df = df.selectExpr(
        "Month",  # เก็บคอลัมน์ Month ไว้
        "stack({}, {}) as (Currency, Currency_Type, Conversion_Rate)".format(
            len(df.columns) - 1,  # จำนวนคอลัมน์ที่ต้อง stack (ไม่รวม Month)
            ", ".join([
                f"'{col_name.split('_')[0]}', '{col_name.split('_')[1]}', `{col_name}`"
                for col_name in df.columns[1:]  # ข้ามคอลัมน์แรก (Month)
            ])
        )
    )
    return melted_df.withColumn("Year", lit(year))
transformed_df = transform_dataframe(df_spark_main, data_year)

display(transformed_df)

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

transformed_df = transformed_df.withColumn("Conversion_Rate", col("Conversion_Rate").cast(DecimalType(20, 4)))
transformed_df = transformed_df.withColumn("scd_active", lit(1).cast(BooleanType()))
transformed_df = transformed_df.withColumn("scd_start",current_timestamp())
transformed_df = transformed_df.withColumn("scd_end", to_date(concat(lit("9999"), lit("12"), lit("31")), "yyyyMMdd").cast("timestamp"))

# COMMAND ----------

transformed_df.cache()

# COMMAND ----------

psdf = transformed_df

# COMMAND ----------

psdf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Update

# COMMAND ----------

psdf.createOrReplaceTempView("tmp_source_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW
# MAGIC tmp_excel
# MAGIC AS
# MAGIC SELECT * FROM tmp_source_data A
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_exchange_rate  B
# MAGIC     WHERE A.src_hash_key = B.src_hash_key
# MAGIC     AND  A.src_hash_diff = B.src_hash_diff
# MAGIC     AND B.scd_active = TRUE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from tmp_excel

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_exchange_rate T1
# MAGIC USING tmp_source_data T2
# MAGIC ON T1.src_hash_key = T2.src_hash_key 
# MAGIC AND T1.src_hash_diff != T2.src_hash_diff
# MAGIC AND T1.scd_active = TRUE
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET 
# MAGIC T1.scd_active = FALSE 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Insert

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_exchange_rate (
# MAGIC   Year,
# MAGIC   Month,
# MAGIC   Currency,
# MAGIC   Currency_Type,
# MAGIC   Conversion_Rate,
# MAGIC   scd_active,
# MAGIC   scd_start,
# MAGIC   scd_end,
# MAGIC   src_hash_key,
# MAGIC   src_hash_diff
# MAGIC )
# MAGIC SELECT
# MAGIC   Year,
# MAGIC   Month,
# MAGIC   Currency,
# MAGIC   Currency_Type,
# MAGIC   Conversion_Rate,
# MAGIC   scd_active,
# MAGIC   scd_start,
# MAGIC   scd_end,
# MAGIC   src_hash_key,
# MAGIC   src_hash_diff
# MAGIC FROM tmp_excel T2
# MAGIC WHERE NOT EXISTS
# MAGIC (
# MAGIC   SELECT 1 FROM scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_exchange_rate T1_sub 
# MAGIC   WHERE T1_sub.src_hash_key = T2.src_hash_key 
# MAGIC   AND T1_sub.scd_active = 1
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #Remove file in brob

# COMMAND ----------

[dbutils.fs.rm(s.path) for s in src_files]

for files in _files:
  print(files)
  dbutils.fs.rm(files.path)
