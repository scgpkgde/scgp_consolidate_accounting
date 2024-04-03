# Databricks notebook source
# MAGIC %md
# MAGIC # 🔃 Load the data from ADLS.

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws

# COMMAND ----------

# SOURCE_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/'
# DESTINATION_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_STG/scgp_fi_acct/'
 
# files = dbutils.fs.ls(SOURCE_PATH)

# COMMAND ----------

dbutils.fs.mkdirs('abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct')

# COMMAND ----------


# dbutils.fs.rm('abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/BS_FC_ACT.xlsx',True)

# COMMAND ----------

DESTINATION_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_STG/scgp_fi_acct/consol_gl/'
                 
# dbutils.fs.mkdirs(DESTINATION_PATH)
# dbutils.fs.rm(DESTINATION_PATH,True)

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl;

# CREATE TABLE IF NOT EXISTS scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl
# (
#   gl_account_number INT,
#   gl_account_name STRING,
#   gl_month INT,
#   gl_year INT,
#   gl_amount DOUBLE,
#   gl_src STRING,
#   scd_active INT,
#   scd_start TIMESTAMP,
#   scd_end TIMESTAMP,
#   src_hash_key STRING,
#   src_hash_diff STRING,
#   load_dts TIMESTAMP
# )
# USING DELTA
# COMMENT 'This table contains financial data related to the company\'s general ledger accounts.'
# LOCATION 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_STG/scgp_fi_acct/consol_gl/';


# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_bs;

# CREATE TABLE IF NOT EXISTS scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_bs
# ( 
#   bs_account_name STRING,
#   bs_month INT,
#   bs_year INT,
#   bs_amount DOUBLE,
#   bs_src STRING,
#   scd_active INT,
#   scd_start TIMESTAMP,
#   scd_end TIMESTAMP,
#   src_hash_key STRING,
#   src_hash_diff STRING,
#   load_dts TIMESTAMP
# )
# USING DELTA
# -- COMMENT 'This table contains financial data related to the company\'s general ledger accounts.'
# LOCATION 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_STG/scgp_fi_acct/consol_bs/';

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_ps;

# CREATE TABLE IF NOT EXISTS scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_ps
# ( 
#   ps_account STRING,
#   ps_product STRING,
#   ps_plant STRING,
#   ps_month INT,
#   ps_year INT,
#   ps_amount DOUBLE,
#   ps_src STRING,
#   scd_active INT,
#   scd_start TIMESTAMP,
#   scd_end TIMESTAMP,
#   src_hash_key STRING,
#   src_hash_diff STRING,
#   load_dts TIMESTAMP
# )
# USING DELTA
# -- COMMENT 'This table contains financial data related to the company\'s general ledger accounts.'
# LOCATION 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_STG/scgp_fi_acct/consol_ps/';

# COMMAND ----------

# %sql
# ALTER TABLE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl
# CLUSTER BY (gl_account_number,gl_month,gl_year)

# COMMAND ----------

 
SOURCE_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/'
files = dbutils.fs.ls(SOURCE_PATH)
print(files)

# COMMAND ----------

# MAGIC  %md
# MAGIC # &#127808; GL &#127808;

# COMMAND ----------

 
SOURCE_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/'
files = dbutils.fs.ls(SOURCE_PATH)
data_files_gl = [f.path for f in files if f.name.startswith('GL_')]

# Initialize an empty DataFrame to hold the appended data
first_file_schema = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat") \
        .option("header", "true")\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .option("inferSchema", "false") \
        .option("multiline", "true") \
        .load(data_files_gl[0]).schema
    
    # Create an empty DataFrame with the inferred schema
df_src_gl = spark.createDataFrame([], first_file_schema)

for src in data_files_gl: 
    df = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat") \
        .option("header", "true")\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .option("inferSchema", "false") \
        .option("multiline", "true") \
        .load(src)
    df.cache() 
 
    # Append the current DataFrame to the overall appended DataFrame
    df_src_gl = df_src_gl.union(df)

df_src_gl.count()
 
display(df_src_gl)

# COMMAND ----------

# MAGIC %md
# MAGIC #😉 Create hash column

# COMMAND ----------

df_src_gl = df_src_gl.withColumn("src_hash_key", sha2(
  concat_ws("_","gl_account_number","gl_month","gl_year","gl_src"), 256
  )
)

df_src_gl = df_src_gl.withColumn("src_hash_diff", sha2(
  concat_ws("_","gl_account_name","gl_amount"), 256
  )
)


# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type 2 operation

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl

# COMMAND ----------

sql_accounting_gl = "SELECT * FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl"
df_existing = spark.sql(sql_accounting_gl)
df_existing.count()
display(df_existing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🦋 Insert new row

# COMMAND ----------

diff_df = df_src_gl.join(df_existing,(df_src_gl["src_hash_key"] == df_existing["src_hash_key"]) & (df_src_gl["src_hash_diff"] == df_existing["src_hash_diff"]), "left_anti")
diff_df.count()
 


# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, to_date, lpad, current_timestamp

# diff_df = diff_df.withColumn("gl_month", lpad(col("gl_month"), 2, "0"))
diff_df = diff_df.withColumn("scd_active",lit(1))
diff_df = diff_df.withColumn("load_dts",current_timestamp())
diff_df = diff_df.withColumn("scd_start", to_date(concat("gl_year", lpad(col("gl_month"), 2, "0"), lit("01")), "yyyyMMdd"))
diff_df = diff_df.withColumn("scd_end", to_date(concat(lit("9999"), lit("12"), lit("31")), "yyyyMMdd"))
 
diff_df = diff_df.withColumn("gl_account_number", col("gl_account_number").cast("int"))
diff_df = diff_df.withColumn("gl_month", col("gl_month").cast("int"))
diff_df = diff_df.withColumn("gl_year", col("gl_year").cast("int"))
diff_df = diff_df.withColumn("gl_amount", col("gl_amount").cast("double"))
diff_df = diff_df.withColumn("scd_start", col("scd_start").cast("timestamp"))
diff_df = diff_df.withColumn("scd_end", col("scd_end").cast("timestamp"))

display(diff_df)

diff_df.write.format("delta").mode("append").saveAsTable('scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐛 Update (Soft Delete Existing) 

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH RankedDates AS (
# MAGIC   SELECT
# MAGIC     src_hash_key,
# MAGIC     load_dts,
# MAGIC     LEAD(load_dts) OVER (PARTITION BY src_hash_key ORDER BY load_dts) AS next_date
# MAGIC   FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl
# MAGIC )
# MAGIC UPDATE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl  
# MAGIC SET 
# MAGIC   scd_active = 0,
# MAGIC   scd_end = RankedDates.load_dts
# MAGIC FROM RankedDates
# MAGIC WHERE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl.src_hash_key = RankedDates.src_hash_key
# MAGIC AND scd_active = 1

# COMMAND ----------

# MAGIC  %md
# MAGIC # &#127808; BS &#127808;

# COMMAND ----------


SOURCE_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/'
files = dbutils.fs.ls(SOURCE_PATH)
data_files_bs = [f.path for f in files if f.name.startswith('BS_')]
# data_files_bs = [f.path for f in files if f.name.startswith('BS_') and f.name.endswith('.csv')]
# Initialize an empty DataFrame to hold the appended data
 
first_file_schema = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat") \
        .option("header", "true")\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .option("inferSchema", "false") \
        .option("multiline", "true") \
        .load(data_files_bs[0]).schema
    
    # Create an empty DataFrame with the inferred schema
df_src_bs = spark.createDataFrame([], first_file_schema)

for src in data_files_bs: 
    df = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat") \
        .option("header", "true")\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .option("inferSchema", "false") \
        .option("multiline", "true") \
        .load(src)
    df.cache() 
 
    # Append the current DataFrame to the overall appended DataFrame
    df_src_bs = df_src_bs.union(df)

# Display the first 200 rows of the appended DataFrame
df_src_bs.limit(200).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #😉 Create hash column

# COMMAND ----------

 
df_src_bs = df_src_bs.withColumn("src_hash_key", sha2(
  concat_ws("_","bs_account_name","bs_month","bs_year","bs_src"), 256
  )
)

df_src_bs = df_src_bs.withColumn("src_hash_diff", sha2(
  concat_ws("_","bs_amount"), 256
  )
)


# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type 2 operation

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_bs

# COMMAND ----------

sql_accounting_bs = "SELECT * FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_bs"
df_existing = spark.sql(sql_accounting_bs)
df_existing.count()
display(df_existing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🦋 Insert new row

# COMMAND ----------

from pyspark.sql.functions import col

diff_df = df_src_bs.join(df_existing,(df_src_bs["src_hash_key"] == df_existing["src_hash_key"]) & (df_src_bs["src_hash_diff"] == df_existing["src_hash_diff"]), "left_anti")
diff_df.count()

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, to_date, lpad, current_timestamp

# diff_df = diff_df.withColumn("gl_month", lpad(col("gl_month"), 2, "0"))
diff_df = diff_df.withColumn("scd_active",lit(1))
diff_df = diff_df.withColumn("load_dts",current_timestamp())
diff_df = diff_df.withColumn("scd_start", to_date(concat("bs_year", lpad(col("bs_month"), 2, "0"), lit("01")), "yyyyMMdd"))
diff_df = diff_df.withColumn("scd_end", to_date(concat(lit("9999"), lit("12"), lit("31")), "yyyyMMdd"))
 
diff_df = diff_df.withColumn("bs_month", col("bs_month").cast("int"))
diff_df = diff_df.withColumn("bs_year", col("bs_year").cast("int"))
diff_df = diff_df.withColumn("bs_amount", col("bs_amount").cast("double"))
diff_df = diff_df.withColumn("scd_start", col("scd_start").cast("timestamp"))
diff_df = diff_df.withColumn("scd_end", col("scd_end").cast("timestamp"))

display(diff_df)

diff_df.write.format("delta").mode("append").saveAsTable('scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_bs')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_bs

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_bs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐛 Update (Soft Delete Existing) 

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH RankedDates AS (
# MAGIC   SELECT
# MAGIC     src_hash_key,
# MAGIC     load_dts,
# MAGIC     LEAD(load_dts) OVER (PARTITION BY src_hash_key ORDER BY load_dts) AS next_date
# MAGIC   FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_bs
# MAGIC )
# MAGIC
# MAGIC UPDATE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_bs  
# MAGIC SET 
# MAGIC   scd_active = 0,
# MAGIC   scd_end = RankedDates.load_dts
# MAGIC FROM RankedDates
# MAGIC WHERE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl.src_hash_key = RankedDates.src_hash_key
# MAGIC AND scd_active = 1

# COMMAND ----------

# MAGIC  %md
# MAGIC # &#127808; PS &#127808;

# COMMAND ----------


SOURCE_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/'
files = dbutils.fs.ls(SOURCE_PATH)
data_files_ps = [f.path for f in files if f.name.startswith('PS_')]
# data_files_bs = [f.path for f in files if f.name.startswith('BS_') and f.name.endswith('.csv')]
# Initialize an empty DataFrame to hold the appended data
 
first_file_schema = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat") \
        .option("header", "true")\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .option("inferSchema", "false") \
        .option("multiline", "true") \
        .load(data_files_ps[0]).schema
    
    # Create an empty DataFrame with the inferred schema
df_src_ps = spark.createDataFrame([], first_file_schema)

for src in data_files_ps: 
    df = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat") \
        .option("header", "true")\
        .option("quote", "\"")\
        .option("escape", "\"")\
        .option("inferSchema", "false") \
        .option("multiline", "true") \
        .load(src)
    df.cache() 
 
    # Append the current DataFrame to the overall appended DataFrame
    df_src_ps = df_src_ps.union(df)

# Display the first 200 rows of the appended DataFrame
df_src_ps.limit(200).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #😉 Create hash column

# COMMAND ----------

df_src_ps = df_src_ps.withColumn("src_hash_key", sha2(
  concat_ws("_","ps_account","ps_product","ps_month","ps_year","ps_src"), 256
  )
)

df_src_ps = df_src_ps.withColumn("src_hash_diff", sha2(
  concat_ws("_","ps_amount","ps_plant"), 256
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD Type 2 operation

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_ps

# COMMAND ----------

sql_accounting_ps = "SELECT * FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_ps"
df_existing = spark.sql(sql_accounting_ps)
df_existing.count()
display(df_existing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🦋 Insert new row

# COMMAND ----------

from pyspark.sql.functions import col

diff_df = df_src_ps.join(df_existing,(df_src_ps["src_hash_key"] == df_existing["src_hash_key"]) & (df_src_ps["src_hash_diff"] == df_existing["src_hash_diff"]), "left_anti")
diff_df.count()

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, to_date, lpad, current_timestamp

# diff_df = diff_df.withColumn("gl_month", lpad(col("gl_month"), 2, "0"))
diff_df = diff_df.withColumn("scd_active",lit(1))
diff_df = diff_df.withColumn("load_dts",current_timestamp())
diff_df = diff_df.withColumn("scd_start", to_date(concat("ps_year", lpad(col("ps_month"), 2, "0"), lit("01")), "yyyyMMdd"))
diff_df = diff_df.withColumn("scd_end", to_date(concat(lit("9999"), lit("12"), lit("31")), "yyyyMMdd"))
 
diff_df = diff_df.withColumn("ps_month", col("ps_month").cast("int"))
diff_df = diff_df.withColumn("ps_year", col("ps_year").cast("int"))
diff_df = diff_df.withColumn("ps_amount", col("ps_amount").cast("double"))
diff_df = diff_df.withColumn("scd_start", col("scd_start").cast("timestamp"))
diff_df = diff_df.withColumn("scd_end", col("scd_end").cast("timestamp"))

display(diff_df)

diff_df.write.format("delta").mode("append").saveAsTable('scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_ps')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_ps

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_ps

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🐛 Update (Soft Delete Existing) 

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH RankedDates AS (
# MAGIC   SELECT
# MAGIC     src_hash_key,
# MAGIC     load_dts,
# MAGIC     LEAD(load_dts) OVER (PARTITION BY src_hash_key ORDER BY load_dts) AS next_date
# MAGIC   FROM scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_ps
# MAGIC )
# MAGIC
# MAGIC UPDATE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_ps  
# MAGIC SET 
# MAGIC   scd_active = 0,
# MAGIC   scd_end = RankedDates.load_dts
# MAGIC FROM RankedDates
# MAGIC WHERE scgp_edl_dev_uat.dev_scgp_edl_staging.accounting_gl.src_hash_key = RankedDates.src_hash_key
# MAGIC AND scd_active = 1

# COMMAND ----------


