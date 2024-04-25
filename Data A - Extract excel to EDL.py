# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, DecimalType, BooleanType
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.functions import col, concat, lit, to_date, lpad, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC # 🥸 Insert excel to volume

# COMMAND ----------

import os

SOURCE_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/data_a/'
src_files = dbutils.fs.ls(SOURCE_PATH)
 
for f in src_files:
  adls_file_path = f.path
  file_name = os.path.basename(f.path) 
  volume_file_path = f"dbfs:/Volumes/scgp_edl_dev_uat/dev_scgp_edl_landing/tmp_excel_file/{file_name}"
  dbutils.fs.cp(adls_file_path, volume_file_path)
 
VOLUME_PATH = 'dbfs:/Volumes/scgp_edl_dev_uat/dev_scgp_edl_landing/tmp_excel_file/'
volume_files = dbutils.fs.ls(VOLUME_PATH)

# COMMAND ----------

import glob

prefix = "Data_A"
files = [file_info for file_info in volume_files if prefix in file_info.name][0]

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎃 Extract data from the excel Sheet: A2023

# COMMAND ----------

#loop list store file
try: 
      year = files.name.split('_')[2].split('.')[0][1:5]
      lst_expected_columns = ["Line_No", "Dimension", "Account", "none1", "none2", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "avg_Jan", "avg_Jan-Feb", "avg_Jan-Mar", "avg_Jan-Apr", "avg_Jan-May", "avg_Jan-Jun", "avg_Jan-Jul", "avg_Jan-Aug", "avg_Jan-Sep", "avg_Jan-Oct", "avg_Jan-Nov", "avg_Jan-Dec", "avg_Q1", "avg_Q2", "avg_Q3", "avg_Q4", "avg_H1", "avg_H2", "avg_Year"]
     
      data_types = {'Line_No':'str','Dimension':'str','Account':'str','none1':'str','none2':'str',
                          'Jan':'float64','Feb':'float64','Mar':'float64','Apr':'float64','May':'float64',
                          'Jun':'float64','Jul':'float64','Aug':'float64','Sep':'float64','Oct':'float64',
                          'Nov':'float64','Dec':'float64','avg_Jan':'float64','avg_Jan-Feb':'float64','avg_Jan-Mar':'float64','avg_Jan-Apr':'float64','avg_Jan-May':'float64',
                          'avg_Jan-Jun':'float64','avg_Jan-Jul':'float64','avg_Jan-Aug':'float64',
                          'avg_Jan-Sep':'float64','avg_Jan-Oct':'float64','avg_Jan-Nov':'float64',
                          'avg_Jan-Dec':'float64','avg_Q1':'float64','avg_Q2':'float64','avg_Q3':'float64','avg_Q4':'float64','avg_H1':'float64','avg_H2':'float64','avg_Year':'float64'}
    
      # Loop get all file in the folder
      df_a = pd.read_excel(files.path.replace('dbfs:', ''), sheet_name=f"A{year}", header=None, dtype=data_types)

      ans_df_a = pd.DataFrame()   
      dict_row = {
                'APV':'45006:45308',
                'APPH': '46006:46308', 
                'BATICO': '53006:53308',
                'CIP': '6006:6308', 
                'CIP-BOX-OV': '18506:18808',
                'CIP-BOX-OV-ID': '19506:19808',
                'CIP-BOX-OV-VN': '19006:19308',
                'CIP-BOX-TH': '18006:18308',
                'CONIMEX': '52006:52308',
                'Consol PKG': '6:308',
                'Consol PKG-CN': '69506:69808',
                'Consol PKG-ES': '4506:4808',
                'Consol PKG-EU': '56006:56308',
                'Consol PKG-ID': '2506:2808', 
                'Consol PKG-MY': '3506:3808', 
                'Consol PKG-NL': '71506:71808',
                'Consol PKG-Others in ASEAN': '55506:55808',
                'Consol PKG-PH': '3006:3308',
                'Consol PKG-ROW': '56506:56808',
                'Consol PKG-SG': '5006:5308', 
                'Consol PKG-TH': '1506:1808',
                'Consol PKG-UK': '4006:4308', 
                'Consol PKG-US': '75506:75808',
                'Consol PKG-VN': '2006:2308',
                'DELTALAB': '55006:55308',
                'DUYTAN': '53506:53808',
                'EPSILON': '35506:35808',
                'FAJAR': '38006:38308', 
                'FAJAR-DOM': '64006:64308', 
                'FAJAR-EXP': '68006:68308',
                'FB': '1006:1308',
                'FB-CN': '70506:70808', 
                'FB-EU': '60506:60808', 
                'FB-EXC FOOD GB': '23506:23808', 
                'FB-FOOD GB': '24006:24308', 
                'FB-ID': '59006:59308', 
                'FB-MY': '25006:25308', 
                'FB-Others in ASEAN': '60006:60308', 
                'FB-PAPER-DOM': '65006:65308', 
                'FB-PAPER-EXP': '69006:69308',
                'FB-PH': '59506:59808',
                'FB-PPW': '27006:27308', 
                'FB-PPW-PAPER': '29006:29308', 
                'FB-PPW-PAPER-FOOD': '29506:29808', 
                'FB-PPW-PAPER-FOOD-OV': '30506:30808',
                'FB-PPW-PAPER-FOOD-TH': '30006:30308',
                'FB-PPW-PAPER-NON FOOD': '31006:31308',
                'FB-PPW-PULP': '27506:27808', 
                'FB-PPW-PULP-DP': '28006:28308',
                'FB-PPW-PULP-NON DP': '28506:28808',
                'FB-PULP-DOM': '64506:64808', 
                'FB-PULP-EXP': '68506:68808', 
                'FB-ROW': '61006:61308', 
                'FB-SFT': '26506:26808', 
                'FB-TH': '24506:24808', 
                'FB-UK': '26006:26308', 
                'FB-VN': '25506:25808',
                'GoPak': '34506:34808',
                'IHP': '54506:54808', 
                'INDOCORR': '47506:47808',
                'INDORIS': '47006:47308',
                'INTAN': '48506:48808', 
                'IPB': '506:808', 
                'IPB-CN': '70006:70308',
                'IPB-ES': '9006:9308',
                'IPB-EU': '58006:58308', 
                'IPB-ID': '8006:8308',
                'IPB-MY': '57006:57308',
                'IPB-Others in ASEAN': '57506:57808', 
                'IPB-PH': '8506:8808',
                'IPB-ROW': '58506:58808',
                'IPB-SG': '9506:9808',
                'IPB-TH': '7006:7308', 
                'IPB-VN': '7506:7808', 
                'IPSB': '35006:35308',
                'IVQ': '32506:32808', 
                'JTI': '73506:73808', 
                'NAI': '44506:44808',
                'NOVA': '74506:74808',
                'Orient': '43006:43308',
                'PEUTE': '72506:72808',
                'PP': '5506:5808', 
                'PP-CB': '10006:10308',
                'PP-CB-ID': '12506:12808', 
                'PP-CB-PH': '13006:13308', 
                'PP-CB-TH': '10506:10808',
                'PP-CB-TH-SKIC': '11006:11308',
                'PP-CB-TH-TCP': '11506:11808',
                'PP-CB-VN': '12006:12308', 
                'PP-ID': '17006:17308',
                'PPP': '6506:6808', 
                'PP-PB': '13506:13808', 
                'PP-PB-ID': '15506:15808', 
                'PP-PB-TH': '14006:14308',
                'PP-PB-TH-DPP': '14506:14808', 
                'PP-PB-TH-PBL': '15006:15308', 
                'PPPC': '33506:33808',
                'PPP-FLEX': '65506:65808', 
                'PP-PH': '17506:17808', 
                'PPP-OV': '21506:21808',
                'PPP-OV-FLEX': '22006:22308',
                'PPP-OV-HEALTHCARE': '23006:23308', 
                'PPP-OV-RIGID': '22506:22808',
                'PPP-RIGID': '66006:66308',
                'PPP-TH': '20006:20308', 
                'PPP-TH-FLEX': '20506:20808', 
                'PPP-TH-RIGID': '21006:21308',
                'PP-TH': '16006:16308',
                'PP-TH-DOM': '62506:62808',
                'PP-TH-EXP': '66506:66808',
                'PP-VN': '16506:16808', 
                'PRECISION': '42506:42808',
                'PREPACK': '50506:50808', 
                'PRIMACORR': '46506:46808',
                'PV': '45506:45808', 
                'RB': '71006:71308',
                'RB-NL': '72006:72308',
                'RB-US': '76006:76308', 
                'SCGP': '31506:31808', 
                'SCGPE': '40006:40308', 
                'SCGPRP': '52506:52808',
                'SCGPRS': '54006:54308',
                'SCGPS': '43506:43808',
                'SCGPSS': '39506:39808',
                'SCGPT': '51006:51308', 
                'SFT': '33006:33308', 
                'SHG': '62006:62308', 
                'SIRIUS': '49006:49308',
                'SKIC': '37006:37308', 
                'SKIC INTER': '73006:73308',
                'SNP': '36506:36808', 
                'SOVI': '48006:48308',
                'SPEC': '32006:32308',
                'STP': '61506:61808',
                'Surveyor': '74006:74308',
                'TCFP': '50006:50308',
                'TCG': '40506:40808', 
                'TCGSS': '44006:44308', 
                'TCKK': '41006:41308', 
                'TCP': '37506:37808', 
                'TCRY': '41506:41808',
                'TPC': '34006:34308',
                'TR': '75006:75308', 
                'TWN': '42006:42308',
                'UPPC': '38506:38808',
                'UPPC-DOM': '63006:63308', 
                'UPPC-EXP': '67006:67308',
                'VEXCEL': '51506:51808',
                'VKPC': '39006:39308',
                'VKPC-DOM': '63506:63808',
                'VKPC-EXP': '67506:67808'}
    
      for key, value in dict_row.items(): 
          row_from = int(value.split(':')[0])-1
          row_to = int(value.split(':')[1])
          desired_range = df_a.iloc[row_from:row_to, 1:37]
          ans_df_a = pd.concat([ans_df_a, desired_range], ignore_index=True) 

      column_names = {i+1: lst_expected_columns[i] for i in range(len(lst_expected_columns))}
      ans_df_a.rename(columns=column_names, inplace=True)
      ans_df_a.dropna(subset=['Account'], inplace=True)
      ans_df_a = ans_df_a[ans_df_a['Account'] != 0]
      ans_df_a.drop(columns=['none1', 'none2'], inplace=True) 
      ans_df_a['Year'] = year
      ans_df_a = ans_df_a.melt(id_vars=["Line_No", "Dimension", "Account","Year"], var_name="Month", value_name="Amount")
      ans_df_a['Amount'].fillna(0, inplace=True)
 
except Exception as e:  
  print(e)

# COMMAND ----------

psdf_a = spark.createDataFrame(ans_df_a)

# COMMAND ----------

# psdf_a.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #😉 Create hash column

# COMMAND ----------

psdf_a = psdf_a.withColumn("src_hash_key", sha2(
  concat_ws("_","Line_No","Dimension","Year","Month"), 256
  )
)
psdf_a = psdf_a.withColumn("src_hash_diff", sha2(
  concat_ws("_","Account","Amount"), 256
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #✔ Cast Data Type

# COMMAND ----------

psdf_a = psdf_a.withColumn("scd_active", lit(1).cast(BooleanType()))
psdf_a = psdf_a.withColumn("scd_start",current_timestamp())
psdf_a = psdf_a.withColumn("Amount", col("Amount").cast(DecimalType(20, 4)))
psdf_a = psdf_a.withColumn("Year", col("Year").cast("int"))
psdf_a = psdf_a.withColumn("scd_end", to_date(concat(lit("9999"), lit("12"), lit("31")), "yyyyMMdd").cast("timestamp"))

# COMMAND ----------

psdf_a.cache()

# COMMAND ----------

#Create tempview for select
psdf_a.createOrReplaceTempView("tmp_source_dataa")

# COMMAND ----------

#For Innitial
# psdf_a.write.format("delta").mode("append").saveAsTable('scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_datae_consolidate_data')

# COMMAND ----------

# MAGIC %md
# MAGIC #🐛 Update

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW 
# MAGIC tmp_excel_cad_dataa_consolidate_data_upd
# MAGIC AS
# MAGIC SELECT * FROM tmp_source_dataa A
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_dataa_consolidate_data B 
# MAGIC     WHERE A.src_hash_key = B.src_hash_key 
# MAGIC     AND  A.src_hash_diff = B.src_hash_diff 
# MAGIC     AND B.scd_active = 1
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_dataa_consolidate_data T1
# MAGIC USING tmp_excel_cad_dataa_consolidate_data_upd T2
# MAGIC ON T1.src_hash_key = T2.src_hash_key AND T1.scd_active = 1
# MAGIC WHEN MATCHED THEN UPDATE SET T1.scd_active = 0, T1.scd_end = CURRENT_TIMESTAMP()

# COMMAND ----------

# MAGIC %md
# MAGIC #🐛 Insert

# COMMAND ----------

# MAGIC %sql  
# MAGIC INSERT INTO scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_dataa_consolidate_data (Line_No, Dimension, Year, Month, Amount, Account, scd_active, scd_start, scd_end, src_hash_key, src_hash_diff)
# MAGIC SELECT Line_No, Dimension, Year, Month, Amount, Account, scd_active, scd_start, scd_end, src_hash_key, src_hash_diff
# MAGIC FROM tmp_excel_cad_dataa_consolidate_data_upd

# COMMAND ----------

psdf_a.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC # 🐹 Extract data from the excel Sheet: CostA2023

# COMMAND ----------

#loop list store file
try: 
      year = files.name.split('_')[2].split('.')[0][1:5]
      lst_expected_columns = ["Line_No", "Dimension", "Items", "Unit", "none", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "avg_Jan", "avg_Jan-Feb", "avg_Jan-Mar", "avg_Jan-Apr", "avg_Jan-May", "avg_Jan-Jun", "avg_Jan-Jul", "avg_Jan-Aug", "avg_Jan-Sep", "avg_Jan-Oct", "avg_Jan-Nov", "avg_Jan-Dec", "avg_Q1", "avg_Q2", "avg_Q3", "avg_Q4", "avg_H1", "avg_H2", "avg_Year"]
      
      data_types = {'Line_No':'str','Dimension':'str','Items':'str','Unit':'str','none':'str',
                    'Jan':'float64','Feb':'float64','Mar':'float64','Apr':'float64','May':'float64',
                    'Jun':'float64','Jul':'float64','Aug':'float64','Sep':'float64','Oct':'float64',
                    'Nov':'float64','Dec':'float64','avg_Jan':'float64','avg_Jan-Feb':'float64',                  'avg_Jan-Mar':'float64','avg_Jan-Apr':'float64','avg_Jan-May':'float64',
                    'avg_Jan-Jun':'float64','avg_Jan-Jul':'float64','avg_Jan-Aug':'float64',
                    'avg_Jan-Sep':'float64','avg_Jan-Oct':'float64','avg_Jan-Nov':'float64',
                    'avg_Jan-Dec':'float64','avg_Q1':'float64','avg_Q2':'float64','avg_Q3':'float64',
                    'avg_Q4':'float64','avg_H1':'float64','avg_H2':'float64','avg_Year':'float64'}
    
      # Loop get all file in the folder
      df_cost = pd.read_excel(files.path.replace('dbfs:', ''), sheet_name=f"CostA{year}", header=None, dtype=data_types)
      ans_df_cost = pd.DataFrame()   
      dict_row = {
            "CIP": "806:1000",
            "FAJAR": "606:800",
            "FB-Paper": "2006:2200",
            "FB-PPW-PULP-DP": "2206:2400",
            "FB-PPW-PULP-NON DP": "2406:2600",
            "FB-Pulp": "1806:2000",
            "Flexible": "1006:1200",
            "Healthcare": "1606:1800",
            "PP": "2806:2905",
            "PPP exc. Healthcare": "1406:1505",
            "PP-TH": "6:200",
            "Rigid": "1206:1400",
            "UPPC": "206:400",
            "VKPC": "406:600",
            "Blank": "2606:2800"
      }
          
      for key, value in dict_row.items(): 
          row_from = int(value.split(':')[0])-1
          row_to = int(value.split(':')[1])
          desired_range = df_cost.iloc[row_from:row_to, 1:37]
          ans_df_cost = pd.concat([ans_df_cost, desired_range], ignore_index=True) 

      # column_names = {i: lst_expected_columns[i] for i in range(len(lst_expected_columns))}
      column_names = {i+1: lst_expected_columns[i] for i in range(len(lst_expected_columns))}
      ans_df_cost.rename(columns=column_names, inplace=True)

      ans_df_cost.dropna(subset=['Unit'], inplace=True) 
      ans_df_cost.drop(columns='none', inplace=True)
      ans_df_cost['Year'] = year
      ans_df_cost = ans_df_cost.melt(id_vars=["Line_No", "Dimension", "Items", "Unit", "Year"], var_name="Month", value_name="Amount")
      ans_df_cost['Amount'].fillna(0, inplace=True)
      ans_df_cost =  ans_df_cost[ans_df_cost['Line_No'] != 0]
      ans_df_cost = ans_df_cost[ans_df_cost['Unit'] != 0]
   
except Exception as e:  
      print(e)

# COMMAND ----------

psdf_cost = spark.createDataFrame(ans_df_cost)

# COMMAND ----------

# MAGIC %md
# MAGIC #😉 Create hash column

# COMMAND ----------

psdf_cost = psdf_cost.withColumn("src_hash_key", sha2(
  concat_ws("_","Line_No","Dimension","Year","Month"), 256
  )
)

psdf_cost = psdf_cost.withColumn("src_hash_diff", sha2(
  concat_ws("_","Items","Unit","Amount"), 256
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #✔ Cast Data Type

# COMMAND ----------

psdf_cost = psdf_cost.withColumn("scd_active", lit(1).cast(BooleanType()))
psdf_cost = psdf_cost.withColumn("scd_start",current_timestamp())
psdf_cost = psdf_cost.withColumn("Amount", col("Amount").cast(DecimalType(20, 4)))
psdf_cost = psdf_cost.withColumn("Year", col("Year").cast("int"))
psdf_cost = psdf_cost.withColumn("scd_end", to_date(concat(lit("9999"), lit("12"), lit("31")), "yyyyMMdd").cast("timestamp"))

# COMMAND ----------

#For Innitial
# psdf_cost.write.format("delta").mode("append").saveAsTable('scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_datae_costa_consolidate_data')

# COMMAND ----------

psdf_cost.createOrReplaceTempView("tmp_source_dataa_cost")

# COMMAND ----------

# MAGIC %md
# MAGIC #🐛 Update

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW 
# MAGIC tmp_excel_cad_dataa_consolidate_cost_data_upd
# MAGIC AS
# MAGIC SELECT * FROM tmp_source_dataa_cost A
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_dataa_costa_consolidate_data B 
# MAGIC     WHERE A.src_hash_key = B.src_hash_key 
# MAGIC     AND  A.src_hash_diff = B.src_hash_diff 
# MAGIC     AND B.scd_active = 1
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_dataa_costa_consolidate_data T1
# MAGIC USING tmp_excel_cad_dataa_consolidate_cost_data_upd T2
# MAGIC ON T1.src_hash_key = T2.src_hash_key AND T1.scd_active = 1
# MAGIC WHEN MATCHED THEN UPDATE SET T1.scd_active = 0 , T1.scd_end = CURRENT_TIMESTAMP()

# COMMAND ----------

# MAGIC %md
# MAGIC #🐛 Insert

# COMMAND ----------

# MAGIC %sql  
# MAGIC INSERT INTO scgp_edl_dev_uat.dev_scgp_edl_staging.excel_cad_dataa_costa_consolidate_data (Line_No, Dimension, Items, Unit, Year, Month, Amount, scd_active, scd_start, scd_end, src_hash_key, src_hash_diff)
# MAGIC SELECT Line_No, Dimension, Items, Unit, Year, Month, Amount, scd_active, scd_start, scd_end, src_hash_key, src_hash_diff
# MAGIC FROM tmp_excel_cad_dataa_consolidate_cost_data_upd

# COMMAND ----------

# MAGIC %md
# MAGIC # 🗑 Remove Files 

# COMMAND ----------

[dbutils.fs.rm(s.path) for s in src_files]
dbutils.fs.rm(files.path)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE tmp_excel_cad_dataa_consolidate_data_upd;
