# Databricks notebook source
dbutils.fs.rm('abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/data_e/20240318_Data_E2023.xlsx',True)

# COMMAND ----------

SOURCE_PATH = 'abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/data_e/'
files = dbutils.fs.ls(SOURCE_PATH)
data_files = [f.path for f in files]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import expr, col, substring
from functools import reduce
excel_file_path = "abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/data_e/20240318_Data_E2023.xlsx"

# dict_row = {'APPH': '46006:46308', 
#         'BATICO': '53006:53006:53308',
#         'CIP': '6006:6308', 
#         'CIP-BOX-OV': '18506:18808',
#         'CIP-BOX-OV-ID': '19506:19808',
#         'CIP-BOX-OV-VN': '19006:19308',
#         'CIP-BOX-TH': '18006:18308',
#         'CONIMEX': '52006:52308',
#         'Consol PKG': '6:308',
#         'Consol PKG-CN': '69506:69808',
#         'Consol PKG-ES': '4506:4808',
#         'Consol PKG-EU': '56006:56308',
#         'Consol PKG-ID': '2506:2808', 
#         'Consol PKG-MY': '3506:3808', 
#         'Consol PKG-NL': '71506:71808',
#         'Consol PKG-Others in ASEAN': '55506:55808',
#         'Consol PKG-PH': '3006:3308',
#         'Consol PKG-ROW': '56506:56808',
#         'Consol PKG-SG': '5006:5308', 
#         'Consol PKG-TH': '1506:1808',
#         'Consol PKG-UK': '4006:4308', 
#         'Consol PKG-US': '75506:75808',
#         'Consol PKG-VN': '2006:2308',
#         'DELTALAB': '55006:55308',
#         'DUYTAN': '53506:53808',
#         'EPSILON': '35506:35808',
#         'FAJAR': '38006:38308', 
#         'FAJAR-DOM': '64006:64308', 
#         'FAJAR-EXP': '68006:68308',
#         'FB': '1006:1308',
#         'FB-CN': '70506:70808', 
#         'FB-EU': '60506:60808', 
#         'FB-EXC FOOD GB': '23506:23808', 
#         'FB-FOOD GB': '24006:24308', 
#         'FB-ID': '59006:59308', 
#         'FB-MY': '25006:25308', 
#         'FB-Others in ASEAN': '60006:60308', 
#         'FB-PAPER-DOM': '65006:65308', 
#         'FB-PAPER-EXP': '69006:69308',
#         'FB-PH': '59506:59808',
#         'FB-PPW': '27006:27308', 
#         'FB-PPW-PAPER': '29006:29308', 
#         'FB-PPW-PAPER-FOOD': '29506:29808', 
#         'FB-PPW-PAPER-FOOD-OV': '30506:30808',
#         'FB-PPW-PAPER-FOOD-TH': '30006:30308',
#         'FB-PPW-PAPER-NON FOOD': '31006:31308',
#         'FB-PPW-PULP': '27506:27808', 
#         'FB-PPW-PULP-DP': '28006:28308',
#         'FB-PPW-PULP-NON DP': '28506:28808',
#         'FB-PULP-DOM': '64506:64808', 
#         'FB-PULP-EXP': '68506:68808', 
#         'FB-ROW': '61006:61308', 
#         'FB-SFT': '26506:26808', 
#         'FB-TH': '24506:24808', 
#         'FB-UK': '26006:26308', 
#         'FB-VN': '25506:25808',
#         'GoPak': '34506:34808',
#         'IHP': '54506:54808', 
#         'INDOCORR': '47506:47808',
#         'INDORIS': '47006:47308',
#         'INTAN': '48506:48808', 
#         'IPB': '506:808', 
#         'IPB-CN': '70006:70308',
#         'IPB-ES': '9006:9308',
#         'IPB-EU': '58006:58308', 
#         'IPB-ID': '8006:8308',
#         'IPB-MY': '57006:57308',
#         'IPB-Others in ASEAN': '57506:57808', 
#         'IPB-PH': '8506:8808',
#         'IPB-ROW': '58506:58808',
#         'IPB-SG': '9506:9808',
#         'IPB-TH': '7006:7308', 
#         'IPB-VN': '7506:7808', 
#         'IPSB': '35006:35308',
#         'IVQ': '32506:32808', 
#         'JTI': '73506:73808', 
#         'NAI': '44506:44808',
#         'NOVA': '74506:74808',
#         'Orient': '43006:43308',
#         'PEUTE': '72506:72808',
#         'PP': '5506:5808', 
#         'PP-CB': '10006:10308',
#         'PP-CB-ID': '12506:12808', 
#         'PP-CB-PH': '13006:13308', 
#         'PP-CB-TH': '10506:10808',
#         'PP-CB-TH-SKIC': '11006:11308',
#         'PP-CB-TH-TCP': '11506:11808',
#         'PP-CB-VN': '12006:12308', 
#         'PP-ID': '17006:17308',
#         'PPP': '6506:6808', 
#         'PP-PB': '13506:13808', 
#         'PP-PB-ID': '15506:15808', 
#         'PP-PB-TH': '14006:14308',
#         'PP-PB-TH-DPP': '14506:14808', 
#         'PP-PB-TH-PBL': '15006:15308', 
#         'PPPC': '33506:33808',
#         'PPP-FLEX': '65506:65808', 
#         'PP-PH': '17506:17808', 
#         'PPP-OV': '21506:21808',
#         'PPP-OV-FLEX': '22006:22308',
#         'PPP-OV-HEALTHCARE': '23006:23308', 
#         'PPP-OV-RIGID': '22506:22808',
#         'PPP-RIGID': '66006:66308',
#         'PPP-TH': '20006:20308', 
#         'PPP-TH-FLEX': '20506:20808', 
#         'PPP-TH-RIGID': '21006:21308',
#         'PP-TH': '16006:16308',
#         'PP-TH-DOM': '62506:62808',
#         'PP-TH-EXP': '66506:66808',
#         'PP-VN': '16506:16808', 
#         'PRECISION': '42506:42808',
#         'PREPACK': '50506:50808', 
#         'PRIMACORR': '46506:46808',
#         'PV': '45506:45808', 
#         'RB': '71006:71308',
#         'RB-NL': '72006:72308',
#         'RB-US': '76006:76308', 
#         'SCGP': '31506:31808', 
#         'SCGPE': '40006:40308', 
#         'SCGPRP': '52506:52808',
#         'SCGPRS': '54006:54308',
#         'SCGPS': '43506:43808',
#         'SCGPSS': '39506:39808',
#         'SCGPT': '51006:51308', 
#         'SFT': '33006:33308', 
#         'SHG': '62006:62308', 
#         'SIRIUS': '49006:49308',
#         'SKIC': '37006:37308', 
#         'SKIC INTER': '73006:73308',
#         'SNP': '36506:36808', 
#         'SOVI': '48006:48308',
#         'SPEC': '32006:32308',
#         'STP': '61506:61808',
#         'Surveyor': '74006:74308',
#         'TCFP': '50006:50308',
#         'TCG': '40506:40808', 
#         'TCGSS': '44006:44308', 
#         'TCKK': '41006:41308', 
#         'TCP': '37506:37808', 
#         'TCRY': '41506:41808',
#         'TPC': '34006:34308',
#         'TR': '75006:75308', 
#         'TWN': '42006:42308',
#         'UPPC': '38506:38808',
#         'UPPC-DOM': '63006:63308', 
#         'UPPC-EXP': '67006:67308',
#         'VEXCEL': '51506:51808',
#         'VKPC': '39006:39308',
#         'VKPC-DOM': '63506:63808',
#         'VKPC-EXP': '67506:67808'}

dict_row = {'Consol PKG': '5:10', 
         'IPB': '11:15'  }

# schema = StructType([ 
#     StructField("LINE_NO", StringType(), True),
#     StructField("DIMENSION", StringType(), True),
#     StructField("account", StringType(), True),
#     StructField("none_2", StringType(), True),
#     StructField("none_3", StringType(), True),
#     StructField("Jan", IntegerType(), True),
#     StructField("Feb", IntegerType(), True),
#     StructField("Mar", IntegerType(), True),
#     StructField("Apr", IntegerType(), True),
#     StructField("May", IntegerType(), True),
#     StructField("Jun", IntegerType(), True),
#     StructField("Jul", IntegerType(), True),
#     StructField("Aug", IntegerType(), True),
#     StructField("Sep", IntegerType(), True),
#     StructField("Oct", IntegerType(), True),
#     StructField("Nov", IntegerType(), True),
#     StructField("Dec", IntegerType(), True),
#     StructField("Jan_2", IntegerType(), True),
#     StructField("Jan_Feb", IntegerType(), True),
#     StructField("Jan_Mar", IntegerType(), True),
#     StructField("Jan_Apr", IntegerType(), True),
#     StructField("Jan_May", IntegerType(), True),
#     StructField("Jan_Jun", IntegerType(), True),
#     StructField("Jan_Jul", IntegerType(), True),
#     StructField("Jan_Aug", IntegerType(), True),
#     StructField("Jan_Sep", IntegerType(), True),
#     StructField("Jan_Oct", IntegerType(), True),
#     StructField("Jan_Nov", IntegerType(), True),
#     StructField("Jan_Dec", IntegerType(), True),
#     StructField("Q1", IntegerType(), True),
#     StructField("Q2", IntegerType(), True),
#     StructField("Q3", IntegerType(), True),
#     StructField("Q4", IntegerType(), True),
#     StructField("H1", IntegerType(), True),
#     StructField("H2", IntegerType(), True),
#     StructField("Year", IntegerType(), True)
# ])

dict_row = {'Consol PKG': '6:10', 
         'IPB': '11:15'}

ans_df = None
# ans_df.unpersist()

for key, value in dict_row.items():
    row_from, row_to = map(int, value.split(':')) 
    print(key)

    df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("header", "false") \
            .option("dataAddress", f"'Data_2023'!B{row_from}:AK{row_to}") \
            .option("inferSchema", "true") \
            .load(excel_file_path)\
            .repartition(8)    

    if ans_df is None:
        ans_df = df
    else:
        ans_df = ans_df.union(df)
    ans_df.cache() 

ans_df.unpersist()
# ans_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎄 Ingest Sheet CostA2023

# COMMAND ----------

    
#     data_costa2023 = {
#     "CIP": "B806:AK1000",
#     "FAJAR": "B606:AK800",
#     "FB-Paper": "B2006:AK2200",
#     "FB-PPW-PULP-DP": "B2206:AK2400",
#     "FB-PPW-PULP-NON DP": "B2406:AK2600",
#     "FB-Pulp": "B1806:AK2000",
#     "Flexible": "B1006:AK1200",
#     "Healthcare": "B1606:AK1800",
#     "PP": "B2806:AK2905",
#     "PPP exc. Healthcare": "B1406:AK1505",
#     "PP-TH": "B6:AK105",
#     "Rigid": "B1206:AK1400",
#     "UPPC": "B206:AK400",
#     "VKPC": "B406:AK600",
#     "Blank": "B2606:AK2800"
# }
ans_df = None

for key, value in dict_row.items():

    row_from, row_to = map(value.split(':')) 

    df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("header", "false") \
        .option("dataAddress", f"'CostA2023'!{row_from}:{row_to}") \
        .option("inferSchema", "true") \
        .load(excel_file_path)\

    if ans_df is None:
        ans_df = df
    else:
        ans_df = ans_df.union(df)

    ans_df.cache() 





# COMMAND ----------

# MAGIC %md 
# MAGIC  🐨

# COMMAND ----------

import pyspark.pandas as pd
# import koalas as ks
lst_expected_columns = ["none", "LINE NO.", "DIMENSION", "account", "none", "none", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Jan-Feb", "Jan-Mar", "Jan-Apr", "Jan-May", "Jan-Jun", "Jan-Jul", "Jan-Aug", "Jan-Sep", "Jan-Oct", "Jan-Nov", "Jan-Dec", "Q1", "Q2", "Q3", "Q4", "H1", "H2", "Year"]

dict_row = {'Consol PKG': '6:10', 
         'IPB': '11:15'}
# df = pd.read_excel("./Data_E2023.xlsm", sheet_name="A2023", header=None)
# excel_file_path = "abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/data_e/20240318_Data_E2023.xlsx"

abfss_path = "abfss://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/data_e/20240318_Data_E2023.xlsx"

# Use PySpark to read the Excel file into a DataFrame
df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "false") \
    .load(abfss_path)

# Convert PySpark DataFrame to Pandas DataFrame
df = df.toPandas()
 

ans_df = pd.DataFrame() 
for key, value in dict_row.items():

    row_from = int(value.split(':')[0])-1
    row_to = int(value.split(':')[1])
    desired_range = df.iloc[row_from:row_to, 1:37]
    ans_df = pd.concat([ans_df, desired_range], ignore_index=True)

column_names = {i: lst_expected_columns[i] for i in range(len(lst_expected_columns))}
ans_df.rename(columns=column_names, inplace=True)
      
ans_df.dropna(subset=['account'], inplace=True)
ans_df = ans_df[ans_df['account'] != 0]
print(ans_df)

# COMMAND ----------



# COMMAND ----------

import pandas as pd

excel_file_path_new = "adl://scgpdldev@scgpkgdldevhot.dfs.core.windows.net/EDW_DATA_LANDING/scgp_fi_acct/data_e/20240318_Data_E2023.xlsx"

df = pd.read_excel(excel_file_path_new)

# COMMAND ----------


