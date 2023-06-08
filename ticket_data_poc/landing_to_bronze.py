# Databricks notebook source
# MAGIC %md
# MAGIC ###` Tickets Data Landing To Bronze Process`
# MAGIC
# MAGIC * __`Bronze process  From Landing To Bronze Delta Tables Process`__
# MAGIC
# MAGIC #### `Validations`
# MAGIC *  `Metadata Validations will be done in this step based on columns and data types with delimiter using pyspark schema `
# MAGIC
# MAGIC ###Details
# MAGIC
# MAGIC | Details | Information 
# MAGIC |----|-----
# MAGIC |Notebook Created By | Raveendra  
# MAGIC |Object Name | Tickets  data Processing
# MAGIC |File Type | delimited file (csv)
# MAGIC |Target Location | Databricks Delta Tables 
# MAGIC
# MAGIC ###History
# MAGIC |Date | Developed By | comments
# MAGIC |----|-----|----
# MAGIC |01/06/2022|Ravendra| Initial Version
# MAGIC | Find more Videos | Youtube   | <a href="https://www.youtube.com/watch?v=FpxkiGPFyfM&list=PL50mYnndduIHRXI2G0yibvhd3Ck5lRuMn" target="_blank"> Youtube link </a>|

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./ingestion

# COMMAND ----------

try:
    df_category = spark.read.schema(category_schema).option("badRecordsPath",gv_baddata_path).csv(gv_landing_path+"category/",header=True,sep="|")
    # add udit columns in existing dataframe 
    df_category = add_audit_cols(df_category)
    # Writing into Broze table 
    print('No of Records in Category Bronze file : ',df_category.count())
    df_category.write.format("delta").mode("overWrite").saveAsTable(gv_bronze_db_name+".category_bronze")
except Exception as e:
    print('Exception Raised at : CATEGORY_BRONZE : ',str(e))

# COMMAND ----------

try:
    df_date = spark.read.schema(date_schema).option("badRecordsPath",gv_baddata_path+"date").csv(gv_landing_path+"date/",header=True,sep="|")
    # add udit columns in existing dataframe 
    df_date = add_audit_cols(df_date)
    print('No of Records in Date Bronze   : ',df_date.count())
    # Writing into Broze table 
    df_date.write.format("delta").mode("overWrite").saveAsTable(gv_bronze_db_name+".date_bronze")
except Exception as e:
    print('Exception Raised at : DATE_BRONZE : ',str(e))

# COMMAND ----------

try:
    df_events = spark.read.schema(events_schema).option("badRecordsPath",gv_baddata_path+"events").csv(gv_landing_path+"events/",header=True,sep="|")
    df_events = df_events.withColumn("STARTTIME",col("STARTTIME").cast("timestamp"))
    # add udit columns in existing dataframe 
    df_events = add_audit_cols(df_events)
    print('No of Records in events Bronze   : ',df_events.count())
    # Writing into Broze table 
    df_events.write.format("delta").mode("overWrite").option("mergeSchema", "true").saveAsTable(gv_bronze_db_name+".events_bronze")
except Exception as e:
    print('Exception Raised at : DATE_BRONZE : ',str(e))

# COMMAND ----------

try:
    df_listings = spark.read.schema(listings_schema).option("badRecordsPath",gv_baddata_path+"listings").csv(gv_landing_path+"listings/",header=True,sep="|")
    # add udit columns in existing dataframe 
    df_listings = add_audit_cols(df_listings)
    print('No of Records in listings Bronze   : ',df_listings.count())
    # Writing into Broze table 
    df_listings.write.format("delta").mode("overWrite").saveAsTable(gv_bronze_db_name+".listings_bronze")
except Exception as e:
    print('Exception Raised at : LISTINGS_BRONZE : ',str(e))

# COMMAND ----------

try:
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    df_sales = spark.read.schema(sales_schema).option("badRecordsPath",gv_baddata_path+"sales").csv(gv_landing_path+"sales/",header=True,sep="\t")
    # add udit columns in existing dataframe 
    df_sales = add_audit_cols(df_sales)
    print('No of Records in sales Bronze   : ',df_listings.count())
    # changing string data type to timestamp
    df_sales = df_sales.withColumn("SALETIME",from_unixtime(unix_timestamp(col("SALETIME"),"MM/dd/yyyy hh:mm:ss")).cast("timestamp"))
    # Writing into Broze table 
    df_sales.write.format("delta").mode("overWrite").saveAsTable(gv_bronze_db_name+".sales_bronze")
except Exception as e:
    print('Exception Raised at : SALES_BRONZE : ',str(e))

# COMMAND ----------

try:
    df_users = spark.read.schema(users_schema).option("badRecordsPath",gv_baddata_path+"users").csv(gv_landing_path+"users/",header=True,sep="|")
    # add udit columns in existing dataframe 
    df_users = add_audit_cols(df_users)
    print('No of Records in users Bronze   : ',df_users.count())
    # Writing into Broze table 
    df_users.write.format("delta").mode("overWrite").saveAsTable(gv_bronze_db_name+".users_bronze")
except Exception as e:
    print('Exception Raised at : USERS_BRONZE : ',str(e))

# COMMAND ----------

try:
    df_venue = spark.read.schema(venue_schema).option("badRecordsPath",gv_baddata_path+"venue").csv(gv_landing_path+"venue/",header=True,sep="|")
    # add udit columns in existing dataframe 
    df_venue = add_audit_cols(df_venue)
    print('No of Records in venue Bronze   : ',df_users.count())
    # Writing into Broze table 
    df_venue.write.format("delta").mode("overWrite").saveAsTable(gv_bronze_db_name+".venue_bronze")
except Exception as e:
    print('Exception Raised at : VENUE_BRONZE : ',str(e))
