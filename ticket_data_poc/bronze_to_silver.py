# Databricks notebook source
# MAGIC %md
# MAGIC ###` Tickets Data Bronze To Silver Process`
# MAGIC
# MAGIC * __`Bronze process  From Bronze To Silver Delta Tables Process`__
# MAGIC
# MAGIC #### `Validations`
# MAGIC *  ` Data Validations will be doing in this stage`
# MAGIC *  ` Null Data Validations `
# MAGIC *  ` Duplicate Data Validations `
# MAGIC
# MAGIC ###Details
# MAGIC
# MAGIC | Details | Information 
# MAGIC |----|-----
# MAGIC |Notebook Created By | Raveendra  
# MAGIC |Object Name | Tickets  data Processing
# MAGIC |Source Type | Databricks Delta Tables
# MAGIC |Target Type | Databricks Delta Tables 
# MAGIC
# MAGIC ###History
# MAGIC |Date | Developed By | comments
# MAGIC |----|-----|----
# MAGIC |01/06/2022|Ravendra| Initial Version
# MAGIC | Find more Videos | Youtube   | <a href="https://www.youtube.com/watch?v=FpxkiGPFyfM&list=PL50mYnndduIHRXI2G0yibvhd3Ck5lRuMn" target="_blank"> Youtube link </a>|

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

try:
    df_category_bronze = spark.sql("select * from bronze_ticket_db.category_bronze")
    df_date_bronze = spark.sql("select * from bronze_ticket_db.date_bronze")
    df_events_bronze = spark.sql("select * from bronze_ticket_db.events_bronze")
    df_listings_bronze = spark.sql("select * from bronze_ticket_db.listings_bronze")
    df_sales_bronze = spark.sql("select * from bronze_ticket_db.sales_bronze")
    df_users_bronze = spark.sql("select * from bronze_ticket_db.users_bronze")
    df_venue_bronze = spark.sql("select * from bronze_ticket_db.venue_bronze")
except Exception as e:
    print('Exception Raised while Creating bronze dataframe :',str(e))

# COMMAND ----------

#Import Delta library for using delta tables merge
from delta.tables import *

# COMMAND ----------

try:
    #df_category_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/category_silver")
    df_category_tgt = DeltaTable.forName(spark, "silver_ticket_db.category_silver")
    df_category_valid = data_validations('category',df_category_bronze,'catid')
    df_category_tgt.alias("t").merge(df_category_valid.alias("s"),"t.catid = s.catid")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised : ',str(e))
    

# COMMAND ----------

try:
    #df_category_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/category_silver")
    df_date_tgt = DeltaTable.forName(spark, "silver_ticket_db.date_silver")
    df_date_valid = data_validations('date',df_date_bronze,'dateid')
    df_date_tgt.alias("t").merge(df_date_valid.alias("s"),"t.dateid = s.dateid")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised at Date Processing : ',str(e))
    

# COMMAND ----------

try:
    #df_events_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/events_silver")
    df_events_tgt = DeltaTable.forName(spark, "silver_ticket_db.events_silver")
    df_events_valid = data_validations('events',df_events_bronze,'eventid')
    df_events_tgt.alias("t").merge(df_events_valid.alias("s"),"t.eventid = s.eventid")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised : ',str(e))
    

# COMMAND ----------

try:
    #df_listings_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/category_silver")
    df_listings_tgt = DeltaTable.forName(spark, "silver_ticket_db.listings_silver")
    df_listings_valid = data_validations('category',df_listings_bronze,'listid')
    df_listings_tgt.alias("t").merge(df_listings_valid.alias("s"),"t.listid = s.listid")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised while processing listings : ',str(e))
    

# COMMAND ----------

try:
    #df_sales_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/sales_silver")
    df_sales_tgt = DeltaTable.forName(spark, "silver_ticket_db.sales_silver")
    df_sales_valid = data_validations('sales',df_sales_bronze,'salesid')
    df_sales_tgt.alias("t").merge(df_sales_valid.alias("s"),"t.salesid = s.salesid")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised : ',str(e))
    

# COMMAND ----------

try:
    #df_users_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/users_silver")
    df_users_tgt = DeltaTable.forName(spark, "silver_ticket_db.users_silver")
    df_users_valid = data_validations('users',df_users_bronze,'userid')
    df_users_tgt.alias("t").merge(df_users_valid.alias("s"),"t.userid = s.userid")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised : ',str(e))
    

# COMMAND ----------

try:
    #df_venue_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/venue_silver")
    df_venue_tgt = DeltaTable.forName(spark, "silver_ticket_db.venue_silver")
    df_venue_valid = data_validations('venue',df_venue_bronze,'venueid')
    df_venue_tgt.alias("t").merge(df_venue_valid.alias("s"),"t.venueid = s.venueid")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised : ',str(e))
    

# COMMAND ----------


