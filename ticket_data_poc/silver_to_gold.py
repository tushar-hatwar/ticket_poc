# Databricks notebook source
# MAGIC %md
# MAGIC ###` Tickets Data Silver To Gold Process`
# MAGIC
# MAGIC * __`Bronze process  From Silver To Gold Delta Tables Process`__
# MAGIC
# MAGIC #### `Validations`
# MAGIC *  ` Aggregations and Dimensional model relationships will be added here`
# MAGIC *  ` Surrogate Key Generation `
# MAGIC *  ` joining with dimenstional tabels and loading in fact tables `
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
    # Creating Dataframes from silver tables
    df_category_silver = spark.sql("select * from silver_ticket_db.category_silver")
    df_date_silver = spark.sql("select * from silver_ticket_db.date_silver")
    df_events_silver = spark.sql("select * from silver_ticket_db.events_silver")
    df_listings_silver = spark.sql("select * from silver_ticket_db.listings_silver")
    df_sales_silver = spark.sql("select * from silver_ticket_db.sales_silver")
    df_users_silver = spark.sql("select * from silver_ticket_db.users_silver")
    df_venue_silver = spark.sql("select * from silver_ticket_db.venue_silver")
except Exception as e:
    print('Exception Raised while Creating silver dataframe :',str(e))

# COMMAND ----------

#Import Delta library for using delta tables merge
from delta.tables import *

# COMMAND ----------

try:
    #df_category_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/gold_ticket_db/category_dim")
    df_category_tgt = DeltaTable.forName(spark, "gold_ticket_db.category_dim")
    df_category_valid = add_surrogate_key('category',df_category_silver,'category_key','catid')
    df_category_tgt.alias("t").merge(df_category_valid.alias("s"),"t.category_key = s.category_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised : ',str(e))    

# COMMAND ----------

try:
    #df_category_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/category_silver")
    df_date_tgt = DeltaTable.forName(spark, "gold_ticket_db.date_dim")
    df_date_valid = add_surrogate_key('date',df_date_silver,'date_key','dateid')
    df_date_tgt.alias("t").merge(df_date_valid.alias("s"),"t.date_key = s.date_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised at Date Processing : ',str(e))
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_ticket_db.date_dim

# COMMAND ----------

try:
    #df_users_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/gold_ticket_db/users_dim")
    df_users_tgt = DeltaTable.forName(spark, "gold_ticket_db.users_dim")
    df_users_valid = add_surrogate_key('users',df_users_silver,'user_key','userid')
    df_users_tgt.alias("t").merge(df_users_valid.alias("s"),"t.user_key = s.user_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised : ',str(e))
    

# COMMAND ----------

try:
    #df_venue_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/gold_ticket_db/venue_dim")
    df_venue_tgt = DeltaTable.forName(spark, "gold_ticket_db.venue_dim")
    df_venue_valid = add_surrogate_key('venue_dim',df_venue_silver,'venue_key','venueid')
    df_venue_tgt.alias("t").merge(df_venue_valid.alias("s"),"t.venue_key = s.venue_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
    
except Exception as e:
    print('Exception Raised : ',str(e))
    

# COMMAND ----------

try:
    #df_listings_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/category_silver")
    df_seller_dim = spark.sql("select * from gold_ticket_db.users_dim").selectExpr("userid","user_key as seller_key")
    df_date_dim = spark.sql("select * from gold_ticket_db.date_dim").select("dateid","date_key")
    df_event_fact = spark.sql("select * from gold_ticket_db.events_fact").select("eventid","event_key")
    df_listings_tgt = DeltaTable.forName(spark, "gold_ticket_db.listings_fact")
    df_listings_valid = add_surrogate_key('list',df_listings_silver,'list_key','listid')
    df_listings_final = (df_listings_valid.join(df_seller_dim,df_listings_valid["sellerid"]==df_seller_dim["userid"],'left').join(df_date_dim,"dateid",'left').join(df_event_fact,"eventid",'left').selectExpr("list_key","listid","nvl(seller_key,-1) as seller_key","nvl(event_key,-1) as event_key","nvl(date_key,-1) as date_key","numtickets","priceperticket","totalprice","listtime","created_by","created_date","updated_by","updated_date"))
    
    df_listings_tgt.alias("t").merge(df_listings_final.alias("s"),"t.list_key = s.list_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised while processing listings : ',str(e))
    

# COMMAND ----------



# COMMAND ----------

try:
    #df_events_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/events_silver")
    df_venue_dim = spark.sql("select * from gold_ticket_db.venue_dim")
    df_category_dim = spark.sql("select * from gold_ticket_db.category_dim")
    df_date_dim = spark.sql("select * from gold_ticket_db.date_dim")
    df_events_tgt = DeltaTable.forName(spark, "gold_ticket_db.events_fact")
    df_events_valid = add_surrogate_key('events',df_events_silver,'event_key','eventid')
    # joining with dimentional tables and getting surrogate key...
    df_events_valid=(df_events_valid.join(df_venue_dim,"venueid",'left').join(df_category_dim,"catid",'left').join(df_date_dim,"dateid",'left').selectExpr("event_key","nvl(venue_key,-1) as venue_key","nvl(category_key,-1) as category_key","nvl(date_key,-1) as date_key","eventid","eventname","starttime","spark_catalog.silver_ticket_db.events_silver.created_by","spark_catalog.silver_ticket_db.events_silver.created_date","spark_catalog.silver_ticket_db.events_silver.updated_by","spark_catalog.silver_ticket_db.events_silver.updated_date"))
    # using merge upserting into target table
    df_events_tgt.alias("t").merge(df_events_valid.alias("s"),"t.event_key = s.event_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised : ',str(e))
    

# COMMAND ----------

try:
    #df_sales_tgt = DeltaTable.forPath(spark, "/user/hive/warehouse/silver_ticket_db/sales_silver")
    df_seller_dim = spark.sql("select * from gold_ticket_db.users_dim").selectExpr("userid","user_key as seller_key")
    df_buyer_dim = spark.sql("select * from gold_ticket_db.users_dim").selectExpr("userid","user_key as buyer_key")
    df_date_dim = spark.sql("select * from gold_ticket_db.date_dim").select("dateid","date_key")
    df_event_fact = spark.sql("select * from gold_ticket_db.events_fact").select("eventid","event_key")
    df_listing_fact = spark.sql("select * from gold_ticket_db.listings_fact").select("listid","list_key")
    # delta object for target table
    df_sales_tgt = DeltaTable.forName(spark, "gold_ticket_db.sales_fact")
    df_sales_valid = add_surrogate_key('category',df_sales_silver,'sales_key','salesid')
    df_dales_final = (df_sales_valid.join(df_seller_dim,df_sales_valid["sellerid"]==df_seller_dim["userid"],'left').join(df_buyer_dim,df_sales_valid["buyerid"]==df_buyer_dim["userid"],'left').join(df_listing_fact,"listid",'left').join(df_date_dim,"dateid",'left').join(df_event_fact,"eventid",'left').selectExpr("sales_key","nvl(list_key,-1) as list_key","nvl(seller_key,-1) as seller_key","nvl(buyer_key,-1) as buyer_key","nvl(event_key,-1) as event_key","nvl(date_key,-1) as date_key","qtysold","pricepaid","commission","saletime","created_by","created_date","updated_by","updated_date"))

    df_sales_tgt.alias("t").merge(df_dales_final.alias("s"),"t.sales_key = s.sales_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
except Exception as e:
    print('Exception Raised : ',str(e))
    
