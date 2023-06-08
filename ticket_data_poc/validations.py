# Databricks notebook source
# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC #### Varify records count after processing in bronze tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total_rows ,'category_bronze' as table_name from bronze_ticket_db.category_bronze union all
# MAGIC select count(*) as total_rows ,'date_bronze' as table_name from bronze_ticket_db.date_bronze union all
# MAGIC select count(*) as total_rows ,'events_bronze' as table_name from bronze_ticket_db.events_bronze union all
# MAGIC select count(*) as total_rows ,'listings_bronze' as table_name from bronze_ticket_db.listings_bronze union all
# MAGIC select count(*) as total_rows ,'sales_bronze' as table_name from bronze_ticket_db.sales_bronze union all
# MAGIC select count(*) as total_rows ,'users_bronze' as table_name from bronze_ticket_db.users_bronze union all
# MAGIC select count(*) as total_rows ,'venue_bronze' as table_name from bronze_ticket_db.venue_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #### Varify records count after processing in silver tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total_rows ,'category_silver' as table_name from silver_ticket_db.category_silver union all
# MAGIC select count(*) as total_rows ,'date_silver' as table_name from silver_ticket_db.date_silver union all
# MAGIC select count(*) as total_rows ,'events_silver' as table_name from silver_ticket_db.events_silver union all
# MAGIC select count(*) as total_rows ,'listings_silver' as table_name from silver_ticket_db.listings_silver union all
# MAGIC select count(*) as total_rows ,'sales_silver' as table_name from silver_ticket_db.sales_silver union all
# MAGIC select count(*) as total_rows ,'users_silver' as table_name from silver_ticket_db.users_silver union all
# MAGIC select count(*) as total_rows ,'venue_silver' as table_name from silver_ticket_db.venue_silver

# COMMAND ----------

# MAGIC %md
# MAGIC #### Varify records count after processing in gold tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total_rows ,'category_dim' as table_name from gold_ticket_db.category_dim union all
# MAGIC select count(*) as total_rows ,'date_dim' as table_name from gold_ticket_db.date_dim union all
# MAGIC select count(*) as total_rows ,'events_fact' as table_name from gold_ticket_db.events_fact union all
# MAGIC select count(*) as total_rows ,'listings_fact' as table_name from gold_ticket_db.listings_fact union all
# MAGIC select count(*) as total_rows ,'sales_fact' as table_name from gold_ticket_db.sales_fact union all
# MAGIC select count(*) as total_rows ,'users_dim' as table_name from gold_ticket_db.users_dim union all
# MAGIC select count(*) as total_rows ,'venue_dim' as table_name from gold_ticket_db.venue_dim

# COMMAND ----------

df_bad_data = spark.read.json(f"{gv_baddata_path}/*/*/bad_records")
display(df_bad_data)

# COMMAND ----------

display(df_bad_data.groupBy("path").count())

# COMMAND ----------

display(df_bad_data.filter("path='dbfs:/mnt/saticketpocdata08062023/lake/landing/tickets/events/allevents_pipe.csv'"))

# COMMAND ----------

display(df_bad_data.filter("path='dbfs:/mnt/saticketpocdata08062023/lake/landing/tickets/venue/venue_pipe.csv'"))

# COMMAND ----------


