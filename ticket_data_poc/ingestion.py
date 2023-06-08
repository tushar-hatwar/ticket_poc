# Databricks notebook source
# MAGIC %md
# MAGIC ###Details
# MAGIC
# MAGIC | Details | Information 
# MAGIC |----|-----
# MAGIC |Object Name | Data Ingestion process
# MAGIC |Source Location | Github location csv files
# MAGIC |Target Location | databricks dbfs location as csv files
# MAGIC
# MAGIC ## Data Ingestion 
# MAGIC * Data `ingestion` is the process of obtaining and importing data for immediate use or storage in a data lake/database. 
# MAGIC * To ingest something into data lake/database need to extract from different sources.
# MAGIC * __`Here we are exporting data from GitHub repository and storing in data lake path '/mnt(storage_acc_name)/landing/tickets'`__

# COMMAND ----------

print('started extracting data from github : .......')

# COMMAND ----------

# MAGIC %md
# MAGIC * `urllib.request.urlretrieve` return data, 
# MAGIC * the first argument is source location (file name with path)
# MAGIC * second argument is temporary location with filename . it will be saved in a new file.

# COMMAND ----------

start_path = "/mnt/saticketpocdata08062023/lake/landing"

# COMMAND ----------

dbutils.fs.rm(start_path,True)

# COMMAND ----------


import urllib.request
urllib.request.urlretrieve("https://raw.githubusercontent.com/raveendratal/PysparkTelugu/master/tickets_data/allevents_pipe.csv","/tmp/allevents_pipe.csv")
dbutils.fs.mv("file:/tmp/allevents_pipe.csv",f"{start_path}/tickets/events/allevents_pipe.csv")
urllib.request.urlretrieve("https://raw.githubusercontent.com/raveendratal/PysparkTelugu/master/tickets_data/allusers_pipe.csv","/tmp/allusers_pipe.csv")
dbutils.fs.mv("file:/tmp/allusers_pipe.csv",f"{start_path}/tickets/users/allusers_pipe.csv")
urllib.request.urlretrieve("https://raw.githubusercontent.com/raveendratal/PysparkTelugu/master/tickets_data/category_pipe.csv","/tmp/category_pipe.csv")
dbutils.fs.mv("file:/tmp/category_pipe.csv",f"{start_path}/tickets/category/category_pipe.csv")
urllib.request.urlretrieve("https://raw.githubusercontent.com/raveendratal/PysparkTelugu/master/tickets_data/date2008_pipe.csv","/tmp/date2008_pipe.csv")
dbutils.fs.mv("file:/tmp/date2008_pipe.csv",f"{start_path}/tickets/date/date2008_pipe.csv")
urllib.request.urlretrieve("https://raw.githubusercontent.com/raveendratal/PysparkTelugu/master/tickets_data/listings_pipe.csv","/tmp/listings_pipe.csv")
dbutils.fs.mv("file:/tmp/listings_pipe.csv",f"{start_path}/tickets/listings/listings_pipe.csv")
urllib.request.urlretrieve("https://raw.githubusercontent.com/raveendratal/PysparkTelugu/master/tickets_data/sales_tab.csv","/tmp/sales_tab.csv")
dbutils.fs.mv("file:/tmp/sales_tab.csv",f"{start_path}/tickets/sales/sales_tab.csv")
urllib.request.urlretrieve("https://raw.githubusercontent.com/raveendratal/PysparkTelugu/master/tickets_data/venue_pipe.csv","/tmp/venue_pipe.csv")
dbutils.fs.mv("file:/tmp/venue_pipe.csv",f"{start_path}/tickets/venue/venue_pipe.csv")

# COMMAND ----------

print('Data Extraction completed....')

# COMMAND ----------

dfName= spark.read.format("csv") \
.option("header", True) \
.option("inferSchema",True) \
.option("delimiter","|") \
.load(f"{start_path}/tickets/venue/venue_pipe.csv")

dfName.show()


# COMMAND ----------


