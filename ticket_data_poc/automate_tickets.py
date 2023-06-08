# Databricks notebook source
# MAGIC %md
# MAGIC ###` Tickets Data End To End Process`
# MAGIC
# MAGIC * __`Ingestion process  From Github To Landing`__
# MAGIC * __`Bronze process  From Landing To Bronze Delta Tables Process`__
# MAGIC * __`Silver process  From Bronze To Silver Delta Tables Process`__
# MAGIC * __`Gold process  From Silver To Gold Tables Process`__
# MAGIC
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

# MAGIC %md
# MAGIC ##### Ingestion step. 
# MAGIC * Get data from github repository and ingesting into /mnt/landing/tickets/ location

# COMMAND ----------

# MAGIC %run ./ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ##### calling databricks and tables creation notebook

# COMMAND ----------

# MAGIC %run ./ticket_db_ddl_scripts

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calling landing to bronze loading notebook

# COMMAND ----------

# MAGIC %run ./landing_to_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calling bronze to silver loading notebook

# COMMAND ----------

# MAGIC %run ./bronze_to_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calling silver to gold loading notebook

# COMMAND ----------

# MAGIC %run ./silver_to_gold

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calling data validations notebook

# COMMAND ----------

# MAGIC %run ./validations
