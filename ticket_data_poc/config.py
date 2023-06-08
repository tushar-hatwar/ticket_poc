# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#Global Variables database names
gv_bronze_db_name = "bronze_ticket_db"
gv_silver_db_name = "silver_ticket_db"
gv_gold_db_name = "gold_ticket_db"
#Global Variables database location path
gv_bronze_db_path = "/user/hive/warehouse/bronze_ticket_db"
gv_silver_db_path = "/user/hive/warehouse/silver_ticket_db"
gv_gold_db_path = "/user/hive/warehouse/gold_ticket_db"
# Global Landing Variable Path
gv_landing_path = "/mnt/landing/tickets/"
gv_baddata_path = "/mnt/baddata/"

# COMMAND ----------

from pyspark.sql.types import *
category_schema= StructType([StructField('CATID', IntegerType(), True), 
                             StructField('CATGROUP', StringType(), True), 
                             StructField('CATNAME', StringType(), True), 
                             StructField('CATDESC', StringType(), True)])

# COMMAND ----------

events_schema= StructType([StructField('EVENTID', IntegerType(), True), 
                           StructField('VENUEID', IntegerType(), True), 
                           StructField('CATID', IntegerType(), True), 
                           StructField('DATEID', IntegerType(), True), 
                           StructField('EVENTNAME', StringType(), True), 
                           StructField('STARTTIME', StringType(), True)])

# COMMAND ----------

date_schema = StructType([StructField('DATEID', IntegerType(), True), 
                          StructField('CALDATE', DateType(), True), 
                          StructField('DAY', StringType(), True), 
                          StructField('WEEK', IntegerType(), True), 
                          StructField('MONTH', StringType(), True), 
                          StructField('QTR', IntegerType(), True), 
                          StructField('YEAR', IntegerType(), True), 
                          StructField('HOLIDAY', BooleanType(), True)])

# COMMAND ----------

sales_schema = StructType([StructField('SALESID', IntegerType(), True), 
                           StructField('LISTID', IntegerType(), True), 
                           StructField('SELLERID', IntegerType(), True), 
                           StructField('BUYERID', IntegerType(), True), 
                           StructField('EVENTID', IntegerType(), True), 
                           StructField('DATEID', IntegerType(), True), 
                           StructField('QTYSOLD', IntegerType(), True), 
                           StructField('PRICEPAID', IntegerType(), True), 
                           StructField('COMMISSION', DecimalType(8,2), True), 
                           StructField('SALETIME', StringType(), True)])

# COMMAND ----------

listings_schema = StructType([StructField('LISTID', IntegerType(), True), 
                              StructField('SELLERID', IntegerType(), True), 
                              StructField('EVENTID', IntegerType(), True), 
                              StructField('DATEID', IntegerType(), True), 
                              StructField('NUMTICKETS', IntegerType(), True), 
                              StructField('PRICEPERTICKET', DecimalType(8,2), True), 
                              StructField('TOTALPRICE', DecimalType(8,2), True), 
                              StructField('LISTTIME', TimestampType(), True)])

# COMMAND ----------

venue_schema = StructType([StructField('VENUEID', IntegerType(), True), 
                           StructField('VENUENAME', StringType(), True), 
                           StructField('VENUECITY', StringType(), True), 
                           StructField('VENUESTATE', StringType(), True), 
                           StructField('VENUESEATS', IntegerType(), True)])

# COMMAND ----------

users_schema = StructType([StructField('USERID', IntegerType(), True), 
                           StructField('USERNAME', StringType(), True), 
                           StructField('FIRSTNAME', StringType(), True), 
                           StructField('LASTNAME', StringType(), True), 
                           StructField('CITY', StringType(), True), 
                           StructField('STATE', StringType(), True), 
                           StructField('EMAIL', StringType(), True), 
                           StructField('PHONE', StringType(), True), 
                           StructField('LIKESPORTS', BooleanType(), True), 
                           StructField('LIKETHEATRE', BooleanType(), True), 
                           StructField('LIKECONCERTS', BooleanType(), True), 
                           StructField('LIKEJAZZ', BooleanType(), True), 
                           StructField('LIKECLASSICAL', BooleanType(), True), 
                           StructField('LIKEOPERA', BooleanType(), True), 
                           StructField('LIKEROCK', BooleanType(), True), 
                           StructField('LIKEVEGAS', BooleanType(), True), 
                           StructField('LIKEBROADWAY', BooleanType(), True), 
                           StructField('LIKEMUSICALS', BooleanType(), True)])

# COMMAND ----------

def add_audit_cols(df):
    df = df.withColumn("created_by",lit("databricks"))\
           .withColumn("created_date",current_timestamp())\
           .withColumn("updated_by",lit("databricks"))\
           .withColumn("updated_date",current_timestamp())
    return df

def get_dataframes():
    from pyspark.sql import DataFrame
    return [k for (k, v) in globals().items() if isinstance(v, DataFrame)]

def data_validations(job,df,pk):
    print('Job Name : ',job)
    df_unique = df.dropDuplicates(pk.split(","))
    print('Duplicate rows : ',(df.count()-df_unique.count()))
    df_good = df_unique.dropna(how='all')
    print('Null rows : ',(df_unique.count()-df_good.count()))
    return df_good

def add_surrogate_key(job,df,sk,pk):
    print('Job Name : ',job)
    df = df.withColumn(sk,sha2(concat(*pk.split(",")),256))
    return df
