# Databricks notebook source
# MAGIC %md
# MAGIC ###` DDL'S Script for all stages databases and tables creation`
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
# MAGIC
# MAGIC | __`Bronze_ticket_db`__ | __`Silver_ticket_db`__ | __`Gold_ticket_db`__
# MAGIC |--|--|--
# MAGIC |category_bronze|category_silver|category_dim|
# MAGIC |date_bronze|date_silver|date_dim|
# MAGIC |events_bronze|events_silver|events_fact|
# MAGIC |listings_bronze|listings_silver|listings_fact|
# MAGIC |sales_bronze|sales_silver|sales_fact|
# MAGIC |users_bronze|users_silver|users_dim|
# MAGIC |venue_bronze|venue_silver|venue_dim|

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

dbutils.fs.rm(gv_bronze_db_path,True)
dbutils.fs.rm(gv_silver_db_path,True)
dbutils.fs.rm(gv_gold_db_path,True)

# COMMAND ----------

# MAGIC %md
# MAGIC #####  Dropping databases if exists and creating 

# COMMAND ----------

spark.sql("drop database if exists {0} cascade".format(gv_bronze_db_name))
spark.sql("create database if not exists {0} location '{1}'  comment 'tickt database for storing three fact tables and 4 dimentional tables'".format(gv_bronze_db_name,gv_bronze_db_path))
spark.sql("drop database if exists {0} cascade".format(gv_silver_db_name))
spark.sql("create database if not exists {0}  location '{1}' comment 'tickt database for storing three fact tables and 4 dimentional tables'".format(gv_silver_db_name,gv_silver_db_path))
spark.sql("drop database if exists {0} cascade".format(gv_gold_db_name))
spark.sql("create database if not exists {0} location '{1}' comment 'tickt database for storing three fact tables and 4 dimentional tables'".format(gv_gold_db_name,gv_gold_db_path))

# COMMAND ----------

print(spark.sql("describe database {0}".format(gv_bronze_db_name)).show(truncate=False))
print(spark.sql("describe database {0}".format(gv_silver_db_name)).show(truncate=False))
print(spark.sql("describe database {0}".format(gv_gold_db_name)).show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating Bronze Database Tables

# COMMAND ----------

spark.sql("drop table if exists {0}.venue_bronze".format(gv_bronze_db_name))
spark.sql("""
create table if not exists {0}.venue_bronze(
  venueid int not null,
  venuename varchar(100),
  venuecity varchar(30),
  venuestate varchar(2),
  venueseats integer,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_bronze_db_name))

spark.sql("drop table if exists {0}.category_bronze".format(gv_bronze_db_name))
spark.sql("""
create table {0}.category_bronze(
  catid int not null,
  catgroup varchar(10),
  catname varchar(10),
  catdesc varchar(50),
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_bronze_db_name))

spark.sql("drop table if exists {0}.users_bronze".format(gv_bronze_db_name))
spark.sql("""
create table {0}.users_bronze(
  userid integer not null,
  username varchar(8),
  firstname varchar(30),
  lastname varchar(30),
  city varchar(30),
  state varchar(2),
  email varchar(100),
  phone varchar(14),
  likesports boolean,
  liketheatre boolean,
  likeconcerts boolean,
  likejazz boolean,
  likeclassical boolean,
  likeopera boolean,
  likerock boolean,
  likevegas boolean,
  likebroadway boolean,
  likemusicals boolean,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_bronze_db_name))

spark.sql("drop table if exists {0}.date_bronze".format(gv_bronze_db_name))

spark.sql("""
create table {0}.date_bronze(
  dateid int not null,
  caldate date not null,
  day varchar(3) not null,
  week int not null,
  month varchar(5) not null,
  qtr int not null,
  year int not null,
  holiday boolean,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_bronze_db_name))

spark.sql("drop table if exists {0}.events_bronze".format(gv_bronze_db_name))

spark.sql("""
create table {0}.events_bronze(
  eventid int not null,
  venueid int not null,
  catid int not null,
  dateid int not null,
  eventname varchar(200),
  starttime timestamp,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_bronze_db_name))

spark.sql("drop table if exists {0}.listings_bronze".format(gv_bronze_db_name))

spark.sql("""
create table if not exists {0}.listings_bronze(
  listid int not null,
  sellerid int not null,
  eventid int not null,
  dateid int not null,
  numtickets int not null,
  priceperticket decimal(8, 2),
  totalprice decimal(8, 2),
  listtime timestamp,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_bronze_db_name))

spark.sql("drop table if exists {0}.sales_bronze".format(gv_bronze_db_name))
spark.sql("""
create table if not exists {0}.sales_bronze(
  salesid int not null,
  listid int ,
  sellerid int ,
  buyerid int ,
  eventid int ,
  dateid int,
  qtysold int ,
  pricepaid int,
  commission decimal(8, 2),
  saletime timestamp,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_bronze_db_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating Silver Database Tables

# COMMAND ----------

spark.sql("drop table if exists {0}.venue_silver".format(gv_silver_db_name))
spark.sql("""
create table if not exists {0}.venue_silver(
  venueid int not null,
  venuename varchar(100),
  venuecity varchar(30),
  venuestate varchar(2),
  venueseats integer,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_silver_db_name))

spark.sql("drop table if exists {0}.category_silver".format(gv_silver_db_name))

spark.sql("""
create table {0}.category_silver(
  catid int not null,
  catgroup varchar(10),
  catname varchar(10),
  catdesc varchar(50),
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_silver_db_name))

spark.sql("drop table if exists {0}.users_silver".format(gv_silver_db_name))
spark.sql("""
create table {0}.users_silver(
  userid integer not null,
  username varchar(8),
  firstname varchar(30),
  lastname varchar(30),
  city varchar(30),
  state varchar(2),
  email varchar(100),
  phone varchar(14),
  likesports boolean,
  liketheatre boolean,
  likeconcerts boolean,
  likejazz boolean,
  likeclassical boolean,
  likeopera boolean,
  likerock boolean,
  likevegas boolean,
  likebroadway boolean,
  likemusicals boolean,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_silver_db_name))

spark.sql("drop table if exists {0}.date_silver".format(gv_silver_db_name))

spark.sql("""
create table {0}.date_silver(
  dateid int not null,
  caldate date not null,
  day varchar(3) not null,
  week int not null,
  month varchar(5) not null,
  qtr varchar(5) not null,
  year int not null,
  holiday boolean,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_silver_db_name))

spark.sql("drop table if exists {0}.events_silver".format(gv_silver_db_name))

spark.sql("""
create table {0}.events_silver(
  eventid int not null,
  venueid int not null,
  catid int not null,
  dateid int not null,
  eventname varchar(200),
  starttime timestamp,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_silver_db_name))

spark.sql("drop table if exists {0}.listings_silver".format(gv_silver_db_name))

spark.sql("""
create table if not exists {0}.listings_silver(
  listid int not null,
  sellerid int not null,
  eventid int not null,
  dateid smallint not null,
  numtickets int not null,
  priceperticket decimal(8, 2),
  totalprice decimal(8, 2),
  listtime timestamp,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_silver_db_name))

spark.sql("drop table if exists {0}.sales_silver".format(gv_silver_db_name))
spark.sql("""
create table if not exists {0}.sales_silver(
  salesid int not null,
  listid int not null,
  sellerid int not null,
  buyerid int not null,
  eventid int not null,
  dateid int not null,
  qtysold int not null,
  pricepaid decimal(8, 2),
  commission decimal(8, 2),
  saletime timestamp,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_silver_db_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating gold Database Tables

# COMMAND ----------

spark.sql("drop table if exists {0}.venue_dim".format(gv_gold_db_name))
spark.sql("""
create table if not exists {0}.venue_dim(
  venue_key varchar(4000) not null,
  venueid int not null,
  venuename varchar(100),
  venuecity varchar(30),
  venuestate varchar(2),
  venueseats integer,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_gold_db_name))

spark.sql("drop table if exists {0}.category_dim".format(gv_gold_db_name))

spark.sql("""
create table {0}.category_dim(
  category_key varchar(4000) not null,
  catid int not null,
  catgroup varchar(10),
  catname varchar(10),
  catdesc varchar(50),
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_gold_db_name))

spark.sql("drop table if exists {0}.users_dim".format(gv_gold_db_name))

spark.sql("""
create table {0}.users_dim(
  user_key varchar(4000) not null,
  userid int not null,
  username varchar(8),
  firstname varchar(30),
  lastname varchar(30),
  city varchar(30),
  state varchar(2),
  email varchar(100),
  phone varchar(14),
  likesports boolean,
  liketheatre boolean,
  likeconcerts boolean,
  likejazz boolean,
  likeclassical boolean,
  likeopera boolean,
  likerock boolean,
  likevegas boolean,
  likebroadway boolean,
  likemusicals boolean,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)
""".format(gv_gold_db_name))

spark.sql("drop table if exists {0}.date_dim".format(gv_gold_db_name))

spark.sql("""
create table {0}.date_dim(
  date_key varchar(4000) not null,
  dateid int not null,
  caldate date not null,
  day varchar(3) not null,
  week int not null,
  month varchar(5) not null,
  qtr varchar(5) not null,
  year int not null,
  holiday boolean,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_gold_db_name))

spark.sql("drop table if exists {0}.events_fact".format(gv_gold_db_name))

spark.sql("""
create table {0}.events_fact(
  event_key varchar(4000) not null,  
  venue_key varchar(4000) not null,
  category_key varchar(4000) not null,
  date_key varchar(4000) not null,
  eventid int not null,
  eventname varchar(200),
  starttime timestamp,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)
""".format(gv_gold_db_name))

spark.sql("drop table if exists {0}.listings_fact".format(gv_gold_db_name))

spark.sql("""
create table if not exists {0}.listings_fact(
  list_key varchar(4000) not null,
  listid int not null,
  seller_key varchar(4000) not null,
  event_key varchar(4000) not null,
  date_key varchar(4000) not null,
  numtickets int not null,
  priceperticket decimal(8, 2),
  totalprice decimal(8, 2),
  listtime timestamp,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_gold_db_name))

spark.sql("drop table if exists {0}.sales_fact".format(gv_gold_db_name))

spark.sql("""
create table if not exists {0}.sales_fact(
  sales_key varchar(4000) not null,
  list_key varchar(4000) not null,
  seller_key varchar(4000) not null,
  buyer_key varchar(4000) not null,
  event_key varchar(4000) not null,
  date_key varchar(4000) not null,
  qtysold integer not null,
  pricepaid decimal(8, 2),
  commission decimal(8, 2),
  saletime timestamp,
  created_by varchar(50),
  created_date timestamp,
  updated_by varchar(50),
  updated_date timestamp
)""".format(gv_gold_db_name))
