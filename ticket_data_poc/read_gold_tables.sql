-- Databricks notebook source

show databases;

-- COMMAND ----------

use gold_ticket_db; 

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from gold_ticket_db.category_dim;

-- COMMAND ----------

select * from gold_ticket_db.date_dim;

-- COMMAND ----------

SELECT * from gold_ticket_db.events_fact;

-- COMMAND ----------

SELECT * from gold_ticket_db.listings_fact;

-- COMMAND ----------

SELECT * from gold_ticket_db.sales_fact;

-- COMMAND ----------

SELECT * from gold_ticket_db.users_dim

-- COMMAND ----------

SELECT * from gold_ticket_db.venue_dim
