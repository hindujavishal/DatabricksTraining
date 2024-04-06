-- Databricks notebook source
use catalog dev

-- COMMAND ----------

select * from dev.marsh.emp

-- COMMAND ----------

create table dev.marsh.dept (dept_id int, dept_name string)

-- COMMAND ----------

create schema vis

-- COMMAND ----------

create table dev.vis.dept (dept_id int, dept_name string)

-- COMMAND ----------

create table dev.marsh.dept (dept_id int, dept_name string)

-- COMMAND ----------

select * from dev.marsh.agg_heartrate

-- COMMAND ----------

select dev.marsh.mask(mrn) as maskedmrn, * from dev.marsh.agg_heartrate

-- COMMAND ----------


