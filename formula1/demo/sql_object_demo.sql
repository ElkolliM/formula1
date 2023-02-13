-- Databricks notebook source
create database datameta;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

desc extended race_results_python;

-- COMMAND ----------

select * from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

create table race_results_sql
as
select * from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path" , f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc  demo.race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql
(
race_year	int	,
race_name	string	,
race_date	timestamp,	
circuits_location	string,	
driver_name	string,	
driver_number	int,	
driver_nationality	string,	
team	string,	
grid	int,	
fastest_lap	int,	
race_time	string,	
points	float,
created_date	timestamp
)
using parquet
location "/mnt/formula1md/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

create temp view v_race_results
AS 
select * 
  from race_results_python
  where race_year = 2018;

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view v_race_results
AS 
select * 
  from race_results_python
  where race_year = 2018;

-- COMMAND ----------

select * from global_temp.v_race_results