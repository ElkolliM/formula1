-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_demo
LOCATION "/mnt/formula1md/demo"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read\
-- MAGIC .option("inferSchema" , True)\
-- MAGIC .json("dbfs:/mnt/formula1md/raw/2021-03-28/results.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").save("/mnt/formula1md/demo/results_external")

-- COMMAND ----------

select * from f1_demo.results_managed

-- COMMAND ----------

create table f1_demo.results_external
using delta
location "/mnt/formula1md/demo/results_external"

-- COMMAND ----------

select * from f1_demo.results_external

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_external_df = spark.read.format("delta").load("/mnt/formula1md/demo/results_external")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partition")

-- COMMAND ----------

show partitions f1_demo.results_partition

-- COMMAND ----------

desc history f1_demo.results_partition