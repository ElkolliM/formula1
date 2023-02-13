# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df  = race_results_df.filter("race_year=2020")

# COMMAND ----------

from pyspark.sql.functions import count , countDistinct , sum

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df\
.groupBy("driver_name")\
.agg(sum("points"), countDistinct("race_name"))\
.show()