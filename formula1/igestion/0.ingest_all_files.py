# Databricks notebook source


# COMMAND ----------

v_result = dbutils.notebook.run("ingest_circuit_file",0,{"p_data_source" : "Ergast API"})

# COMMAND ----------

v_result