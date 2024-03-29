# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType , DateType, FloatType

# COMMAND ----------

lap_times_schema = StructType (fields = [StructField("raceId", IntegerType() , False) , 
                                       StructField("driverId" , IntegerType() , True),
                                       StructField("stop", StringType() , True) , 
                                       StructField("lap" , IntegerType() , True) ,
                                       StructField("time", StringType() ,True) , 
                                       StructField("milliseconds", IntegerType() , True)
                                        ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp ,lit

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1md/processed/lap_times")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1md/processed/lap_times'))