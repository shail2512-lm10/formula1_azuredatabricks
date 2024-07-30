# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../include/common_functions"
# MAGIC

# COMMAND ----------

# MAGIC %run "../include/configurations"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType


qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])


qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"/mnt/formula1dlshail/bronze/{v_file_date}/qualifying")


from pyspark.sql.functions import current_timestamp, lit


final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("ingestion_source", lit(v_data_source)) \
.withColumn("ingestion_date", lit(v_file_date))



# final_df.write.mode("overwrite").parquet("/mnt/formula1dlshail/silver/qualifying")
# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')
merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")