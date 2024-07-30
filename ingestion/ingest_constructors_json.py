# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import to_timestamp, concat, lit, col, current_timestamp

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.json(f"/mnt/formula1dlshail/bronze/{v_file_date}/constructors.json", schema=constructors_schema)

# COMMAND ----------

constructors_df_selected = constructors_df.select(col("constructorId").alias("constructor_id"), col("constructorRef").alias("constructor_ref"), col("name"), col("nationality"))
constructors_df_final = constructors_df_selected.withColumn("ingestion_date", current_timestamp()).withColumn("ingestion_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# constructors_df_final.write.mode("overwrite").parquet("/mnt/formula1dlshail/silver/constructors")
constructors_df_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")