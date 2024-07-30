# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp, lit, concat

# COMMAND ----------

name_schema = StructType(fields=
                         [StructField("forename", StringType(), True),
                          StructField("surname", StringType(), True)])

drivers_schema = StructType(fields=
                            [StructField("driverId", IntegerType(), False),
                            StructField("driverRef", StringType(), True),
                            StructField("number", IntegerType(), True),
                            StructField("code", StringType(), True),
                            StructField("name", name_schema),
                            StructField("dob", DateType(), True),
                            StructField("nationality", StringType(), True),
                            StructField("url", StringType(), True)])

# COMMAND ----------

drivers_df = spark.read.json(f"/mnt/formula1dlshail/bronze/{v_file_date}/drivers.json", schema=drivers_schema)

# COMMAND ----------

drivers_df_new = drivers_df.withColumnRenamed("driverRef", "driver_ref") \
                            .withColumnRenamed("driverId", "driver_id") \
                .withColumn("ingestion_date", current_timestamp()) \
                .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                .withColumn("ingestion_source", lit(v_data_source)) \
                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_df_final = drivers_df_new.drop("url")

# COMMAND ----------

# drivers_df_final.write.mode("overwrite").parquet("/mnt/formula1dlshail/silver/drivers")
drivers_df_final.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")