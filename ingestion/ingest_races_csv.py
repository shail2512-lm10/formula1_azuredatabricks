# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=
    [StructField("raceId", IntegerType(), False),
     StructField("year", IntegerType(), True),
     StructField("round", IntegerType(), True),
     StructField("circuitId", IntegerType(), True),
     StructField("name", StringType(), True),
     StructField("date", DateType(), True),
     StructField("time", StringType(), True),
     StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.csv(f"dbfs:/mnt/formula1dlshail/bronze/{v_file_date}/races.csv", header=True, schema=races_schema)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit, col, current_timestamp

# COMMAND ----------

race_df_filter = races_df.drop("url")
races_df_renamed = race_df_filter.withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId", "circuit_id")
races_df_final = races_df_renamed.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
     .withColumn("ingestion_date", current_timestamp()) \
     .withColumn("ingestion_source", lit(v_data_source)) \
     .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# races_df_final.write.mode("overwrite").partitionBy("race_year")parquet("/mnt/formula1dlshail/silver/races")
races_df_final.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")