# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration
# MAGIC 
# MAGIC In this notebook we are going to explore the data. The aim of this is to identify patterns or areas of interest in the data. Notebooks are helpful for this, allowing you to work on different queries in different commands. Databricks Notebooks are especially useful because of the `display()` function which allows easy visualation of the data which aids in identifying patterns.

# COMMAND ----------

filepath = 'dbfs:/user/highways_reports'

df = spark.read.parquet(filepath)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Total Volume by Month

# COMMAND ----------

lengths = [
    '0-520cm',
    '521-660cm',
    '661-1160cm',
    '1160+cm'
]display(df \
        .groupBy('ReportDateMonth') \
        .sum('TotalVolume') \
        .orderBy('ReportDateMonth'))

# COMMAND ----------

lengths = [
    '0-520cm',
    '521-660cm',
    '661-1160cm',
    '1160+cm'
]

display(df \
        .groupBy('ReportDateMonth') \
        .sum(*lengths) \
        .orderBy('ReportDateMonth'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Volume for Vehicle Size by Hour of Day

# COMMAND ----------

lengths = [
    '0-520cm',
    '521-660cm',
    '661-1160cm',
    '1160+cm'
]

display(df \
        .groupBy('TimePeriodEndingHour') \
        .avg(*lengths)
        .orderBy('TimePeriodEndingHour'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Mean Total Volume by Weekday

# COMMAND ----------

display(df \
        .groupBy('ReportDateWeekday') \
        .avg('TotalVolume') \
        .orderBy('ReportDateWeekday'))

# COMMAND ----------

# total volume by day of the week

from pyspark.sql.functions import date_format
from pyspark.sql.functions import col


report_df \
    .withColumn("Week Day", date_format(col("Report Date"), "E")) \
    .groupBy('Week Day') \
    .sum('Total Volume') \
    .withColumnRenamed('Sum(Total Volume)', 'Total Volume') \
    .orderBy('Total Volume', ascending=False) \
    .show()


# COMMAND ----------

# small and large vehicles by hour of the day

from pyspark.sql.functions import to_timestamp, date_format


small_df = report_df \
    .withColumn('Hour', date_format(col("Time Period Ending"), "k").cast('int')) \
    .groupBy('Hour') \
    .sum('0 - 520 cm')

large_df = report_df \
    .withColumn('Hour', date_format(col("Time Period Ending"), "k").cast('int')) \
    .groupBy('Hour') \
    .sum('1160+ cm')

small_df \
    .join(large_df, 'Hour', 'outer') \
    .orderBy('Hour') \
    .show()
