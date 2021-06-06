# Databricks notebook source
# MAGIC %md
# MAGIC # Highways API Data
# MAGIC 
# MAGIC Highways England have a public API which is free to query. Check it out below:
# MAGIC 
# MAGIC > https://webtris.highwaysengland.co.uk
# MAGIC 
# MAGIC We have collected a years worth of data from this API and saved it to an Azure Storage Account for you to use. In the following exercises we will explore, manipulate and analyse that data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load in some data
# MAGIC 
# MAGIC Have a go at loading in a file.
# MAGIC 
# MAGIC Data is avaliable at the following location:
# MAGIC 
# MAGIC - storage account: `bigdataacademy`
# MAGIC - container: `data`
# MAGIC - folder: `raw`
# MAGIC 
# MAGIC Which can be accessed via this address: `wasbs://data@bigdataacademy.blob.core.windows.net/raw`
# MAGIC 
# MAGIC Each file contains data from one sensor collected over a whole year. The filenames follow this structure. There are two types of file, `report` and `quality`.
# MAGIC 
# MAGIC `<type>_<sensor-id>_<start-date>_<finish-date>.json.gz`
# MAGIC 
# MAGIC Let's take a look at what data we have...

# COMMAND ----------

# MAGIC %fs ls wasbs://data@bigdataacademy.blob.core.windows.net/raw

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Open one up
# MAGIC 
# MAGIC Let's jump into the data and see what we've got. Choose a file `report` file and open it up.
# MAGIC 
# MAGIC **Note:** Spark is smart enough to deal with the `.gz` compression so you can read the file the same as an uncompressed file.

# COMMAND ----------

filepath = 'wasbs://data@bigdataacademy.blob.core.windows.net/raw/report_10002_01012020_01012021.json.gz'

raw_df = spark.read.json(filepath)

raw_df.printSchema()

raw_df.show(1, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Empty Columns
# MAGIC Some columns are empty so they don't represent much use to use. Let's simplify and remove them. Do this using either the `.select()` or `.drop()` method.

# COMMAND ----------

columns = [
    'Site Name',
    'Report Date',
    'Time Interval',
    'Time Period Ending',
    '0 - 520 cm',
    '521 - 660 cm',
    '661 - 1160 cm',
    '1160+ cm',
    'Total Volume',
    'Avg mph'
]

df0 = raw_df.select(columns)

df0.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas
# MAGIC Print out the schema to check if the columns have been typed correctly.

# COMMAND ----------

df0.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's have a go at manipulating our columns to be the right types.
# MAGIC 
# MAGIC ### Read with schema
# MAGIC 
# MAGIC Instead of reading in a file and then manipulating it we can instruct Spark to do all this work at the point of reading the file in. We do this by defining a **schema** which we pass to Spark. Have a go at reading your chosen file in again but this time using a schema.
# MAGIC 
# MAGIC Your schema needs only included the columns you are interested and you can specify how the column should be typed. Use `TimestampType` and `DateType` where appropriate. However, do **NOT** try to convert any columns from `StringType` to `IntegerType` in the schema (such as `Time Interval`) because Spark doesn't like doing this - have a go and see that a `null` dataframe is returned.
# MAGIC 
# MAGIC **Note:** Spark seeks to guess the types for each column by sampling values from that column. Spark is usually very good at this depending on how the data is formatted and how clean the data is. Therefore, the fact that Spark did not identify our integer columns originally suggests that those columns contain some values such as empty strings or a string such as `"n/a"`.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DateType
)

schema = StructType([
    StructField('Site Name', StringType(), False),
    StructField('Report Date', DateType(), True),
    StructField('Time Period Ending', TimestampType(), True),
    StructField('Time Interval', StringType(), True),
    StructField('0 - 520 cm', StringType(), True),
    StructField('521 - 660 cm', StringType(), True),
    StructField('661 - 1160 cm', StringType(), True),
    StructField('1160+ cm', StringType(), True),
    StructField('Total Volume', StringType(), True),
    StructField('Avg mph', StringType(), True)
])


df1 = spark \
        .read \
        .schema(schema) \
        .json(filepath)

df1.printSchema()
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Casting `IntegerType`s
# MAGIC 
# MAGIC Now have a go at **casting** appropraite columns to `IntegerType`

# COMMAND ----------

from pyspark.sql.functions import col

cast_as_ints = [
    'Time Interval',
    '0 - 520 cm',
    '521 - 660 cm',
    '661 - 1160 cm',
    '1160+ cm',
    'Total Volume',
    'Avg mph'
]

df2 = df1

for column in cast_as_ints:
    
    df2 = df2.withColumn(column, col(column).cast('int'))

df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Null values
# MAGIC 
# MAGIC Let's investigate to see if we have any null values in our dataframe.

# COMMAND ----------

df2 \
    .filter(df2['Total Volume'].isNull()) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC These null values were likely previously empty strings before being cast as integers.
# MAGIC 
# MAGIC Null values can cause problems when exploring the data further down the line and can't be analysed so let's remove them.
# MAGIC 
# MAGIC Use either:
# MAGIC - `.na.drop()` [docs](http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions)
# MAGIC - `.dropna()` [docs](http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.dropna.html?highlight=dropna#pyspark.sql.DataFrame.dropna)

# COMMAND ----------

df3 = df2.na.drop('any')

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Let's have a quick check to make sure that worked...

# COMMAND ----------

df3 \
    .filter(df3['Total Volume'].isNull()) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading in multiple files
# MAGIC 
# MAGIC Now that we have a rough idea of the data available to us let's read in all we have. Spark's `.read()` method accepts wildcards (`*`) in the path. This allows us to read in multiple files together.
# MAGIC 
# MAGIC For example, to read in all the csv files in a particular directory that begin with `'finance_'` you could do the following
# MAGIC 
# MAGIC `df = spark.read.csv('path/to/dir/finance_*.csv')`
# MAGIC 
# MAGIC Have a go at reading in all the `report` files together.

# COMMAND ----------

# is it possible to read in all the report files? How long does that take?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expand Columns
# MAGIC 
# MAGIC In order to allow us finer granularity in our analysis, it would be helpful to expand the date and timestamp columns into year, month, day and hour columns. This will then allow us to provide aggeregations on different timescales to hopefully identify various patterns at daily, monthly and year-wide levels.
# MAGIC 
# MAGIC #### `Report Date` Column
# MAGIC 
# MAGIC Have a go at expanding the `Report Date` column into **year**, **month**, **day** and **weekday**

# COMMAND ----------


from pyspark.sql.functions import year, month, dayofmonth, dayofweek

df = df3

df = df \
        .withColumn('Report Date Year', year(df['Report Date'])) \
        .withColumn('Report Date Month', month(df['Report Date'])) \
        .withColumn('Report Date Day', dayofmonth(df['Report Date'])) \
        .withColumn('Report Date Weekday', dayofweek(df['Report Date']))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Attempt an alternative solution
# MAGIC 
# MAGIC Using methods:
# MAGIC - `split` [docs](http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.split.html?highlight=split#pyspark.sql.functions.split)
# MAGIC - `date_format` [docs](http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.date_format.html?highlight=date_format#pyspark.sql.functions.date_format)

# COMMAND ----------


from pyspark.sql.functions import split, col, date_format

df_ = df3

df_ = df_ \
        .withColumn('Report Date Year', split(df['Report Date'], '-').getItem(0).cast('int')) \
        .withColumn('Report Date Month', split(df['Report Date'], '-').getItem(1).cast('int')) \
        .withColumn('Report Date Day', split(df['Report Date'], '-').getItem(2).cast('int')) \
        .withColumn('Report Date Weekday', date_format(col("Report Date"), "E"))

display(df_)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### `Time Period Ending` Column
# MAGIC 
# MAGIC We're probably not going to identify any patterns at the level of minutes and seconds so let's simplify and have a go at extracting the **hour** from this column.

# COMMAND ----------

from pyspark.sql.functions import hour

df = df \
        .withColumn('Time Period Ending Hour', hour(df['Time Period Ending']))

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Optional: Drop columns
# MAGIC 
# MAGIC If fitting the data on screen when you `display()` the dataframe is a bit hard to read feel free to drop the `ReportDate` and `Time Period Ending` now that we have extracted what we're interested from them.

# COMMAND ----------

clean_df = df \
        .drop('Report Date') \
        .drop('Time Period Ending')

display(clean_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save
# MAGIC 
# MAGIC Let's save our dataframe as a **parquet** file so we can then pick it up and continue working with it in other notebooks. We'll save it as a **parquet** file to the `user` folder on the Databricks File System (`dbfs`).
# MAGIC 
# MAGIC 
# MAGIC #### Apache Parquet
# MAGIC Parquet is an efficient open source file format. It is columnar as opposed to row based such as `.csv`.
# MAGIC 
# MAGIC For further details about Parquet checkout the databricks [docs](https://databricks.com/glossary/what-is-parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In Parquet files column headers cannot contain certains characters such as a space (` `) so we need to rename our columns before we save.

# COMMAND ----------

for c in df.columns:
    df = df.withColumnRenamed(c, c.replace(' ', ''))

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Now let's write the dataframe to a file using the `.write.parquet()` method ([docs](http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet))

# COMMAND ----------

df.write.parquet('dbfs:/user/highways_reports', 'overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC Check you can find your new file
# MAGIC 
# MAGIC Have a look inside to see how a parquet format is structured.
# MAGIC 
# MAGIC A parquet "file" actually comprises multiple files. Some files contain metadata about the data such as schema information. The rest of the files actually have the `.parquet` extension and they contain the actual data. Each of these represents a **partition** of the data. Spark jobs can then be easily run over these by distributing the partitions across different nodes so each node can process a part of the data in parallel. The way that a parquet file is partitioned can influence performance, sometimes significantly*.
# MAGIC 
# MAGIC \* **e.g.** If the data is partitioned by a column whose values are not evenly distributed this can result in **skew**. This is where some partitions end up far larger than others and the advantages of distributed computing are lost as the bulk of the work is loaded onto one or a few machines. Often data is partitioned by date which avoids this. 

# COMMAND ----------

# MAGIC %fs ls user/highways_reports
