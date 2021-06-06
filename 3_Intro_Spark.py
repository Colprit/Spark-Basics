# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Introduction to Spark
# MAGIC 
# MAGIC Spark is an open source framework for distributed big data processing. It is best introduced by way of an example. Databricks has a load of example datasets which we can play around with to learn from. We'll use a dataset they have on flights. Feel free to explore what other datasets are available.

# COMMAND ----------

# MAGIC %fs ls ./databricks-datasets/flights

# COMMAND ----------

# MAGIC %md
# MAGIC If you installed Spark locally you would need to write some boiler plate code to set-up a Spark session. In Databricks this is done for us and we can simple access the object `spark` straight away. Run the cell below to check

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC Let's learn some Spark concepts
# MAGIC 
# MAGIC ### Resilient Distributed Datasets (RDD)
# MAGIC 
# MAGIC RDDs are the fundamental data structure in Spark. They are immutable distributed collection of objects. Each dataset in RDD is divided into logical partitions, which may be operated on in parallel by different nodes in a cluster. In other words, it is a read-only partition collection of records.
# MAGIC 
# MAGIC ### Dataframes
# MAGIC 
# MAGIC A Dataframe is an RDD with more structure. They require a schema and can be thought of as "tables". In most cases you will be working with Dataframes rather than RDDs because of Spark's Catalyst Optimizer for Dataframes. 
# MAGIC 
# MAGIC (note: the dataframe concept is inspired by R / the Pandas library but is not the same)
# MAGIC 
# MAGIC ### Jobs, Stages, Tasks and Shuffles
# MAGIC 
# MAGIC Spark uses lazy execution so it waits until an action which requires some output for the user. When this happens a **job** is created and this is submitted to the SparkContext. The jobs are divided into **stages**. A stage is made up of **tasks** where each a task is to be run on a single node and comprises a partition of data and an operation to be run on it. Between each stage a **shuffle** of data is required, this is where nodes return the data they have finised processing and recieve the next partition of data they are to process (ie data is shared and shuffled across the machines).
# MAGIC 
# MAGIC ### Catalyst Optimizer
# MAGIC 
# MAGIC When you carry out operations on an RDD each operation is carried out in the order you specify. While when you operate on Dataframes all the jobs are passed through the **Catalyst Optimizer** which determines the most efficient plan to run your query. To do this it also uses a special format for CPU and memory efficiency called the **Tungsten engine**.
# MAGIC 
# MAGIC Optimizations are based on the fact that in order to carry out operations on a distributed system you want to maxmise on parralelisation across machines (ie you don't want machines waiting idle for other machines to finish processing something) and minimise on shuffles because they are time consuming. There are many rules that the optimizer uses to determine these optimizations.
# MAGIC 
# MAGIC The optimizer allows the advantage that you are able to focus on clarity in your code and spend less time considering optimisations. It is potentially worth noting that Spark is not able to optimize on all operations. For example the `.apply()` method allows you to provide a user-defined function to a dataframe, Spark is unable to optimize so effectively for this and so it is recommended to use native methods where possible.

# COMMAND ----------

# MAGIC %md
# MAGIC Enough theory, let's start using Spark. We'll begin by reading in the departure delays data using the `.read.csv()` method [(docs)](http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.csv.html?highlight=csv#pyspark.sql.DataFrameReader.csv)

# COMMAND ----------

delays_df = spark.read.csv('/databricks-datasets/flights/departuredelays.csv')

delays_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Notice we have a few issues:
# MAGIC - the headers (date, delay etc) have been included as a row of data rather than as headers (instead we have column names `_c0`, `_c1` etc)
# MAGIC - the data types of the columns have not been inferred incorrectly (you can see the incorrect schema by clicking the drop down above the output window which looks like `df:pyspark.sql.dataframe.DataFrame = [_c0: string, _c1: string ... 3 more fields]` - all columns are strings when the delay and distance would be integer columns)
# MAGIC 
# MAGIC Let's learn a couple options we can apply when reading a file.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("date", StringType()),
    StructField("delay", IntegerType()),
    StructField("distance", IntegerType()),
    StructField("origin", StringType()),
    StructField("destination", StringType()),
])

delays_df = spark.read \
        .option('header', True) \
        .schema(schema) \
        .csv('/databricks-datasets/flights/departuredelays.csv')

delays_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks incorporates some useful features for outputting results.
# MAGIC 
# MAGIC ### Display()
# MAGIC 
# MAGIC The display function allows you to print a Dataframe in an interactive viewer. Have a go at printing the flights dataframe below. 
# MAGIC 
# MAGIC (note this is NOT part of Spark, it is an extension built in the Databricks platform)

# COMMAND ----------

display(delays_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display options
# MAGIC After using the `display()` method, the window generated will have three icons beneath it:
# MAGIC - Table icon: show data in a table (selected by default)
# MAGIC - Graph icon: plot the data on a graph - there are various types of chart and you can select which columns to use for x and y variables etc. by clicking the `Plot Options...` button
# MAGIC - Download icon: to download data as a `.csv`
# MAGIC 
# MAGIC Try choosing a few different graph options to get a feel for it using the flights dataframe.
# MAGIC 
# MAGIC (Note: to resize the Display window, to make it bigger and easier to read, look for a small black triangle made of two small lines at the bottom right of the display window)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Airport Codes
# MAGIC The delay data uses airport codes, to translate these into full names let's read in the airport codes for North America file (`airport-codes-na.txt`)
# MAGIC 
# MAGIC Note here we are using another `.option()` to specify a delimiter (tab `\t` instead of the standard comma `,`)

# COMMAND ----------

codes_df = spark.read \
            .format('csv') \
            .option("header", True) \
            .option("delimiter", '\t') \
            .load('/databricks-datasets/flights/airport-codes-na.txt')

display(codes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### JOINS
# MAGIC You can do many different operations on DataFrames in Spark. If you are familiar with SQL, many of the concepts from SQL translate over to Spark. In particular the various types of joins. Let's have a go at joining our two DataFrames together using the `.join()` method [(docs)](http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join)
# MAGIC 
# MAGIC Learn about SQL join types [here](http://www.sql-join.com/sql-join-types/)

# COMMAND ----------

full_df = delays_df.join(
                other=codes_df,
                on=(delays_df.origin == codes_df.IATA),
                how='left'
                )

display(full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Right, we also want to do the same for the `destination` column. So let's join to the codes dataframe `codes_df` a second time, but this time on the destination column. 

# COMMAND ----------

full_df = full_df.join(
                codes_df,
                delays_df.destination == codes_df.IATA,
                'left'
                )

display(full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Running the above Spark will raise an `AnalysisException` the resulting DataFrame would have ambiguous column naming (i.e. columns with the same name).
# MAGIC 
# MAGIC #### Column Rename
# MAGIC 
# MAGIC You can rename the column of a dataframe using the `.withColumnRenamed()` method [(docs)](http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.withColumnRenamed.html?highlight=withcolumnrenamed#pyspark.sql.DataFrame.withColumnRenamed). Let's remove any ambiguity around the columns we've just joined.

# COMMAND ----------

def add_col_prefix(df, prefix, cols):
    new_df = df
    for col in cols:
        new_df = new_df.withColumnRenamed(col, f'{prefix}_{col}')
    
    return new_df

full_df = add_col_prefix(full_df, "origin", codes_df.columns)

display(full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Brill, now we can try joining to `codes_df` again.

# COMMAND ----------

full_df = full_df.join(
                codes_df,
                full_df.destination == codes_df.IATA,
                'left'
                )

full_df = add_col_prefix(full_df, "dest", codes_df.columns)

display(full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Adding Columns
# MAGIC 
# MAGIC Having looked at the data it probably stands out to you that the `date` column doesn't look much like a date. These column captures the start and end times of the flight in 24-hour time. At the moment, the column is simply a string with these times concatenated. This isn't very easy to manipulate and analyse - it would be much easier if we could
# MAGIC 
# MAGIC - extract the two times into seperate columns
# MAGIC - convert them from strings to timestamps
# MAGIC 
# MAGIC To achieve both these aims we can use the `withColumn()` method [(docs)](http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html?highlight=withcolumn#pyspark.sql.DataFrame.withColumn). This allows us to define a new column to be appended to the dataframe which is being acted on. It is possible to define new columns based on existing ones. This is what we will do.

# COMMAND ----------

# from pyspark.sql.functions import split
#
# delays_df = delays_df \
#                 .withColumn("lift_off", split(col("date"), "-").getItem(0))

# COMMAND ----------

delays_df = delays_df \
                .withColumn("lift_off", delays_df.date[0:4]) \
                .withColumn("touch_down", delays_df.date[5:8])

display(delays_df)

# COMMAND ----------

import pyspark.sql.functions as F

delays_df = delays_df \
            .withColumn("lift_off", F.to_timestamp("lift_off", "HHmm")) \
            .withColumn("touch_down", F.to_timestamp("touch_down", "HHmm"))

display(delays_df)
