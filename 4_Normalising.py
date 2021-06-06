# Databricks notebook source
# MAGIC %md
# MAGIC # Normalisation & Denormalization
# MAGIC 
# MAGIC Normalisation is the process of manipulating a database so as to reduce redundancy.
# MAGIC 
# MAGIC There are varying normal forms, 1NF to 6NF, with increasing strictness of definition. Each normal form is usually defined by building on the previous as follows:
# MAGIC 
# MAGIC ```
# MAGIC A table is in (N+1) Normal Form if:
# MAGIC     - the table is in N Normal Form
# MAGIC     - AND [insert another condition]
# MAGIC ```
# MAGIC 
# MAGIC ### Third Normal Form (3NF)
# MAGIC 3NF is the most important form to be familiar with. Usually, normalising a table refers to conforming to 3NF.
# MAGIC 
# MAGIC Here are some articles which explain the Normal Form definitions:
# MAGIC - https://www.guru99.com/database-normalization.html
# MAGIC - https://www.datanamic.com/support/database-normalization.html
# MAGIC 
# MAGIC **Bonus**: A fun approximation of Codd's definition of 3NF is *"every non-key attribute must provide a fact about the key, the whole key, and nothing but the key, so help me Codd"*.

# COMMAND ----------

## set-up cell, run to read delay and codes data into dataframe objects.

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("date", StringType()),
    StructField("delay", IntegerType()),
    StructField("distance", IntegerType()),
    StructField("origin", StringType()),
    StructField("destination", StringType()),
])

raw_delays_df = spark.read \
        .option('header', True) \
        .schema(schema) \
        .csv('/databricks-datasets/flights/departuredelays.csv')

raw_codes_df = spark.read \
            .format('csv') \
            .option("header", True) \
            .option("delimiter", '\t') \
            .load('/databricks-datasets/flights/airport-codes-na.txt')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Normalising `raw_delays_df` Dataframe

# COMMAND ----------

print('Original delays table:')
raw_delays_df.show(5)

print('---------------- NORMALISED ----------------')

dist_df = raw_delays_df \
            .select(['origin', 'destination', 'distance']) \
            .distinct()
print('New Distances tables:')
dist_df.show(5)

delays_df = raw_delays_df.drop('distance')
print('New Delays table (without distances):')
delays_df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Normalising `raw_codes_df` Dataframe

# COMMAND ----------

print('Original codes table:')
raw_codes_df.show(5)

print('---------------- NORMALISED ----------------')

codes_df = raw_codes_df.select(['City', 'State', 'IATA'])
print('New Codes table (without country):')
codes_df.show(5)

states_df = raw_codes_df.select(['State', 'Country']).distinct()
print('New states and country table:')
states_df.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Denormalizing
# MAGIC Let's have a go at denormalizing our new tables into one single table. To do this we will have to **join** them back together. Have a pause a think how we might need to do this and with what type of join we will need to use.
# MAGIC - `delays_df`
# MAGIC - `dist_df`
# MAGIC - `codes_df`
# MAGIC - `states_df`
# MAGIC 
# MAGIC A denormalized table is:
# MAGIC - **less efficient** from a **storage** perspective because of all the redundancy in storing certain relationships many times over.
# MAGIC - **more efficient** from an **analytical** perspective because all the data is in one table so queries are simpler and faster with fewer or no joins involved.

# COMMAND ----------

denorm_df = delays_df \
              .join(dist_df, (delays_df.origin == dist_df.origin) & (delays_df.destination == dist_df.destination), 'left') \
              .join(codes_df, (delays_df.origin == codes_df.origin), 'left') \
              .join(states_df, (delays_df.state == states_df.state), 'left')

display(denorm_df)
