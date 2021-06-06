-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Data in Databricks
-- MAGIC 
-- MAGIC In databricks you can create databases and tables within the workspace. By default all tables are saved in the [**Delta Lake**](https://databricks.com/product/delta-lake-on-databricks) format based on the Apache **Parquet** format.
-- MAGIC 
-- MAGIC In this notebook we will use a simple example to explore how data is stored in the Databricks workspace.
-- MAGIC 
-- MAGIC (Note the default language in this notebook is SQL)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Run the code below to create a table and populate it with some data. (It will be created within the *Default* database)

-- COMMAND ----------

DROP TABLE IF EXISTS persons;

CREATE TABLE persons (
  name VARCHAR(50),
  birth_date DATE,
  phone VARCHAR(11)
);

INSERT INTO
  persons (name, birth_date, phone)
VALUES
  ('Ruth Jones', '1994-07-25', '07566948602'),
  ('James Samson', '1999-05-02', '02333628655');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Let's investigate
-- MAGIC 
-- MAGIC Using SQL let's verify the table successfully created.

-- COMMAND ----------

SELECT
  *
FROM
  persons;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can also inspect all databases and tables via the **Data** tab in the left hand vertical menu. Clicking on a table will then open a page listing of details about the table and will also show a sample of rows. Verify that you can find our `persons` table created succesfully in the `default` database.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC "But where is the data actually stored?" I hear you asking. Great question! Let's explore the Databricks File System (DBFS) to find our table.

-- COMMAND ----------

-- MAGIC %fs ls ./user/hive/warehouse/persons

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Run the cell below to remove the table. You can then check via each of the means above that the table has indeed been dropped.

-- COMMAND ----------

DROP TABLE IF EXISTS persons;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create your own database
-- MAGIC Have a go at creating your own database, with a table, with data.

-- COMMAND ----------

DROP DATABASE IF EXISTS science;

CREATE DATABASE science;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS science.elements (
  name VARCHAR(50),
  atomic_number INT
);

INSERT INTO
  science.elements (name, atomic_number)
VALUES
  ('Hydrogen', 1),
  ('Helium', 2);

-- COMMAND ----------

SELECT * FROM science.elements
