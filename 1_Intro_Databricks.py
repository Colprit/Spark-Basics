# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Introduction to Azure Databricks
# MAGIC 
# MAGIC Welcome to your first _Azure Databricks Notebook_. Let's begin by clarifying each of these terms in reverse order:
# MAGIC 
# MAGIC - **Notebook**: Normally in a python script when you run it, it runs all the code contained. In a notebook there are commands/cells which can run independently, be rearranged and reference variables defined in other cells. There are many open source notebook systems for various languages, such as iPython and Jupyter.
# MAGIC - **Databricks**: A company providing a web-based platform for working with Apache Spark using notebook interfaces. The advantage that Databricks offers is cluster management which can be very tricky (i.e. managing all the nodes which you'll be running your distributed Spark jobs over)
# MAGIC - **Azure**: This service is hosted in Azure - Microsoft's cloud platform.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Clusters
# MAGIC In order to run any code in your notebook you need a cluster on which to run it.
# MAGIC 
# MAGIC ##### What is a Cluster?
# MAGIC A cluster is a group of nodes/machines managed by Databricks on which your code and Spark Jobs can be run.
# MAGIC 
# MAGIC ##### Creating a Cluster
# MAGIC Follow the steps [here](https://docs.microsoft.com/en-gb/azure/databricks/scenarios/quickstart-create-databricks-workspace-portal?tabs=azure-portal#create-a-spark-cluster-in-databricks) to create a cluster. All the defaults should be fine except **set max workers to 4**.
# MAGIC 
# MAGIC ##### Attaching to a cluster
# MAGIC To run notebook code you need to attach it to a cluster. This isn't permanent, you can dettach and reattach or attach to a different cluster later. Just beneath the notebook name at the top is the cluster drop down menu, it should say "Dettached". Click to open the drop down and then select your cluster. (Note that clusters can take a few minutes to start up)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Running Code
# MAGIC Let's have a go at running some code. To run the code in a cell you can either
# MAGIC 
# MAGIC - click on the **play button** in the top right corner of the cell when you hover your mouse over the cell
# MAGIC - use the keyboard shortcut **CTRL+Enter** (to review all keyboard shortcuts click on the keyboard icon next to "Schedule" and "Comments" in the top right of the notebook)
# MAGIC 
# MAGIC Another handy shortcut is **SHIFT+Enter**, this runs the current cell and then moves the cursor to the next cell

# COMMAND ----------

for i in [3,2,1]:
    print(f"Lift off in T-{i} seconds")
print("LIFT OFF!!!")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Commands/Cells
# MAGIC 
# MAGIC You can **create a new command** by either:
# MAGIC - Hovering between the two commands where you want the new command then a **plus icon** will appear midway along the cells horizontally across the screen
# MAGIC - Hovering your mouse in a cell then choosing the **down arrow** in the top right of the cell and then "Add Cell Above/Below"
# MAGIC 
# MAGIC From the **down arrow** menu you can also
# MAGIC - copy, paste and cut cells
# MAGIC - move cells up and down
# MAGIC 
# MAGIC All these options can also be accessed from the **Edit menu** drop down at the top of the notebook (between "File" and "View")
# MAGIC 
# MAGIC Have a go at creating and manipulating some cells.

# COMMAND ----------

# example cell to move and play with
print("hello world")

# COMMAND ----------

# celda de ejemplo para moverse y jugar
print("hola mundo")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Magic Commands
# MAGIC 
# MAGIC Notebooks have a **default language** which you specify when you create the notebook. It is shown in brackets after the notebook name at the top and can be changed by clicking on the language.
# MAGIC 
# MAGIC Magic commands allow you to write code in languages other than the notebook default language. To specify a language in a cell you must begin the cell with `%language`. See the examples below.
# MAGIC 
# MAGIC Languages supported:
# MAGIC - Python
# MAGIC - Scala
# MAGIC - SQL
# MAGIC - R

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC val greeting: String = "Hello World!"
# MAGIC println(greeting)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 'HELLO WORLD!'

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC greeting <- "Hello World!"
# MAGIC print(greeting)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There are other magic commands too. Two useful ones are
# MAGIC - File System: `%fs` for exploring the Databricks file system (very useful as databricks doesn't have a GUI file explorer)
# MAGIC - Mark Down: `%md` for writing non-code cells such as this one

# COMMAND ----------

# MAGIC %fs ls ./databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### I am a mark down cell
# MAGIC Double click on me to edit me 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Databricks Tabs (vertical menu to the left)
# MAGIC 
# MAGIC - **Home**: Shortcut to your user folder in the workspace
# MAGIC - **Workspace**: Opens workspace explorer
# MAGIC - **Recents**: Shortcut to recently opened notebooks
# MAGIC - **Data**: Opens database explorer (see below for example)
# MAGIC - **Clusters**: Tab for managing clusters
# MAGIC - **Jobs**: Tab for managing jobs (scheduled runs of notebooks)
# MAGIC - **Models**: For machine learning models
# MAGIC - **Search**: For searching throughout the workspace
