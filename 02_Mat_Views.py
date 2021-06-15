# Databricks notebook source
# MAGIC %md
# MAGIC <a href="$./AMS_AirBnB_Change_Data_Feed_demo">Return to top level notebook</a>

# COMMAND ----------

# DBTITLE 1,Imports
from os import path,system
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from delta.tables import *

# Create widgets
dbutils.widgets.text('home_dir', 'cdf_demo@databricks.com', 'Home Directory')
dbutils.widgets.text('db', 'cdf_demo', 'Database')

# Cleanup the old tables
spark.sql(f"use {dbutils.widgets.get('db')}")

# COMMAND ----------

# MAGIC %md # populate a gold level table from Silver table Change Data Feed

# COMMAND ----------

# DBTITLE 1,Input table
# MAGIC %sql select * from Amsterdam_Reviews_Silver order by id limit 5

# COMMAND ----------

# DBTITLE 1,Create a final version for user consumption
# MAGIC %sql
# MAGIC -- We enable ChangeDataFeed on the cluster level, any new table will record the records changes
# MAGIC CREATE OR REPLACE TABLE Amsterdam_Reviews_Gold (
# MAGIC   id string,
# MAGIC   name string,
# MAGIC   last_scraped string,
# MAGIC   host_acceptance_rate string,
# MAGIC   last_review string,
# MAGIC   review_scores_rating string,
# MAGIC   beds double,
# MAGIC   price decimal(18,2)
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,In SQL - using Merge command
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Amsterdam_Reviews_Gold_Merge USING DELTA AS SELECT * FROM Amsterdam_Reviews_Gold where 1 = 2;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Amsterdam_Reviews_Gold_Merge_Prep as
# MAGIC     SELECT id, last_scraped, name, host_acceptance_rate, last_review, review_scores_rating, beds, price, _change_type, _commit_version, lead__change_type
# MAGIC     from
# MAGIC          (SELECT lead(_change_type) over (partition by id order by _change_type) as lead__change_type,*
# MAGIC          FROM table_changes('Amsterdam_Reviews_Silver', 0)
# MAGIC          -- we do not need the update_preimage record
# MAGIC          WHERE _change_type != 'update_preimage') lead_view
# MAGIC     -- select the latest record for the version
# MAGIC     WHERE lead__change_type is null;
# MAGIC 
# MAGIC MERGE INTO Amsterdam_Reviews_Gold_Merge as gold
# MAGIC USING Amsterdam_Reviews_Gold_Merge_Prep AS prep
# MAGIC ON prep.id = gold.id 
# MAGIC WHEN MATCHED  AND prep._change_type = "delete" THEN DELETE
# MAGIC WHEN MATCHED  AND prep._change_type = "update_postimage" THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN insert *

# COMMAND ----------

# DBTITLE 1,Streaming - in Python
spark.sql("""CREATE OR REPLACE TABLE Amsterdam_Reviews_Gold_Append USING DELTA AS SELECT * FROM Amsterdam_Reviews_Gold where 1 = 2;""")
gold_delta = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/cdf_demo.db/amsterdam_reviews_gold_append")
gold_cols= gold_delta.toDF().columns

silver_CDC_stream_df = spark.readStream \
      .format("delta") \
      .option("readChangeData", "true") \
      .option("startingVersion", 0) \
      .table("Amsterdam_Reviews_Silver") \
      .filter("_change_type !='update_preimage'")
  
w = Window.partitionBy("id").orderBy(desc("_commit_version"))
  
def upsertToDeltaIncremental(microBatchOutputDF, batchId):
  silver = microBatchOutputDF.withColumn("__idx", row_number().over(w)) \
      .where("__idx = 1") \
      .drop("__idx") \
      .select(gold_cols) \
      .alias("silver")
  gold_delta.alias("gold").merge(
    silver,
    "silver.id = gold.id ") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

silver_CDC_stream_df.writeStream \
  .format("delta") \
  .foreachBatch(upsertToDeltaIncremental) \
  .trigger(once=True) \
  .outputMode("append") \
  .start()

# COMMAND ----------

# MAGIC %sql select count(1),id from Amsterdam_Reviews_Gold_Append group by id order by 1 desc

# COMMAND ----------

# MAGIC %sql describe history Amsterdam_Reviews_Gold_Append

# COMMAND ----------

# MAGIC %md ### populate a materialized view or Snapshot table

# COMMAND ----------

# DBTITLE 1,Input data
# MAGIC %sql select sum(beds) as total_beds,last_review from Amsterdam_Reviews_Gold_Append group by last_review

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS Amsterdam_Reviews_Gold_Mat_View (
# MAGIC   last_review string,
# MAGIC   total_beds decimal(18,2)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC MERGE INTO Amsterdam_Reviews_Gold_Mat_View mat_view
# MAGIC USING (SELECT sum(beds) as total_beds,last_review
# MAGIC        FROM Amsterdam_Reviews_Gold_Merge_Prep
# MAGIC        GROUP BY last_review) as gold
# MAGIC ON mat_view.last_review = gold.last_review
# MAGIC WHEN MATCHED THEN UPDATE SET total_beds=mat_view.total_beds+gold.total_beds
# MAGIC WHEN NOT MATCHED THEN insert *

# COMMAND ----------

# MAGIC %md ###Advaced - Retain processed version in CDF Batch mode

# COMMAND ----------

# DBTITLE 1,UDF to read and write the version metadata
#  Define functions for processing Bronze to Silver.  Adds change data.
from delta.tables import *
import datetime
import json
import time

PREFIX = "increment-"

def get_latest_processed(startingVersion=0):
  latest = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/cdf_demo.db/amsterdam_reviews_gold_batch") \
    .history() \
    .filter(col("userMetadata") \
            .like(f"{PREFIX}%")) \
    .orderBy(col("version").desc()) \
    .select("userMetadata") \
    .take(1)
  
  # if it finds a snapshot it will return snapshot otherwise it will get you to read the table from begining
  if len(latest) > 0:
    return ["startingTimestamp", latest[0].userMetadata.lstrip(PREFIX)]
  else:
    return ["startingVersion", startingVersion]
  
def update_batch_gold_table():
  # Read the Silver table
  this_silver_increment = spark.read.format("delta") \
        .option("readChangeData", "true") \
        .option(*get_latest_processed(0)) \
        .table("Amsterdam_Reviews_Silver") \
        .filter(col('_change_type') != 'update_preimage')

  # List all the new versions
  versions = ", ".join([str(row["_commit_version"]) for row in this_silver_increment.select(col("_commit_version")).distinct().collect()])
  print(f"Processing increment of: {this_silver_increment.count()} rows with versions: {versions} into gold table!")

  # Prep the data for a Merge
  this_silver_increment = this_silver_increment \
                          .withColumn('rank', rank().over(Window.partitionBy("id").orderBy(col('_commit_version').desc()))) \
                          .filter(col('rank') == 1).drop('rank') 

  # Set the processed version
  processed_metadata = PREFIX+datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
  spark.sql("set spark.databricks.delta.commitInfo.userMetadata="+processed_metadata)

  # Merge the data into the Silver
  gold_delta.alias("gold").merge(
    this_silver_increment.alias("silver"),
    "gold.id = silver.id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS Amsterdam_Reviews_Gold_Batch;""")
spark.sql("set spark.databricks.delta.commitInfo.userMetadata=''")

spark.sql("""CREATE TABLE Amsterdam_Reviews_Gold_Batch USING DELTA AS SELECT * FROM Amsterdam_Reviews_Gold where 1 = 2;""")
gold_delta = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/cdf_demo.db/amsterdam_reviews_gold_batch")

update_batch_gold_table()

# COMMAND ----------

# MAGIC %sql UPDATE Amsterdam_Reviews_Silver SET price=price=1.1 WHERE last_review like '2020-02%'

# COMMAND ----------

update_batch_gold_table()

# COMMAND ----------

# MAGIC %md ## Example CDF based Use Cases
# MAGIC + <a href="$./AirBnB_CDF_Under_The_Hood">CDF Under the hood</a>
# MAGIC + <a href="$./AirBnB_Gold_table_MV">Update a Gold Level table and a Materialized View</a>
# MAGIC + <a href="$./AirBnB_Downstream">Send changes to a downstream application</a>
# MAGIC + <a href="$./AirBnB_Audit_Trail">Create an Audit trail table</a>
# MAGIC + <a href="$./AirBnB_display_differences">Identify differences in fields and values between table versions<a>
