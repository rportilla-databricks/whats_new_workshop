# Databricks notebook source
# MAGIC %md #Delta Change Data Feed: AirBnB Demonstration

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/Delta_CDF_flow.png" width=1000/>

# COMMAND ----------

# MAGIC %md ##### Setup

# COMMAND ----------

# DBTITLE 1,Imports, Download and Cleanups
from os import path,system
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
from delta.tables import *

# Create widgets
dbutils.widgets.text('home_dir', 'cdf_demo@databricks.com', 'Home Directory')
dbutils.widgets.text('db', 'cdf_demo', 'Database')

file_dates_mapping = {"2020-09-09": "Sep2020","2020-10-09": "Oct2020","2020-11-03": "Nov2020", "2020-12-12": "Dec2020","2021-01-09": "Jan2021" }

dir_to_download = f"/Users/{dbutils.widgets.get('home_dir')}/AMS_AirBnB/"
dbutils.fs.mkdirs(dir_to_download)

# Download the data if needed 
for date, file_name in file_dates_mapping.items():
  if path.exists(f"/dbfs{dir_to_download}{file_name}.csv.gz"):
    print(f"Found file /dbfs{dir_to_download}{file_name}.csv.gz exists so not downloading file!")
  else:
    download_url = f'http://data.insideairbnb.com/the-netherlands/north-holland/amsterdam/{date}/data/listings.csv.gz'
    print(f"Downloading file: {download_url}")
    cmd = f'wget {download_url} -O /dbfs{dir_to_download}{file_name}".csv.gz"'
    system(cmd)
   
# Cleanup the old tables
spark.sql(f"create database if not exists {dbutils.widgets.get('db')}")
spark.sql(f"use {dbutils.widgets.get('db')}")
spark.sql("""drop table if exists Amsterdam_Reviews_Bronze;""")
spark.sql("""drop table if exists Amsterdam_Reviews_Silver;""")
spark.sql("""drop table if exists Amsterdam_Reviews_Gold;""")

# spark.sql("""drop table if exists AMSRent_Bronze;""")
# spark.sql("""drop table if exists AMSRent_Silver;""")
# spark.sql("""drop table if exists AMSRent_Gold;""")

# Given a month in the format e.g. Sep2020, append the .csv.gz file to the Bronze table
def add_month_to_bronze(month):
  spark.createDataFrame(pd.read_csv(f"/dbfs{dir_to_download}{month}.csv.gz")) \
    .drop("minimum_minimum_nights") \
    .drop("maximum_minimum_nights") \
    .drop("minimum_maximum_nights") \
    .drop("maximum_maximum_nights") \
    .write.format('delta').mode("append").saveAsTable('Amsterdam_Reviews_Bronze')

# Seed bronze table with only September data
add_month_to_bronze('Sep2020')


# COMMAND ----------

# DBTITLE 1,Initial Bronze data with only September 2020 rentals
# MAGIC %sql SELECT * FROM Amsterdam_Reviews_Bronze LIMIT 3

# COMMAND ----------

# MAGIC %md ## Delta CDF Overview

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/Delta_CDF_process_records.png" width=1000/>
# MAGIC 
# MAGIC <p>We start with the original table, which has three records (A1 through A3) and two fields (PK and B)
# MAGIC 
# MAGIC <p>We then receive an updated version of this table that shows a change to field B in record A2, removal of record A3, and the addition of record A4
# MAGIC 
# MAGIC <p>As this gets processed, the Delta Change Data Feed captures only records for which there was a change. This is what allows Delta Change Data Feed to speed up ETL pipelines as less data is being touched. 
# MAGIC <p>Note that record A1 is not reflected in the Change Data Feed output as no changes were made to that record.
# MAGIC <p>In the case of updates, the output contains what the record looked like prior to the change, called the preimage, and what it held after the change, called the postimage. This can be particularly helpful when producing aggregated facts or materialized views as appropriate updates could be made to individual records without having to reprocess all of the data underlying the table. This allows changes to be reflected more quickly in the data used for BI and visualization.
# MAGIC <p>For deletes and inserts, the affected record is shown with an indication of whether it is being added or removed.
# MAGIC <p>Additionally, the Delta version is noted so that logs are maintained of what happened to the data when. This allows greater granularity for regulatory and audit purposes as needed. We do capture the timestamp of these commits as well and will show this in a few minutes.
# MAGIC <p>While this depicts a batch process, the source of updates could just as easily by a stream instead. The Change Data Feed output would be the same.

# COMMAND ----------

# DBTITLE 1,Enable CDF for a single table (SQL, Python, Scala)
# MAGIC %sql
# MAGIC ALTER TABLE Amsterdam_Reviews_Bronze SET TBLPROPERTIES (delta.enableChangeDataCapture = true);

# COMMAND ----------

# DBTITLE 1,...OR... Enable CDF for a cluster (Python, Scala)
spark.conf.set('spark.databricks.delta.properties.defaults.enableChangeDataCapture',True)

# COMMAND ----------

# DBTITLE 1,No Changes to Bronze Table So Far
# MAGIC %sql DESCRIBE HISTORY Amsterdam_Reviews_Bronze

# COMMAND ----------

# DBTITLE 1,Let's Emulate Some New Reviews for October 2020
add_month_to_bronze("Oct2020")

# COMMAND ----------

# DBTITLE 0,No Changes to Bronze Table So Far
# MAGIC %sql DESCRIBE HISTORY Amsterdam_Reviews_Bronze

# COMMAND ----------

# DBTITLE 1,Simple CDF query to see all changes since October dump
# MAGIC %sql SELECT name, last_scraped, amenities, _change_type, _commit_version, _commit_timestamp FROM table_changes('Amsterdam_Reviews_Bronze', 2) limit 10

# COMMAND ----------

# DBTITLE 1,Add November
add_month_to_bronze("Nov2020")

# COMMAND ----------

# DBTITLE 1,View all changes between two versions or timestamps
# MAGIC %sql
# MAGIC SELECT
# MAGIC   _commit_version,
# MAGIC   _change_type,
# MAGIC   LAST(_commit_timestamp) as commit_time,
# MAGIC   count(*) as num_changes
# MAGIC FROM
# MAGIC   table_changes('Amsterdam_Reviews_Bronze', 1, 3)
# MAGIC GROUP BY
# MAGIC   _commit_version,
# MAGIC   _change_type

# COMMAND ----------

# MAGIC %md #### Incremental Updates

# COMMAND ----------

# DBTITLE 1,Populate base data
# MAGIC %sql
# MAGIC -- We enable ChangeDataFeed on the cluster level, any new table will record the records changes
# MAGIC CREATE OR REPLACE TABLE Amsterdam_Reviews_Silver (
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
# MAGIC 
# MAGIC INSERT INTO
# MAGIC   TABLE Amsterdam_Reviews_Silver
# MAGIC SELECT
# MAGIC   id,
# MAGIC   name,
# MAGIC   last_scraped,
# MAGIC   host_acceptance_rate,
# MAGIC   last_review,
# MAGIC   review_scores_rating,
# MAGIC   beds,
# MAGIC   cast(replace(replace(price,'$',''),',','') as decimal(18,2))  as price
# MAGIC FROM
# MAGIC   table_changes('Amsterdam_Reviews_Bronze', 3)
# MAGIC WHERE
# MAGIC   NOT (
# MAGIC     last_scraped IS null
# MAGIC     OR host_acceptance_rate IS null
# MAGIC     OR last_review IS null
# MAGIC     OR review_scores_rating IS null
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,Ingest new data to Bronze
add_month_to_bronze("Dec2020")

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY Amsterdam_Reviews_Bronze

# COMMAND ----------

# DBTITLE 1,Incremental Merges
# MAGIC %sql 
# MAGIC MERGE INTO Amsterdam_Reviews_Silver silver USING (
# MAGIC   SELECT *
# MAGIC   FROM table_changes('Amsterdam_Reviews_Bronze', ${last_tx})
# MAGIC   WHERE NOT (
# MAGIC       last_scraped IS null
# MAGIC       OR host_acceptance_rate IS null
# MAGIC       OR last_review IS null
# MAGIC       OR review_scores_rating IS null
# MAGIC     )
# MAGIC ) bronze_changes ON silver.id = bronze_changes.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql SELECT * FROM Amsterdam_Reviews_Silver

# COMMAND ----------

# MAGIC %md
# MAGIC #### Minor Data Fixes
# MAGIC * As we processed the records we noticed that there's an error in June 2019 review date, we will adjust the price up by 10% <br>
# MAGIC * We will delete any data prior to 2015 as well

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE Amsterdam_Reviews_Silver
# MAGIC   set price=price*1.1
# MAGIC   where substr(last_review,0,7)='2019-06';

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM Amsterdam_Reviews_Silver
# MAGIC   where cast(substr(last_review,0,4) as integer) < 2016;

# COMMAND ----------

# MAGIC %sql SELECT count(1),_change_type from   table_changes('Amsterdam_Reviews_Silver', 2) where _change_type != 'update_preimage' group by _change_type

# COMMAND ----------

# MAGIC %md ## Example CDF based Use Cases
# MAGIC + <a href="$./AirBnB_CDF_Under_The_Hood">CDF Under the hood</a>
# MAGIC + <a href="$./AirBnB_Gold_table_MV">Update a Gold Level table and a Materialized View</a>
# MAGIC + <a href="$./AirBnB_Downstream">Send changes to a downstream application</a>
# MAGIC + <a href="$./AirBnB_Audit_Trail">Create an Audit trail table</a>
# MAGIC + <a href="$./AirBnB_display_differences">Identify differences in fields and values between table versions<a>
