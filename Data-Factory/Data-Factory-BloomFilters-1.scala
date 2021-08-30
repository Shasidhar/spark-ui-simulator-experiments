// Databricks notebook source
// MAGIC %sql
// MAGIC SET spark.databricks.io.skipping.bloomFilter.enabled = true;
// MAGIC SET delta.bloomFilter.enabled = true;

// COMMAND ----------

// MAGIC %python
// MAGIC # This SAS token should be valid for no more than 8 hours
// MAGIC sas_token = "sp=racwdl&st=2021-08-03T16:30:40Z&se=2021-08-11T00:30:40Z&spr=https&sv=2020-08-04&sr=c&sig=QUfhbcs1BVddsDRHXuY7rNrv7lwSJrSzvvR5k1icbJk%3D"
// MAGIC config_map = {"fs.azure.sas.spark-ui-simulator.dbacademy.blob.core.windows.net": sas_token}
// MAGIC 
// MAGIC mount_point = f"/mnt/dbacademy/spark-ui-simulator-rw"
// MAGIC source_path = f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net"
// MAGIC 
// MAGIC try: dbutils.fs.unmount(mount_point)
// MAGIC except: print(f"{mount_point} is not mounted...")
// MAGIC 
// MAGIC dbutils.fs.mount(source_path, mount_point, extra_configs=config_map)
// MAGIC print(f"{mount_point} has been mounted from {source_path}")

// COMMAND ----------

// MAGIC %fs ls /mnt/dbacademy/spark-ui-simulator-rw/global-sales/transactions

// COMMAND ----------

spark.read.format("delta").load("dbfs:/mnt/dbacademy/spark-ui-simulator-rw/global-sales/transactions/2011-to-2018-1tb-zo_trx.delta").count()

// COMMAND ----------

spark.read.format("delta").load("dbfs:/mnt/dbacademy/spark-ui-simulator-rw/global-sales/transactions/2011-to-2018-1tb-bloom_trx_1.delta").count()

// COMMAND ----------

spark.read.format("delta").load("dbfs:/mnt/dbacademy/spark-ui-simulator-rw/global-sales/transactions/2011-to-2018-1tb-bloom_trx_2.delta").count()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### First table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP table IF EXISTS bloomfilters_v1;
// MAGIC -- 24B rows
// MAGIC CREATE TABLE bloomfilters_v1 ( 
// MAGIC     `transacted_at` TIMESTAMP, 
// MAGIC     `z_city_id` INT, 
// MAGIC     `z_trx_id` STRING, 
// MAGIC     `z_retailer_id` INT, 
// MAGIC     `description` STRING, 
// MAGIC     `amount` DECIMAL(38,2), 
// MAGIC     `row_hash_basic` STRING, 
// MAGIC     `row_hash_z` STRING,
// MAGIC     `row_hash_z_bloom` STRING) 
// MAGIC  USING delta 
// MAGIC  LOCATION '/mnt/dbacademy/spark-ui-simulator-rw/global-sales/transactions/2011-to-2018-1tb-bloom_trx_1.delta/'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE BLOOMFILTER INDEX
// MAGIC ON TABLE bloomfilters_v1
// MAGIC 
// MAGIC FOR COLUMNS(row_hash_z_bloom OPTIONS (fpp=0.1, numItems=50000000))

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC WITH sample (
// MAGIC   SELECT
// MAGIC     *,
// MAGIC     sha (z_trx_id) row_hash_basic,
// MAGIC     sha (concat_ws(z_trx_id,rand(555),z_retailer_id)) row_hash_z,
// MAGIC     sha (concat_ws(z_trx_id, z_retailer_id,rand(99999),z_city_id)) row_hash_z_bloom 
// MAGIC     from
// MAGIC     delta.`/mnt/dbacademy/spark-ui-simulator-rw/global-sales/transactions/2011-to-2018-1tb-zo_trx.delta/`
// MAGIC )
// MAGIC INSERT INTO bloomfilters_v1
// MAGIC SELECT transacted_at, 
// MAGIC     z_city_id,
// MAGIC     z_trx_id,
// MAGIC     z_retailer_id,
// MAGIC     description,
// MAGIC     amount,
// MAGIC     row_hash_basic,
// MAGIC     row_hash_z,
// MAGIC     row_hash_z_bloom
// MAGIC FROM sample

// COMMAND ----------

// MAGIC %sql
// MAGIC OPTIMIZE bloomfilters_v1
// MAGIC ZORDER BY row_hash_z_bloom
