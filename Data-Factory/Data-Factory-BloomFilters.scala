// Databricks notebook source
// MAGIC %sql
// MAGIC SET spark.databricks.io.skipping.bloomFilter.enabled = true;
// MAGIC SET delta.bloomFilter.enabled = true;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### First table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP table bloomfilters_v1;
// MAGIC -- 24B rows
// MAGIC CREATE TABLE bloomfilters_v1 ( ah 
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
// MAGIC  LOCATION 'wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-bloom_trx_1.delta/'

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
// MAGIC     delta.`wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-zo_trx.delta/`
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

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ## Second table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC DROP table bloomfilters_v2;
// MAGIC -- 24B rows
// MAGIC CREATE TABLE bloomfilters_v2 ( 
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
// MAGIC  LOCATION 'wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-bloom_trx_2.delta/'

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE BLOOMFILTER INDEX
// MAGIC ON TABLE bloomfilters_v2
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
// MAGIC     delta.`wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-zo_trx.delta/`
// MAGIC )
// MAGIC INSERT INTO bloomfilters_v2
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
// MAGIC OPTIMIZE bloomfilters_v2
// MAGIC ZORDER BY row_hash_z
