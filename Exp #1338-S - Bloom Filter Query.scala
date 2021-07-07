// Databricks notebook source
// MAGIC %md
// MAGIC <table>
// MAGIC   <tr>
// MAGIC     <td></td>
// MAGIC     <td>VM</td>
// MAGIC     <td>Quantity</td>
// MAGIC     <td>Total Cores</td>
// MAGIC     <td>Total RAM</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Driver:</td>
// MAGIC     <td>**Standard_DS3_v2**</td>
// MAGIC     <td>**1**</td>
// MAGIC     <td>**4 cores**</td>
// MAGIC     <td>**14 GB**</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Workers:</td>
// MAGIC     <td>**Standard_DS3_v2**</td>
// MAGIC     <td>**4**</td>
// MAGIC     <td>**4 cores**</td>
// MAGIC     <td>**14 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

val table_path_1 = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-bloom_trx_1.delta"
val table_path_2 = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-bloom_trx_2.delta'"

// COMMAND ----------

sc.setJobDescription("Step B: Create tables")

spark.sql("""CREATE TABLE bloomfilters_V1 
 USING delta 
 LOCATION 'wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-bloom_trx_1.delta' """)

spark.sql("""CREATE TABLE bloomfilters_V2 
 USING delta 
 LOCATION 'wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-bloom_trx_2.delta' """)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ### Find hash values for filtering

// COMMAND ----------

// MAGIC %sql SELECT * FROM bloomfilters_V1 where z_trx_id in ("10-01-00","99999999-09-08")

// COMMAND ----------

// MAGIC %sql SELECT * FROM bloomfilters_V2 where z_trx_id in ("10-01-00","99999999-09-08")

// COMMAND ----------

sc.setJobDescription("Step C: Establish baseline")

// Filtering string column which doesn't have BloomFilter and Z-Ordering on it

spark.read.table("bloomfilters_V1") \
  .filter(col("row_hash_basic") == "<replace with row_hash_basic column value from cmd5 >") \
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step D: Measure Z-Order performance ")

// Filtering string column which has Z-Ordering but no BloomFilter on it

spark.read.table("bloomfilters_V2") \
  .filter(col("row_hash_z") == "<replace with row_hash_z column value from cmd6 >") \
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step E: Measure Z-Order + Bloom Filter performance ")

// Filtering string column which has both BloomFilter and Z-Ordering on it

spark.read.table("bloomfilters_V1") \
  .filter(col("row_hash_z_bloom") == "<replace with row_hash_z_bloom column value from cmd5 >") \
  .write.format("noop").mode("overwrite").save()
