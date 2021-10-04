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
// MAGIC     <td>**Standard_E4ds_v4**</td>
// MAGIC     <td>**1**</td>
// MAGIC     <td>**4 cores**</td>
// MAGIC     <td>**32 GB**</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Workers:</td>
// MAGIC     <td>**Standard_E4ds_v4**</td>
// MAGIC     <td>**2**</td>
// MAGIC     <td>**8 cores**</td>
// MAGIC     <td>**64 GB**</td>
// MAGIC   </tr>
// MAGIC </table>
// MAGIC 
// MAGIC ### DBR - 9.1 LTS Photon (includes Apache Spark 3.1.2, Scala 2.12)

// COMMAND ----------

// MAGIC %md
// MAGIC ## With Photon

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

val sourceFile = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.parquet"

val fullSchema = "transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,18), city_id integer"

sc.setJobDescription("Step B: Project and Drop a column from parquet. Uses photon in background.")

spark
  .read.schema(fullSchema)                                     // Specifying all columns
  .parquet(sourceFile)                                         // Load the transactions table
  .select("trx_id", "retailer_id", "city_id", "transacted_at") // Select 4 columns
  .drop("transacted_at")                                       // Drop the 4th column
  .write.format("noop").mode("overwrite").save()               // Test with a noop write

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Without Photon

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

val sourceFile = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.parquet"

val fullSchema = "transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,18), city_id integer"

sc.setJobDescription("Step B: Project and Drop a column from parquet. Uses photon in background.")

spark
  .read.schema(fullSchema)                                     // Specifying all columns
  .parquet(sourceFile)                                         // Load the transactions table
  .select("trx_id", "retailer_id", "city_id", "transacted_at") // Select 4 columns
  .drop("transacted_at")                                       // Drop the 4th column
  .write.format("noop").mode("overwrite").save()               // Test with a noop write
