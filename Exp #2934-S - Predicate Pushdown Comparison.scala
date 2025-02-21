// Databricks notebook source
// MAGIC %md
// MAGIC <table>
// MAGIC   <tr>
// MAGIC     <td></td>
// MAGIC       <td>VM</td>
// MAGIC       <td>Quantity</td>
// MAGIC       <td>Total Cores</td>
// MAGIC       <td>Total RAM</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC       <td>Driver:</td>
// MAGIC       <td>**Standard_DS12_v2**</td>
// MAGIC       <td>**1**</td>
// MAGIC       <td>**4 cores**</td>
// MAGIC       <td>**28.0 GB**</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC       <td>Workers:</td>
// MAGIC       <td>**Standard_DS12_v2**</td>
// MAGIC       <td>**4**</td>
// MAGIC       <td>**8 cores**</td>
// MAGIC       <td>**56 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")
import org.apache.spark.sql.functions._

// Disable the Delta Cache (reduce side affects)
spark.conf.set("spark.databricks.io.cache.enabled", "false") 

// The city with the mostest
val targetCity = 2063810344

// COMMAND ----------

sc.setJobDescription("Step B: Establish a baseline")

// Not optimized for any type of filtering
val deltaPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.delta"

spark
  .read.format("delta").load(deltaPath)          // Load the delta table
  .filter($"city_id" === targetCity)             // Filter by target city
  .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step C: Partitioned by city")

// Partitioned on disk by city_id
val parPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb-par_city.delta"

spark.read.format("delta").load(parPath)            // Load the partitioned, delta table
     .filter($"p_city_id" === targetCity)           // Filter by target city
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step D: Z-Ordered by city")

// Delta table Z-Ordered by city_id
val zoPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb-zo_city.delta"

spark.read.format("delta").load(zoPath)             // Load the z-ordered, delta table
     .filter($"z_city_id" === targetCity)           // Filter by target city
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test

// COMMAND ----------

sc.setJobDescription("Step E: Bucketed by city")

// Parquet file previously bucketed by city_id into 400 buckets
val bucketPath = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb-bkt_city_400.parquet"

val tableName = "transactions_bucketed"               // The name of our table
spark.sql(s"CREATE DATABASE IF NOT EXISTS dbacademy") // Create the database
spark.sql(s"USE dbacademy")                           // Use the database
spark.sql(s"DROP TABLE IF EXISTS $tableName")         // Drop the table if it already exists

// Recate the buckted table from the provided files
spark.sql(s"""
  CREATE TABLE $tableName(transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,2), b_city_id integer)
  USING parquet 
  CLUSTERED BY(b_city_id) INTO 400 BUCKETS
  OPTIONS(PATH '$bucketPath')
""")

spark.read.table(tableName)                         // Load the bucketed table
     .filter($"b_city_id" === targetCity)           // Filter by target city
     .write.format("noop").mode("overwrite").save() // Execute a noop write to test
