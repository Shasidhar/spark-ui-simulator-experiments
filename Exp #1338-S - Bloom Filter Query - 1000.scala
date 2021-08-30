// Databricks notebook source
// MAGIC %md
// MAGIC Runtime: **DBR 8.4**
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
// MAGIC     <td>**n/a**</td>
// MAGIC     <td>**4 cores**</td>
// MAGIC     <td>**14 GB**</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Workers:</td>
// MAGIC     <td>**Standard_DS3_v2**</td>
// MAGIC     <td>**1**</td>
// MAGIC     <td>**4 cores**</td>
// MAGIC     <td>**14 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

// DBTITLE 1,Basic Initialization
val set = "tb-examples"
sc.setJobDescription("Step A: Basic Initialization")

import org.apache.spark.sql.functions._

// Disable the Delta IO Cache (reduce side affects)
spark.conf.set("spark.databricks.io.cache.enabled", false)   

val bloomFilter_enabled = spark.conf.get("spark.databricks.io.skipping.bloomFilter.enabled")

println(f"Bloom filter enabled: ${bloomFilter_enabled}")
println("-"*80)

// COMMAND ----------

// DBTITLE 1,Parquet Baselines
sc.setJobDescription("Step B-1: Establish baseline for long_value")

spark.read.parquet(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb.parquet")
  .filter($"long_value" === "2297823720")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step B-2: Establish baseline for hash_value")

spark.read.parquet(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb.parquet")
  .filter($"hash_value" === "f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

// DBTITLE 1,Delta Baselines
sc.setJobDescription("Step C-1: Establish baseline for long_value")

spark.read.load(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb.delta")
  .filter($"long_value" === "2297823720")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step C-2: Establish baseline for hash_value")

spark.read.load(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb.delta")
  .filter($"hash_value" === "f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

// DBTITLE 1,Z-Order Only
sc.setJobDescription("Step D-1:")

spark.read.load(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb-zordered-long.delta")
  .filter($"long_value" === "2297823720")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step D-2:")

spark.read.load(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb-zordered-hash.delta")
  .filter($"hash_value" === "f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

// DBTITLE 1,Bloom Filter Only
sc.setJobDescription("Step E-1:")

spark.read.load(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb-bloom-long.delta")
  .filter($"long_value" === "2297823720")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step E-2:")

spark.read.load(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb-bloom-hash.delta")
  .filter($"hash_value" === "f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

// DBTITLE 1,Z-Order & Bloom Filter
sc.setJobDescription("Step F-1:")

spark.read.load(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb-zo-bloom-long.delta")
  .filter($"long_value" === "2297823720")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step F-2:")

spark.read.load(f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/${set}/1tb-zo-bloom-hash.delta")
  .filter($"hash_value" === "f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2")
  .write.format("noop").mode("overwrite").save()
