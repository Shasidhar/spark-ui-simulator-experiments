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
// MAGIC     <td>**4**</td>
// MAGIC     <td>**16 cores**</td>
// MAGIC     <td>**56 GB**</td>
// MAGIC   </tr>
// MAGIC </table>

// COMMAND ----------

sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

val table_path_1 = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-bloom_trx_1.delta"
val table_path_2 = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-1tb-bloom_trx_2.delta"

// COMMAND ----------

sc.setJobDescription("Step C: Establish baseline")

// Filtering on row_hash_basic which doesn't have BloomFilter or Z-Ordering on it

spark.read.format("delta").load(table_path_1)
  .filter($"row_hash_basic" === "e1c5fa8e500394b84339596f006000a139a71ed3")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step D: Measure Z-Order performance ")

// Filtering on row_hash_z which has Z-Ordering but no BloomFilter on it

spark.read.format("delta").load(table_path_2)
  .filter($"row_hash_z" === "6baa7894b806186de4852e0708245d902ef3563b")
  .write.format("noop").mode("overwrite").save()

// COMMAND ----------

sc.setJobDescription("Step E: Measure Z-Order + Bloom Filter performance ")

// Filtering on row_hash_z_bloom which has both BloomFilter and Z-Ordering on it

spark.read.format("delta").load(table_path_1)
  .filter($"row_hash_z_bloom" === "301b70ea95e945df37113c67b6df903889a21646")
  .write.format("noop").mode("overwrite").save()
