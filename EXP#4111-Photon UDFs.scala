// Databricks notebook source
sc.setJobDescription("Step A: Basic initialization")

import org.apache.spark.sql.functions._

// Disable IO cache so as to minimize side effects
spark.conf.set("spark.databricks.io.cache.enabled", false)

val sourceFile = "wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb.parquet"

val fullSchema = "transacted_at timestamp, trx_id string, retailer_id integer, description string, amount decimal(38,18), city_id integer"


def parserId(description:String): String = {
  val ccdId = "ccd id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (ccdId != null) return ccdId.substring(8)
  
  val ppdId = "ppd id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (ppdId != null) return ppdId.substring(8)
  
  val arcId = "arc id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (arcId != null) return arcId.substring(8)
  
  return null
}
def parseType(description:String): String = {
  val ccdId = "ccd id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (ccdId != null) return ccdId.substring(0, 3)
  
  val ppdId = "ppd id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (ppdId != null) return ppdId.substring(0, 3)
  
  val arcId = "arc id: \\d+".r.findFirstIn(description).getOrElse(null)
  if (arcId != null) return arcId.substring(0, 3)
  
  return null
}

val parserIdUDF = spark.udf.register("parserId", parserId _)
val parseTypeUDF = spark.udf.register("parseType", parseType _)

// COMMAND ----------

sc.setJobDescription("Step A: UDF with write on Delta table")

val baseTrxDF = spark
  .read.schema(fullSchema)                                     
  .parquet(sourceFile)                                         
  .select("description")

val trxDF = baseTrxDF
  .withColumn("trxType", parseTypeUDF($"description"))
  .withColumn("id", parserIdUDF($"description"))

trxDF.write.mode("overwrite").format("delta").save("/tmp/non_photon_udf_output")
