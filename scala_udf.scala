
sc.setJobDescription("Step C: Establish a baseline")

val baseTrxDF = spark
  .read.table("transactions")
  .select("description")

baseTrxDF.write.mode("overwrite").format("noop").save()


val initDF = spark
  .read
  .format("delta")
  .load("wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net/global-sales/transactions/2011-to-2018-100gb-par_year.delta")

initDF.createOrReplaceTempView("transactions")

// Printing the schema here forces spark to read the schema
// avoiding side effects in future benchmarks
initDF.printSchema


sc.setJobDescription("Step C: Establish a baseline")

val baseTrxDF = spark
  .read.table("transactions")
  .select("description")

baseTrxDF.write.mode("overwrite").format("noop").save()

"""
string concatination
"""


def add_string(description: String): String = {
    val ccdId =  "hhh_hhh_hhh_hhh_hhh_hhh_hhh_hhh_hhh_hhh_hhh_hhh_hhh_hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhgvhgghgjgjgjhghgjhgjhggjgjhgjgjhgjghgjhgjggjhgjhgjhgjhgjhgjhghjgjhgjhghh".concat(description)
    ccdId
}

val add_string_UDF = udf(add_string _)



val trxDF = (baseTrxDF
  .withColumn("added", add_string_UDF(col("description")))
)
trxDF.write.mode("overwrite").format("noop").save()



"""
Adding calculations
"""
%scala
def calculate(description: String): Int= {
    val ccdId = description.length()/1000*200+17-11%2+5467
    ccdId
}

val calculate_UDF = udf(calculate _)



val trxDF = (baseTrxDF
  .withColumn("added", calculate_UDF(col("description")))
)
trxDF.write.mode("overwrite").format("noop").save()
