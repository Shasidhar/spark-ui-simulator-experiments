# Databricks notebook source
# This SAS token should be valid for no more than 8 hours
sas_token = "sp=racwdl&st=2021-08-27T20:30:30Z&se=2021-08-31T04:30:30Z&spr=https&sv=2020-08-04&sr=c&sig=bozsjNyIeMSj9eV1kbgRBaYp%2BWiK10Q5r%2FB2%2BIU2U9g%3D"
config_map = {"fs.azure.sas.spark-ui-simulator.dbacademy.blob.core.windows.net": sas_token}

mount_point = f"/mnt/dbacademy/spark-ui-simulator-rw"
source_path = f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net"

try: dbutils.fs.unmount(mount_point)
except: print(f"{mount_point} is not mounted...")

dbutils.fs.mount(source_path, mount_point, extra_configs=config_map)
print(f"{mount_point} has been mounted from {source_path}")

display(dbutils.fs.ls(mount_point))


# COMMAND ----------

# 1000 == partitions 1G each
# 2000 == partitions 512M each
# 4000 == partitions 256M each
# 8000 == partitions 128M each

# We want all part-files to be at 128 MB so that when we do
# optimize these datasets, we can be ensured that the files
# will actually be rewritten, subsequently ending with 1GB part files
partitions = 4000
partitions_parquet = 1000

import time
from pyspark.sql.functions import *

record_count = 15500000000 # magic number that will produce 1TB of data
base_path = f"/mnt/dbacademy/spark-ui-simulator-rw/tb-examples"

spark.conf.set("spark.sql.shuffle.partitions", partitions)

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", False)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", False)

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

from delta.tables import *
tb_delta_path = f"{base_path}/1tb.delta"
source_table = DeltaTable.forPath(spark, tb_delta_path)

# COMMAND ----------

# DBTITLE 1,Create the Master Parquet Table
# We will not use this table for subsequent examples,
# but given how we create these tables it whould be 
# 100% identical in content to the others

# tb_parquet_path = f"{base_path}/1tb.parquet"
# dbutils.fs.rm(tb_parquet_path, True)

# df = (spark
#   .range(0, record_count, numPartitions=partitions_parquet)
#   .withColumnRenamed("id", "long_value")
#   .withColumn("hash_value", sha2(col("long_value").cast("string"), 256))
# )
# df.write.format("parquet").mode("overwrite").save(tb_parquet_path)

# COMMAND ----------

# DBTITLE 1,TB Parquet Stats
total = int(__builtin__.sum(list(map(lambda f: f.size, dbutils.fs.ls(tb_parquet_path))))/1024/1024/1024)
records = spark.read.format("parquet").load(tb_parquet_path).count()

print(f"{total:,d} MB")
print(f"{records:,d}")

# COMMAND ----------

def write_table(path):
  dbutils.fs.rm(path, True)

  df = (spark
    .range(0, record_count, numPartitions=partitions)
    .withColumnRenamed("id", "long_value")
    .withColumn("hash_value", sha2(col("long_value").cast("string"), 256))
  )
  df.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

# DBTITLE 1,Create the Master Delta Table
# dbutils.fs.rm(tb_delta_path, True)

# df = (spark
#   .range(0, record_count, numPartitions=partitions)
#   .withColumnRenamed("id", "long_value")
#   .withColumn("hash_value", sha2(col("long_value").cast("string"), 256))
# )
# df.write.format("delta").mode("overwrite").save(tb_delta_path)

# We will NOT optimize this table yet - it will be
# used as the source of our other permutations and
# only AFTER that, can we optimize this table.

# COMMAND ----------

# DBTITLE 1,TB Delta Stats
total = int(__builtin__.sum(list(map(lambda f: f.size, dbutils.fs.ls(tb_delta_path))))/1024/1024/1024)
records = spark.read.format("delta").load(tb_delta_path).count()

print(f"{total:,d} MB")
print(f"{records:,d}")

# COMMAND ----------

# DBTITLE 1,TB Z-Ordered, Long_Value
dest_path = f"{base_path}/1tb-zordered-long.delta"
write_table(dest_path)

spark.sql(f"OPTIMIZE delta.`{dest_path}` ZORDER BY long_value")
spark.sql(f"VACUUM delta.`{dest_path}` RETAIN 0 HOURS")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("long_value == '2297823720'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Z-Ordered, Hash_Value
dest_path = f"{base_path}/1tb-zordered-hash.delta"
write_table(dest_path)

spark.sql(f"OPTIMIZE delta.`{dest_path}` ZORDER BY hash_value")
spark.sql(f"VACUUM delta.`{dest_path}` RETAIN 0 HOURS")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("hash_value == 'f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Z-Ordered, Bloom Filter, Long_Value
dest_path = f"{base_path}/1tb-zo-bloom-long.delta"

write_table(dest_path)
files = dbutils.fs.ls(dest_path)
display(files)

spark.sql(f"CREATE BLOOMFILTER INDEX ON TABLE delta.`{dest_path}` FOR COLUMNS(long_value OPTIONS (fpp=0.1, numItems={record_count}))")
spark.sql(f"OPTIMIZE delta.`{dest_path}` ZORDER BY long_value")
spark.sql(f"VACUUM delta.`{dest_path}` RETAIN 0 HOURS")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("long_value == '2297823720'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Z-Ordered, Bloom Filter, Hash_Value
dest_path = f"{base_path}/1tb-zo-bloom-hash.delta"
dbutils.fs.rm(dest_path, True)

source_table.clone(dest_path, isShallow=False, replace=True)

spark.sql(f"CREATE BLOOMFILTER INDEX ON TABLE delta.`{dest_path}` FOR COLUMNS(hash_value OPTIONS (fpp=0.1, numItems={record_count}))")
spark.sql(f"OPTIMIZE delta.`{dest_path}` ZORDER BY hash_value")
spark.sql(f"VACUUM delta.`{dest_path}` RETAIN 0 HOURS")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("hash_value == 'f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Bloom Filter Only, Long_Value
dest_path = f"{base_path}/1tb-bloom-long.delta"
dbutils.fs.rm(dest_path, True)

source_table.clone(dest_path, isShallow=False, replace=True)

spark.sql(f"CREATE BLOOMFILTER INDEX ON TABLE delta.`{dest_path}` FOR COLUMNS(long_value OPTIONS (fpp=0.1, numItems={record_count}))")
spark.sql(f"OPTIMIZE delta.`{dest_path}`")
spark.sql(f"VACUUM delta.`{dest_path}` RETAIN 0 HOURS")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("long_value == '2297823720'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Bloom Filter Only, Hash_Value
dest_path = f"{base_path}/1tb-bloom-hash.delta"
dbutils.fs.rm(dest_path, True)

source_table.clone(dest_path, isShallow=False, replace=True)

spark.sql(f"CREATE BLOOMFILTER INDEX ON TABLE delta.`{dest_path}` FOR COLUMNS(hash_value OPTIONS (fpp=0.1, numItems={record_count}))")
spark.sql(f"OPTIMIZE delta.`{dest_path}`")
spark.sql(f"VACUUM delta.`{dest_path}` RETAIN 0 HOURS")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("hash_value == 'f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,Last Steps
# Our 1TB Delta table is no longer being used to clone
# and we can now optimize it bringing it to 1TB
spark.sql(f"OPTIMIZE delta.`{tb_delta_path}`")
