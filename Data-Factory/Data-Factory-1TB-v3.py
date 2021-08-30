# Databricks notebook source
# # This SAS token should be valid for no more than 8 hours
# sas_token = "sp=racwdl&st=2021-08-27T20:30:30Z&se=2021-08-31T04:30:30Z&spr=https&sv=2020-08-04&sr=c&sig=bozsjNyIeMSj9eV1kbgRBaYp%2BWiK10Q5r%2FB2%2BIU2U9g%3D"
# config_map = {"fs.azure.sas.spark-ui-simulator.dbacademy.blob.core.windows.net": sas_token}

# mount_point = f"/mnt/dbacademy/spark-ui-simulator-rw"
# source_path = f"wasbs://spark-ui-simulator@dbacademy.blob.core.windows.net"

# try: dbutils.fs.unmount(mount_point)
# except: print(f"{mount_point} is not mounted...")

# dbutils.fs.mount(source_path, mount_point, extra_configs=config_map)
# print(f"{mount_point} has been mounted from {source_path}")

# display(dbutils.fs.ls(mount_point))

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/dbacademy/spark-ui-simulator-rw/tb-examples

# COMMAND ----------

# 1000 == partitions 1G each
# 2000 == partitions 512M each
# 4000 == partitions 256M each
# 8000 == partitions 128M each

# We want all part-files to be at 128 MB so that when we do
# optimize these datasets, we can be ensured that the files
# will actually be rewritten, subsequently ending with 1GB part files
partitions = 4000

import time
from pyspark.sql.functions import *

record_count = 15500000000 # magic number that will produce 1TB of data
base_path = f"/mnt/dbacademy/spark-ui-simulator-rw/tb-examples"

spark.conf.set("spark.sql.shuffle.partitions", partitions)

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", False)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", False)

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

tb_parquet_path = f"{base_path}/1tb.parquet"

# COMMAND ----------

# DBTITLE 1,Create the Master Parquet Table
# We will not use this table for subsequent examples,
# but given how we create these tables it whould be 
# 100% identical in content to the others

print(f"Using {partitions} partitions")

dbutils.fs.rm(tb_parquet_path, True)

df = (spark
  .range(0, record_count)
  .withColumnRenamed("id", "long_value")
  .withColumn("hash_value", sha2(col("long_value").cast("string"), 256))
  #.repartition(partitions)
)
df.write.format("parquet").mode("overwrite").save(tb_parquet_path)

print(len(dbutils.fs.ls(tb_parquet_path)))

# COMMAND ----------

# DBTITLE 1,TB Parquet Stats
total = __builtin__.sum(list(map(lambda f: f.size, dbutils.fs.ls(tb_parquet_path))))/1024/1024/1024/1024
records = spark.read.format("parquet").load(tb_parquet_path).count()

print(f"{total:0.2f} GB")
print(f"{records:,d}")

# COMMAND ----------

# DBTITLE 1,Create Utility Method
spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy")
spark.sql(f"USE dbacademy")

def write_table(table_name, dest_path, zorder_column, bloom_column):
  spark.sql(f"DROP table IF EXISTS {table_name}")
  dbutils.fs.rm(dest_path, True)

  spark.sql(f"""CREATE TABLE {table_name} (`long_value` LONG, `hash_value` STRING) USING delta LOCATION '{dest_path}' """)
  
  if bloom_column:
    spark.sql(f"CREATE BLOOMFILTER INDEX ON TABLE {table_name} FOR COLUMNS({bloom_column} OPTIONS (fpp=0.1, numItems={record_count}))")

  (spark.read.parquet(tb_parquet_path)
    .repartition(partitions)
    .write.mode("append")
    .format("delta")
    .saveAsTable(table_name))
        
  if zorder_column:
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY {zorder_column}")
  else:
    spark.sql(f"OPTIMIZE {table_name}")
  
  spark.sql(f"VACUUM {table_name} RETAIN 0 HOURS")

# COMMAND ----------

# DBTITLE 1,TB Delta, Indexes
dest_path = f"{base_path}/1tb-zordered-long.delta"
write_table("1tb_delta", dest_path, zorder_column="long_value", bloom_column=None)

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("long_value == '2297823720'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Z-Ordered, Long_Value
dest_path = f"{base_path}/1tb-zordered-long.delta"
write_table("1tb_zordered_long", dest_path, zorder_column="long_value", bloom_column=None)

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("long_value == '2297823720'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Z-Ordered, Hash_Value
dest_path = f"{base_path}/1tb-zordered-hash.delta"
write_table("1tb_zordered_hash", dest_path, zorder_column="hash_value", bloom_column=None)

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("hash_value == 'f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Z-Ordered, Bloom Filter, Long_Value
dest_path = f"{base_path}/1tb-zo-bloom-long.delta"
write_table("1tb_zo_bloom_long", dest_path, zorder_column="long_value", bloom_column="long_value")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("long_value == '2297823720'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Z-Ordered, Bloom Filter, Hash_Value
dest_path = f"{base_path}/1tb-zo-bloom-hash.delta"
write_table("1tb_zo_bloom_hash", dest_path, zorder_column="hash_value", bloom_column="hash_value")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("hash_value == 'f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Bloom Filter Only, Long_Value
dest_path = f"{base_path}/1tb-bloom-long.delta"
write_table("1tb_bloom_long", dest_path, zorder_column=None, bloom_column="long_value")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("long_value == '2297823720'")
      .write.format("noop").mode("overwrite").save())

# COMMAND ----------

# DBTITLE 1,TB Bloom Filter Only, Hash_Value
dest_path = f"{base_path}/1tb-bloom-hash.delta"
write_table("1tb_bloom_hash", dest_path, zorder_column=None, bloom_column="hash_value")

# COMMAND ----------

(spark.read.format("delta").load(dest_path)
      .filter("hash_value == 'f52789d84652e7ed4608c48f52b3dfaa1f6aa310249714af71ed48b98f14eca2'")
      .write.format("noop").mode("overwrite").save())
