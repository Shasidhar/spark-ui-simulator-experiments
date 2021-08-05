# Databricks notebook source


# COMMAND ----------

# MAGIC %fs ls /mnt/dbacademy/spark-ui-simulator-rw/global-sales/transactions/

# COMMAND ----------

try: dbutils.fs.unmount(mount_point)
except: print(f"{mount_point} is not mounted...")
