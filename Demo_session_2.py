# Databricks notebook source
container=dbutils.jobs.taskValues.get(taskKey = "task1", key = "container", default = None)
inputFilePath = "dbfs:/mnt/{}/{}".format(container, "demo_data_process.csv")
df = spark.read.format("csv").load(inputFilePath, header=True, inferSchema=True)
display(df)

# COMMAND ----------

from pyspark.sql import functions as F
yearly_rate_sum = df.groupBy("year").agg(F.sum("Rate").alias("sum_rate"))

# COMMAND ----------

display(yearly_rate_sum)
