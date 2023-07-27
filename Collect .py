# Databricks notebook source
# MAGIC %md
# MAGIC **Autor**   : icaro.martins( icaro.martins@blueshift.com) \
# MAGIC **Data**    : 2022-11-21 \
# MAGIC **Versão**  : v1 \
# MAGIC **Proposta**: PySpark Collect () - Recupere dados do DataFrame \
# MAGIC **Repositório oficial**:\
# MAGIC **link**: https://sparkbyexamples.com/pyspark/pyspark-collect/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create Data Frame

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

dept = [("Finance",10), \
        ("Marketing",20), \
        ("Sales",30), \
        ("IT",40) \
       ]
deptColumns = ["dept_name","dept_id"]


df = spark.createDataFrame(data = dept, schema = deptColumns)

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.1 Usando o collect() para recuperar dados

# COMMAND ----------

dataCollect = df.collect()
print(dataCollect)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2 Prodessando dados com for

# COMMAND ----------

for row in dataCollect:
  print(row['dept_name'] + "," + str(row['dept_id']))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.3 Obtendo dados da coleção

# COMMAND ----------

df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.4 Retornar alguns elementos de um DataFrame

# COMMAND ----------

dataCole = df.select('dept_name').collect()
display(dataCole)