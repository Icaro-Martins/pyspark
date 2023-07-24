# Databricks notebook source
# MAGIC %md
# MAGIC **Autor**   : icaro.martins( icaro.martins@blueshift.com) \
# MAGIC **Data**    : 2022-11-21 \
# MAGIC **Versão**  : v1 \
# MAGIC **Proposta**: PySpark withColumn () \
# MAGIC **Repositório oficial**:\
# MAGIC **link**: https://sparkbyexamples.com/pyspark/pyspark-withcolumn/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando um DataFrame para trabalhar.

# COMMAND ----------

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

df = spark.createDataFrame(data,columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Alterando DataType usando PySpark withColumn()

# COMMAND ----------

df.withColumn('salary', df['salary'].cast('Integer')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Atualize o valor de uma coluna existente

# COMMAND ----------

df.withColumn('salary',df['salary']*100).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Crie uma coluna a partir de uma existente

# COMMAND ----------

df.withColumn('salary',df['salary'] * -1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Adicione uma nova coluna usando withColumn()

# COMMAND ----------

from pyspark.sql.functions import lit
df.withColumn('country', lit('USA')).show()
df.withColumn('country', lit('USA'))\
  .withColumn('anotherColumn', lit('anotherColumn')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Renomeie o nome da coluna

# COMMAND ----------

df.withColumnRenamed('gender','sex').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Solte a coluna do PySpark DataFrame
# MAGIC

# COMMAND ----------

df.drop('salary').show()