# Databricks notebook source
# MAGIC %md
# MAGIC **Autor**   : icaro.martins( icaro.martins@blueshift.com) \
# MAGIC **Data**    : 2022-11-10 \
# MAGIC **Versão**  : v1 \
# MAGIC **Proposta**: Estudo sobre: PySpark show() – Exibe o conteúdo do DataFrame na tabela.\
# MAGIC **Repositório oficial**:
# MAGIC **link**: https://sparkbyexamples.com/pyspark/pyspark-show-display-dataframe-contents-in-table/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 - Exemplo rápido de show()

# COMMAND ----------

# Default - Mostra 20 linhas e 20 caracteres
df.show()

#Mostra tudo o que a coluna contem
df.show(truncate=False)

# Mostra 2 linhas e tudo o que a coluina contem
df.show(2,truncate=False) 

# Mostra duas colunas e 25 caracteres da coluna
df.show(2,truncate=25) 

# Mostra linhas do datframe & colunas na vertical
df.show(n=3,truncate=25,vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 - Show() Sintaxe
# MAGIC * A seguir está a sintaxe da função show().

# COMMAND ----------

def show(self, n=20, trucate=25, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3 - PySpark show() para exibir o conteúdo

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
columns = ["Seqno","Quote"]
data = [("1", "Be the change that you wish to see in the world"),
        ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
        ("3", "The purpose of our lives is to be happy."),
        ("4", "Be cool.")]

df = spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 - Show() com valores de coluna truncados

# COMMAND ----------

df.show(2,truncate=False)
df.show(2,truncate=25)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5 - Exiba o conteúdo verticalmente
# MAGIC

# COMMAND ----------

df.show(n=3,truncate=False,vertical=True)