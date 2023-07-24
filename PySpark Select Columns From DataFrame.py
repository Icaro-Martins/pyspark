# Databricks notebook source
# MAGIC %md
# MAGIC **Autor**   : icaro.martins( icaro.martins@blueshift.com) \
# MAGIC **Data**    : 2022-11-18 \
# MAGIC **Versão**  : v1 \
# MAGIC **Proposta**: PySpark Select Columns From DataFrame \
# MAGIC **Repositório oficial**:\
# MAGIC **link**: https://sparkbyexamples.com/pyspark/select-columns-from-pyspark-dataframe/

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James","Smith","USA","CA"),
        ("Michael","Rose","USA","NY"),
        ("Robert","Williams","USA","CA"),
        ("Maria","Jones","USA","FL")
       ]

columns = ["firstname","lastname","country","state"]

df1 = spark.createDataFrame(data=data, schema=columns)

display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Selecione colunas únicas e múltiplas do PySpark

# COMMAND ----------

df1.select('firstname','lastname').show()
df1.select(df1.firstname,df1.lastname).show()
df1.select(df1['firstname'],df1['lastname']).show()

#By using col() function
from pyspark.sql.functions import col
df1.select(col('firstname'),col('lastname')).show()

#Select columns by regular expression
df1.select(df1.colRegex('`^.*name*`')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Selecione todas as colunas da lista

# COMMAND ----------

# Select All columns from List
df1.select(*columns).show()

# Select All columns
df1.select([col for col in df1.columns]).show()
df1.select('*').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Selecione Colunas por Índice
# MAGIC   Usando os recursos de lista do python, você pode selecionar as colunas por índice.

# COMMAND ----------

df1.select(df1.columns[:3]).show()
df1.select(df1.columns[1:3]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Selecione colunas de estruturas aninhadas do PySpark
# MAGIC
# MAGIC Se você tiver uma coluna struct aninhada (StructType) no PySpark DataFrame, precisará usar um qualificador de coluna explícito para selecionar.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType


data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

schema = StructType([\
                    StructField('name', StructType([\
                                                   StructField('firstname',StringType(),True),
                                                   StructField('middlename',StringType(),True),
                                                   StructField('lastname', StringType(),True),
                                                   ])),
                     StructField('state',StringType(),True),\
                     StructField('gerder',StringType(),True)
                    ])
df = spark.createDataFrame(data=data,schema=schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4.1 Column name é um structtipo que consiste em colunas firstname, middlename, lastname.

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4.2 Agora, vamos selecionar a coluna struct.

# COMMAND ----------

display(df.select('name'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4.3 Qualifique explicitamente o nome da coluna da estrutura aninhada

# COMMAND ----------

display(df.select('name.firstname','name.lastname'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4.4  Obter todas as colunas do struct

# COMMAND ----------

display(df.select('name.*'))