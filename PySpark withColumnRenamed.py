# Databricks notebook source
# MAGIC %md
# MAGIC **Autor**   : icaro.martins( icaro.martins@blueshift.com) \
# MAGIC **Data**    : 2022-11-25\
# MAGIC **Versão**  : v1 \
# MAGIC **Proposta**: PySpark withColumnRenamed para renomear a coluna no DataFrame \
# MAGIC **Repositório oficial**:\
# MAGIC **link**: https://sparkbyexamples.com/pyspark/pyspark-rename-dataframe-column/ \
# MAGIC https://stackoverflow.com/questions/38798567/pyspark-rename-more-than-one-column-using-withcolumnrenamed \
# MAGIC https://www.geeksforgeeks.org/how-to-rename-multiple-columns-in-pyspark-dataframe/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setando Dados

# COMMAND ----------

dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando a Estrutura

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

schema = StructType([StructField('Name', StructType([\
                                           StructField('Fistname', StringType(), True),\
                                           StructField('MiddleName', StringType(), True),\
                                           StructField('LastName', StringType(), True)
                                                    ])),\
           StructField('Dob', StringType(), True),\
           StructField('Gender', StringType(), True),\
           StructField('Salary', IntegerType(), True)
          ])

print(schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando DataFrame

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(dataDF, schema)

df.show()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. PySpark withColumnRenamed - Para renomear o nome da coluna DataFrame
# MAGIC
# MAGIC
# MAGIC * Sintaxe: withColumnRenamed(existingName, newNam)
# MAGIC     * existingName– O nome da coluna existente que você deseja alterar
# MAGIC     * newName– Novo nome da coluna
# MAGIC     

# COMMAND ----------

df.withColumnRenamed('dob','DateOfBirth').printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. PySpark withColumnRenamed – Para renomear várias colunas

# COMMAND ----------

df2 = df.withColumnRenamed('Dob','DateOfBirth') \
        .withColumnRenamed('Salary','Salary_Amount')

df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1 Renomar mais de uma coluna com to.DF()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2.1.1 Criando lista com colunas novas

# COMMAND ----------

columns_news = ['Name_new','Date_new','Gerder_new','Salary_new']

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2.1.2 Renomeando de uma vez todo o DataFrame

# COMMAND ----------

df3 = df2.toDF(*columns_news)
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2.1. Renomeando usando SELECT/ list comprehension

# COMMAND ----------

from pyspark.sql.functions import col

mapping = dict(zip(['Name','Gender'],['Name_new', 'Gender_new']))
df4 = df2.select([col(c).alias(mapping.get(c,c)) for c in df2.columns])

df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Usando PySpark StructType – Para renomear uma coluna aninhada no Dataframe

# COMMAND ----------

