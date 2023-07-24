# Databricks notebook source
# MAGIC %md
# MAGIC **Autor**   : icaro.martins( icaro.martins@blueshift.com) \
# MAGIC **Data**    : 2022-11-10 \
# MAGIC **Versão**  : v1 \
# MAGIC **Proposta**: PySpark Row usando DataFrame e RDD\
# MAGIC **Repositório oficial**:
# MAGIC **link**: https://sparkbyexamples.com/pyspark/pyspark-row-using-rdd-dataframe/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Criando um objeto de linha

# COMMAND ----------

from pyspark.sql import Row

row = Row("James",40,"Icaro")
print(row[0] + "," + str(row[2]))

# COMMAND ----------

row = Row(name="Icaro", age=11,garden='F')
print(row.garden,row.name, row.age)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Crie uma classe personalizada a partir da linha
# MAGIC
# MAGIC * podemos criar uma classe do tipo Row
# MAGIC

# COMMAND ----------

Person = Row("name","age")

p1=Person("James",40)
p2=Person("Alice", 35)

print(p1.name + ", " + p2.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Usando a classe Row no PySpark RDD

# COMMAND ----------

from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName('SparkBy.Examples.com').getOrCreate()

data = [Row(name="James,,Smith",lang=["Java","Scala","C++"],state="CA"),
        Row(name="Michael,Rose,",lang=["Spark","Java","C++"],state="NJ"),
        Row(name="Robert,,Williams",lang=["CSharp","VB"],state="NV")
       ]

rdd = spark.sparkContext.parallelize(data)
display(rdd.collect())

# COMMAND ----------

collData = rdd.collect()

for row in collData:
  print(row.name + ", " + str(row.lang))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Usando a classe Row no PySpark DataFrame

# COMMAND ----------

df = spark.createDataFrame(data)
df.printSchema
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 4.1 Você também pode alterar os nomes das colunas usando a toDF()função

# COMMAND ----------

columns = ["name","languagesAtSchool","currentState"]

df = spark.createDataFrame(data).toDF(*columns)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Crie uma estrutura aninhada usando a classe de linha

# COMMAND ----------

data=[Row(name="James",prop=Row(hair="black",eye="blue")),
      Row(name="Ann",prop=Row(hair="grey",eye="black"))]
df=spark.createDataFrame(data)
display(df)

# COMMAND ----------

