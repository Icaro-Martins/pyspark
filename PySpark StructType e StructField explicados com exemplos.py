# Databricks notebook source
# MAGIC %md
# MAGIC **Autor**   : icaro.martins( icaro.martins@blueshift.com) \
# MAGIC **Data**    : 2022-11-10 \
# MAGIC **Versão**  : v1 \
# MAGIC **Proposta**: PySpark StructType e StructField explicados com exemplos.\
# MAGIC **Repositório oficial**:
# MAGIC **link**: https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. StructType – Define a estrutura do Dataframe
# MAGIC   * O PySpark fornece desde pyspark.sql.types import StructType a classe para definir a estrutura do DataFrame.\
# MAGIC   * StructType é uma coleção ou lista de objetos StructField.\
# MAGIC   * O método printSchema() do PySpark no DataFrame mostra as colunas StructType como struct.

# COMMAND ----------

from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. StructField – Define os metadados da coluna DataFrame
# MAGIC
# MAGIC PySpark fornece pyspark.sql.types import StructFieldclasse para definir as colunas que incluem nome da coluna (String), tipo de coluna ( DataType ), coluna anulável (Boolean) e metadados (MetaData)
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Usando PySpark StructType e StructField com DataFrame

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()

data = [
    ("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)  
]

schema = StructType([
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Definindo a estrutura do objeto StructType aninhado

# COMMAND ----------

structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]


structureSchema = StructType([
    StructField('Name', StructType([
      StructField('firstname',StringType(),True),
      StructField('middlename',StringType(),True),
      StructField('lastname',StringType(),True)
    ])),
   StructField('id',StringType(),True),
   StructField('gender',StringType(),True),
   StructField('salary',IntegerType(),True)  
])

df2 = spark.createDataFrame(data=structureData, schema=structureSchema)

df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Adicionando e alterando a estrutura do DataFrame

# COMMAND ----------

 from pyspark.sql.functions import col, struct, when

 updatedDF = df2.withColumn("OtherInfo",
                           struct(col('id').alias('identifier'),
                                  col('gender').alias('gender'),
                                  col('salary').alias('salary'),
                                  when(col('salary').cast(IntegerType()) < 2000,'Low')
                                 .when(col('salary').cast(IntegerType()) < 4000,'Medium')
                                 .otherwise('High').alias('Salary_Grade'))).drop('id','gender','salary')

updatedDF.printSchema()
updatedDF.show(truncate=False)
display(updatedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Usando SQL ArrayType e MapType
# MAGIC

# COMMAND ----------

from pyspark.sql.types import ArrayType, MapType

arrayStructureSchema = StructType([
       StructField('name', StructType([
                               StructField('firstname', StringType(), True),
                               StructField('middlename', StringType(), True),
                               StructField('lastname', StringType(), True)
       ])),
       StructField('hobbies', ArrayType(StringType()), True),
       StructField('properties', MapType(StringType(), StringType()), True) 
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Criando a estrutura do objeto StructType a partir do arquivo JSON
# MAGIC
# MAGIC Se você tiver muitas colunas e a estrutura do DataFrame mudar de vez em quando, é uma boa prática carregar o esquema SQL StructType do arquivo JSON. Você pode obter o esquema usando df2.schema.json(), armazená-lo em um arquivo e usá-lo para criar um esquema a partir desse arquivo.

# COMMAND ----------

print(df2.schema.json())

# COMMAND ----------


{
  "type" : "struct",
  "fields" : [ {
    "name" : "name",
    "type" : {
      "type" : "struct",
      "fields" : [ {
        "name" : "firstname",
        "type" : "string",
        "nullable" : True,
        "metadata" : { }
      }, {
        "name" : "middlename",
        "type" : "string",
        "nullable" : True,
        "metadata" : { }
      }, {
        "name" : "lastname",
        "type" : "string",
        "nullable" : True,
        "metadata" : { }
      } ]
    },
    "nullable" : True,
    "metadata" : { }
  }, {
    "name" : "dob",
    "type" : "string",
    "nullable" : True,
    "metadata" : { }
  }, {
    "name" : "gender",
    "type" : "string",
    "nullable" : True,
    "metadata" : { }
  }, {
    "name" : "salary",
    "type" : "integer",
    "nullable" : True,
    "metadata" : { }
  } ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 7.1 - Como alternativa, você também pode usar  df.schema.simpleString(), isso retornará um formato de esquema relativamente mais simples.

# COMMAND ----------

df.schema.simpleString()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 7.2 Agora vamos carregar o arquivo json e usá-lo para criar um DataFrame.

# COMMAND ----------

import json
schemaFromJson = StructType.fromJson(json.loads(schema.json))
df3 = spark.createDataFrame(
        spark.sparkContext.parallelize(structureData),schemaFromJson)
df3.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Criando uma estrutura de objeto StructType a partir de DDL String

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Verificando se existe uma coluna em um DataFrame

# COMMAND ----------

print(df2.schema.fieldNames.contains("firstname"))
print(df2.schema.contains(StructField("firstname",StringType,true)))