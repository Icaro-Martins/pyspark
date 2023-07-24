# Databricks notebook source
# MAGIC %md
# MAGIC **Autor**   : icaro.martins( icaro.martins@blueshift.com) \
# MAGIC **Data**    : 2022-12-01 \
# MAGIC **Versão**  : v1 \
# MAGIC **Proposta**: PySpark Where Filter Function | Multiple Conditions \
# MAGIC **Repositório oficial**:\
# MAGIC **link**: https://sparkbyexamples.com/pyspark/pyspark-where-filter/

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. PySpark DataFrame filter() Syntax

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]


schema = StructType([
              StructField('name',StructType([
                StructField("fistname", StringType(), True),
                StructField("middlename", StringType(), True),
                StructField("lastname", StringType(), True)
              ])),
             StructField('laguenge', ArrayType(StringType()), True),
             StructField('state', StringType(), True),
             StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. DataFrame filter() with Column Condition

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.1 Using equals condition

# COMMAND ----------

df.filter(df.state == "OH").show()
df.filter(df.state != "OH").show()
df.filter(~(df.state == "OH")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.2 Using SQL col() function

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col('state') == "OH").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. DataFrame filter() with SQL Expression

# COMMAND ----------

df.filter('gender == "M"').show()
df.filter('gender != "M"').show()
df.filter('gender <> "m"').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. PySpark Filter with Multiple Conditions

# COMMAND ----------

df.filter((df.state == "OH") & (df.gender == "M")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Filter Based on List Values

# COMMAND ----------

li =  ["OH","CA","DE"]
df.filter(df.state.isin(li)).show()
df.filter(~df.state.isin(li)).show()
df.filter(df.state.isin(li)==False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 6. Filtro com base em começa com, termina com, contém
# MAGIC

# COMMAND ----------

df.filter(df.state.startswith("N")).show()
df.filter(df.state.endswith("H")).show()
df.filter(df.state.contains("H")).show()