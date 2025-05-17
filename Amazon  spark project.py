# Databricks notebook source
# Importing required libraries

from pyspark.sql import SparkSession 
from pyspark.sql.functions import avg, count, desc, length, when, col

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/pratikshanishad739@gmail.com/')

# COMMAND ----------

# Creating DF1 by Amazon.csv

df = spark.read.format('csv').option('header', 'true').load('dbfs:/FileStore/shared_uploads/pratikshanishad739@gmail.com/amazon.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select('product_name').display()

# COMMAND ----------

# Top products by rating on Amazon

top_rated_producted = df.groupBy('product_id', 'product_name'). agg(avg('rating').alias('avg_rating')).orderBy(desc('avg_rating')).limit(10).display()

# COMMAND ----------

# Most reviewed product

most_reviewed_product = df.groupBy('product_id', 'product_name').count().orderBy(desc('count')).limit(10).display()

# COMMAND ----------

discount_analysis = df.groupBy('category').agg(avg('discount_percentage').alias('avg_discount')).display()

# COMMAND ----------

# User engagement

user_engagement = df.groupBy('product_id').agg(avg('rating').alias('avg_rating'), count('rating').alias('rating_count')).display()

# COMMAND ----------

# Creating temp table from df

df.createOrReplaceTempView('amazon_sales_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amazon_sales_table order by product_id desc limit 10

# COMMAND ----------

