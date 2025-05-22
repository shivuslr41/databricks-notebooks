# Databricks notebook source
print("Hello from Git Notebook")

# COMMAND ----------

data = spark.sql("""select * from system.billing.usage limit 10""")
display(data)
