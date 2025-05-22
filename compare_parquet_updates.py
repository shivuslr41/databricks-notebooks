# Databricks notebook source
import boto3

# Grab params file
params_s3_uri = dbutils.widgets.get("params_s3_uri")
if params_s3_uri == "":
    raise ValueError("params_s3_uri is not set. Please set a value.")

params_schema = StructType([
    StructField("org_id", IntegerType(), True),
    StructField("s3_files", ArrayType(StringType(), True), True),
    StructField("full_table_name", StringType(), True),
    StructField("dimensions", ArrayType(StringType(), True), True),
    StructField("metrics", ArrayType(StringType(), True), True),
    StructField("save_convergence_file", BooleanType(), True),
])

params_df = spark.read.schema(params_schema).json(params_s3_uri, mode="FailFast")
params = params_df.head()
if not params.save_convergence_file:
    dbutils.notebook.exit(json.dumps({"convergence_s3_uri": ""}))
    
destination_s3_bucket = dbutils.widgets.get("destination_s3_bucket")

s3 = boto3.resource('s3')
bucket = s3.Bucket(destination_s3_bucket)

_, schema, _ = params.full_table_name.split(".")
# list files in the path from the bucket
bucket_files = [file for file in bucket.objects.filter(Prefix=schema)]
if len(bucket_files) < 2:
    dbutils.notebook.exit(f"Only {len(bucket_files)} files found in the bucket for {schema}. Expected at least 2 files.")

# pick latest dated two files from bucket files
latest_files = sorted(bucket_files, key=lambda x: x.last_modified, reverse=True)[:2]
print(f"Using {latest_files[0].key} and {latest_files[1].key}")

# COMMAND ----------

# Load the two latest parquet files into DataFrames
df1 = spark.read.parquet(f"s3://{destination_s3_bucket}/{latest_files[0].key}")
df2 = spark.read.parquet(f"s3://{destination_s3_bucket}/{latest_files[1].key}")

# Get the schema of both DataFrames
schema1 = df1.schema
schema2 = df2.schema

# Compare column names and data types
columns1 = set((field.name, field.dataType) for field in schema1)
columns2 = set((field.name, field.dataType) for field in schema2)

# Find differences
diff1 = columns1 - columns2
diff2 = columns2 - columns1

# Display differences
if not diff1 and not diff2:
    print("The column names and data types are identical in both files.")
else:
    if diff1:
        print("Columns in the new file but not in the old file:")
        for col in diff1:
            print(col)
    if diff2:
        print("Columns in the old file but not in the new file:")
        for col in diff2:
            print(col)

# COMMAND ----------

import time

if dbutils.widgets.get("compare_data") == "true":
    # Get the date 31 days before the current date
    date_threshold = time.strftime("%Y-%m-%d", time.gmtime(time.time() - 31 * 24 * 60 * 60))
    print(f"Comparing data until {date_threshold} to avoid false positives.")

    # Compare the data in the two files
    data = spark.sql(f"""
    -- Select 1000 random distinct rows from the older file
    WITH random_rows AS (
    SELECT distinct timestamp, resource_uid, usage_amount, au_list_price, au_effective_cost, au_net_effective_cost
    FROM PARQUET.`s3://{destination_s3_bucket}/{latest_files[1].key}`
    WHERE resource_uid IS NOT NULL AND resource_uid <> '' AND billing_account <> '' AND timestamp < timestamp({date_threshold})
    ORDER BY RAND() 
    LIMIT 1000
    ),
    -- Select corresponding rows from the latest file
    sample_a AS (
        SELECT * FROM PARQUET.`s3://{destination_s3_bucket}/{latest_files[0].key}`
        WHERE (timestamp, resource_uid, usage_amount, au_list_price, au_effective_cost, au_net_effective_cost) IN (SELECT timestamp, resource_uid, usage_amount, au_list_price, au_effective_cost, au_net_effective_cost FROM random_rows)
    ),
    -- Select corresponding rows from the older file
    sample_b AS (
        SELECT * FROM PARQUET.`s3://{destination_s3_bucket}/{latest_files[1].key}`
        WHERE (timestamp, resource_uid, usage_amount, au_list_price, au_effective_cost, au_net_effective_cost) IN (SELECT timestamp, resource_uid, usage_amount, au_list_price, au_effective_cost, au_net_effective_cost FROM random_rows)
    )
    -- Find differences between the two samples
    (SELECT * FROM sample_a EXCEPT SELECT * FROM sample_b)
    UNION ALL
    (SELECT * FROM sample_b EXCEPT SELECT * FROM sample_a)
    """)

    display(data)
