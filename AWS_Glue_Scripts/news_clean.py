import sys
import boto3
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from AWS Glue Data Catalog
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="covid_sentiment_kafka",
    table_name="vishnu_news_bucket",
    transformation_ctx="DataSource0"
)

# Apply mappings
mapped_frame = ApplyMapping.apply(
    frame=dynamic_frame,
    mappings=[
        ("description", "string", "public_post_comments", "string"),
        ("url", "string", "url", "string"),
        ("publishedat", "string", "published_date", "string")
    ],
    transformation_ctx="Transform0"
)

# Convert to DataFrame and coalesce into a single partition
dataframe = mapped_frame.toDF()

# Select columns in the desired order
ordered_dataframe = dataframe.select("url", "public_post_comments", "published_date")

# Coalesce into a single partition for a single output file
coalesced_dataframe = ordered_dataframe.coalesce(1)

# Define temporary output path
temp_output_path = "s3://combined-source/temp_output"

# Write the DataFrame to the temporary location
coalesced_dataframe.write.mode("overwrite").option("header", "true").csv(temp_output_path)

# Define the final output file path
final_output_path = "s3://combined-source/clean_news_data.csv"

# Create an S3 client
s3 = boto3.client('s3')

# Delete existing files in the final output path
bucket_name, prefix = final_output_path.replace('s3://', '').split('/', 1)
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
if 'Contents' in response:
    object_keys = [content['Key'] for content in response['Contents']]
    s3.delete_objects(Bucket=bucket_name, Delete={'Objects': [{'Key': key} for key in object_keys]})

# Move and rename the file from the temporary location to the final output path
temp_bucket_name, temp_prefix = temp_output_path.replace('s3://', '').split('/', 1)
response = s3.list_objects_v2(Bucket=temp_bucket_name, Prefix=temp_prefix)
if 'Contents' in response:
    for content in response['Contents']:
        temp_key = content['Key']
        if temp_key.endswith('.csv'):
            copy_source = {'Bucket': temp_bucket_name, 'Key': temp_key}
            s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=prefix)
            s3.delete_object(Bucket=temp_bucket_name, Key=temp_key)

job.commit()
