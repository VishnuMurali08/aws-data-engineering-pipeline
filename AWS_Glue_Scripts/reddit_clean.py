import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from AWS Glue Data Catalog
dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="covid_sentiment_kafka",
    table_name="vishnu_reddit_bucket",
    transformation_ctx="AWSGlueDataCatalog_node1715243894175"
)

# Apply mappings to change schema
mapped_frame = ApplyMapping.apply(
    frame=dynamic_frame,
    mappings=[
        ("url", "string", "url", "string"),
        ("title", "string", "public_post_comments", "string"),
        ("time", "string", "published_date", "string")
        
    ],
    transformation_ctx="ChangeSchema_node1715171967694"
)

# Convert DynamicFrame to DataFrame
dataframe = mapped_frame.toDF()

# Coalesce into a single partition for a single output file
coalesced_dataframe = dataframe.coalesce(1)

# Convert back to DynamicFrame
dynamic_frame_single = DynamicFrame.fromDF(coalesced_dataframe, glueContext, "dynamic_frame_coalesced")

# Define the temporary output path
temp_output_path = "s3://combined-source/temp_output"

# Write the DataFrame to the temporary location
dynamic_frame_single.toDF().write.mode("overwrite").option("header", "true").csv(temp_output_path)

# Create an S3 client
import boto3
s3 = boto3.client('s3')

# Define the final output file path
final_output_path = "s3://combined-source/clean_reddit_data.csv"

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
