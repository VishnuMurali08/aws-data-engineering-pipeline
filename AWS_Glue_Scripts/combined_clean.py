import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, date_format
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def format_dates(df, date_column_name, target_format):
    return df.withColumn(date_column_name, date_format(col(date_column_name), target_format))

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Retrieve all CSV files from the S3 path dynamically
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://combined-source/"], "recurse": True},
    transformation_ctx="dynamic_frame"
)

# Check if the DynamicFrame is empty
if dynamic_frame.count() == 0:
    print("DynamicFrame is empty. Exiting without processing.")
else:
    print("DynamicFrame is not empty. Proceeding with processing.")

    # Convert DynamicFrame to DataFrame to use Spark functions
    df = dynamic_frame.toDF()

    # Check if the DataFrame is empty
    if df.rdd.isEmpty():
        print("DataFrame is empty after conversion from DynamicFrame. Exiting without writing to S3.")
    else:
        print("DataFrame is not empty. Proceeding with processing.")

        # Check if 'published_date' column exists and format the date if it does
        if 'published_date' in df.columns:
            print("Column 'published_date' found. Formatting dates.")
            df = format_dates(df, 'published_date', 'yyyy/MM/dd')
        else:
            print("Column 'published_date' does not exist. Continuing without date formatting.")

        # Coalesce to reduce number of files, adjust the number as necessary
        coalesced_df = df.coalesce(1)  # Using 1 to minimize file count

        # Write the DataFrame to a temporary location
        temp_output_path = "s3://combined-clean/temp_output"
        coalesced_df.write.mode("overwrite").option("header", "true").csv(temp_output_path)

        # Create an S3 client
        s3_client = boto3.client('s3')

        # Define the final output file path
        final_output_path = "s3://combined-clean/clean_output_file.csv"

        # Get the temporary file key
        temp_bucket_name, temp_prefix = temp_output_path.replace('s3://', '').split('/', 1)
        response = s3_client.list_objects_v2(Bucket=temp_bucket_name, Prefix=temp_prefix)
        temp_file_key = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')][0]

        # Copy the temporary file to the final output path
        copy_source = {'Bucket': temp_bucket_name, 'Key': temp_file_key}
        final_bucket_name, final_prefix = final_output_path.replace('s3://', '').split('/', 1)
        s3_client.copy_object(CopySource=copy_source, Bucket=final_bucket_name, Key=final_prefix)

        # Delete the temporary files
        for obj in response.get('Contents', []):
            s3_client.delete_object(Bucket=temp_bucket_name, Key=obj['Key'])

job.commit()
