import json
import pandas as pd
import boto3
from io import StringIO

# Initialize the SageMaker runtime client
client = boto3.client('sagemaker-runtime', region_name='ap-southeast-1')

# Specify your endpoint name
endpoint_name = "bert-sentiment-endpoint2024-06-09-03-31-31"

# S3 path to the CSV file
s3_path = "s3://combined-clean/clean_output_file.csv"

# Read the CSV file from S3 into a pandas DataFrame
s3 = boto3.client('s3')
bucket, key = s3_path.replace("s3://", "").split('/', 1)
obj = s3.get_object(Bucket=bucket, Key=key)
data = pd.read_csv(obj['Body'])

# Send the S3 path to the SageMaker endpoint
response = client.invoke_endpoint(
    EndpointName=endpoint_name,
    ContentType='text/plain',
    Body=s3_path.encode('utf-8')
)

# Get the predicted sentiment labels from the response
predicted_sentiments = response['Body'].read().decode('utf-8').splitlines()

# Convert the single string of predicted sentiments into a list of individual sentiments
predicted_sentiments = json.loads(predicted_sentiments[0])

# Ensure the number of sentiments matches the number of rows in your DataFrame
if len(predicted_sentiments) == len(data):
    # Find the index of 'public_post_comments' column to insert the new column right next to it
    index_of_comments = data.columns.get_loc('public_post_comments') + 1
    data.insert(index_of_comments, 'Sentiments', predicted_sentiments)

    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    # Define the S3 path for the output CSV
    output_bucket = 'output-clean'  # Replace with your bucket name if different
    output_key = 'output_with_sentiments.csv'

    # Upload the CSV file to S3
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())
    print("File uploaded successfully to S3 bucket:", output_bucket)
else:
    print("Mismatch in the number of sentiments and the number of DataFrame rows.")
