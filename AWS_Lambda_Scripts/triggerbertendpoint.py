import json
import pandas as pd
import boto3
from io import StringIO

# Initialize SageMaker runtime client
sagemaker_runtime_client = boto3.client('sagemaker-runtime', region_name='ap-southeast-1')

# Specify your endpoint name
endpoint_name = "bert-sentiment-endpoint2024-06-09-03-31-31"

# S3 bucket and key for input file
input_bucket = "combined-clean"
input_key = "clean_output_file.csv"

# S3 bucket and key for output file
output_bucket = "output-clean"
output_key = "output_with_sentiments.csv"

def lambda_handler(event, context):
    # Get the S3 object key from the event
    input_key = event['Records'][0]['s3']['object']['key']
    
    # Check if the event is for the input file
    if input_key == "clean_output_file.csv":
        # Read the CSV file from S3 into a pandas DataFrame
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=input_bucket, Key=input_key)
        data = pd.read_csv(obj['Body'])
        
        # Send the S3 path to the SageMaker endpoint
        response = sagemaker_runtime_client.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType='text/plain',
            Body=f"s3://{input_bucket}/{input_key}".encode('utf-8')
        )
        
        # Get the predicted sentiment labels from the response
        predicted_sentiments = response['Body'].read().decode('utf-8').splitlines()
        predicted_sentiments = json.loads(predicted_sentiments[0])
        
        # Ensure the number of sentiments matches the number of rows in your DataFrame
        if len(predicted_sentiments) == len(data):
            # Find the index of 'public_post_comments' column to insert the new column right next to it
            index_of_comments = data.columns.get_loc('public_post_comments') + 1
            data.insert(index_of_comments, 'Sentiments', predicted_sentiments)
            
            # Convert DataFrame to CSV
            csv_buffer = StringIO()
            data.to_csv(csv_buffer, index=False)
            
            # Upload the CSV file to S3
            s3.put_object(Bucket=output_bucket, Key=output_key, Body=csv_buffer.getvalue())
            print("File uploaded successfully to S3 bucket:", output_bucket)
        else:
            print("Mismatch in the number of sentiments and the number of DataFrame rows.")
    else:
        print("Event is not for the input file. Skipping.")

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda function executed successfully.')
    }