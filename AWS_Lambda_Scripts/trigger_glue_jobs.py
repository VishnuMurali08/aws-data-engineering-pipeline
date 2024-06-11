import boto3
import json

# Initialize the AWS Glue client and Lambda client
glue_client = boto3.client('glue', region_name='ap-southeast-1')
lambda_client = boto3.client('lambda', region_name='ap-southeast-1')

def start_job(job_name):
    """Starts an AWS Glue job and returns the JobRunId."""
    response = glue_client.start_job_run(JobName=job_name)
    print(f"Started {job_name} with JobRunId:", response['JobRunId'])
    return response['JobRunId']

def lambda_handler(event, context):
    """Handles the Lambda event to manage AWS Glue jobs."""
    # Log the received event
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Check if the event is a continuation event with job_index
        if 'job_index' in event:
            job_index = event['job_index']
            jobs = event['jobs']
            print(f"Continuing with job: {jobs[job_index]}")
            
            # Start the job
            job_run_id = start_job(jobs[job_index])
            print(f"Job {jobs[job_index]} started successfully with JobRunId: {job_run_id}")
            
            # If there are more jobs in the sequence, invoke the next one
            if job_index + 1 < len(jobs):
                next_job_index = job_index + 1
                next_payload = json.dumps({'job_index': next_job_index, 'jobs': jobs})
                try:
                    response = lambda_client.invoke(
                        FunctionName=context.function_name,
                        InvocationType='Event',  # Asynchronous invocation
                        Payload=next_payload
                    )
                    print(f"Invoke response for {jobs[next_job_index]}: {response}")
                    print(f"Successfully invoked lambda for {jobs[next_job_index]}")
                except Exception as e:
                    print(f"Error invoking Lambda for {jobs[next_job_index]}: {str(e)}")
                    return {
                        'statusCode': 500,
                        'body': f"Failed to invoke lambda for {jobs[next_job_index]} due to {str(e)}"
                    }
            
            print("All jobs have been started successfully.")
            return {
                'statusCode': 200,
                'body': "Jobs triggered successfully."
            }
        
        # If it's an S3 event
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            print(f"Bucket: {bucket}, Key: {key}")

            # Define job sequences
            job_sequences = {
                'vishnu-reddit-bucket': ['reddit_clean', 'combined-clean'],
                'vishnu-news-bucket': ['news_clean', 'combined-clean']
            }

            # Determine the job sequence based on the bucket
            if bucket in job_sequences:
                jobs = job_sequences[bucket]
                job_index = 0
                
                print(f"Starting job: {jobs[job_index]}")
                
                # Start the first job
                job_run_id = start_job(jobs[job_index])
                print(f"Job {jobs[job_index]} started successfully with JobRunId: {job_run_id}")
                
                # Invoke the next job in the sequence
                if len(jobs) > 1:
                    next_job_index = job_index + 1
                    next_payload = json.dumps({'job_index': next_job_index, 'jobs': jobs})
                    try:
                        response = lambda_client.invoke(
                            FunctionName=context.function_name,
                            InvocationType='Event',  # Asynchronous invocation
                            Payload=next_payload
                        )
                        print(f"Invoke response for {jobs[next_job_index]}: {response}")
                        print(f"Successfully invoked lambda for {jobs[next_job_index]}")
                    except Exception as e:
                        print(f"Error invoking Lambda for {jobs[next_job_index]}: {str(e)}")
                        return {
                            'statusCode': 500,
                            'body': f"Failed to invoke lambda for {jobs[next_job_index]} due to {str(e)}"
                        }
            else:
                print(f"Unrecognized bucket: {bucket}")
        
        print("All jobs have been started successfully.")
        return {
            'statusCode': 200,
            'body': "Jobs triggered successfully."
        }
    except KeyError as e:
        print(f"KeyError: {str(e)}")
        return {
            'statusCode': 400,
            'body': f"Invalid event structure: {str(e)}"
        }
