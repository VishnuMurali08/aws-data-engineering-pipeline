import boto3
import uuid

def lambda_handler(event, context):
    client = boto3.client('quicksight')
    #dataset_id = 'd999cf83-09b3-462f-a580-8ee826e92cf8' 
    dataset_id =  'b961b1e3-ebe0-4af2-81d2-2d81be025334' # Dataset ID as a string
    aws_account_id = '824755793198'  # AWS account ID

    # Generate a unique IngestionId
    ingestion_id = 'Ingestion-' + str(uuid.uuid4())

    # Invoke QuickSight's API to refresh the dataset
    response = client.create_ingestion(
        AwsAccountId=aws_account_id,
        DataSetId=dataset_id,
        IngestionId=ingestion_id,
        IngestionType='FULL_REFRESH'  
    )
    print(response)
