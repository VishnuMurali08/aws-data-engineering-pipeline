import configparser
from kafka import KafkaConsumer
import boto3

# Initialize configparser
config = configparser.ConfigParser()
config.read('configuration.conf')
print("Configuration file read successfully.")

# Read AWS configuration from the configuration file
bucket_name = config.get('aws_config', 'reddit_bucket_name')
aws_region = config.get('aws_config', 'region_name')
aws_access_key_id = config.get('aws_config', 'aws_access_key_id')
aws_secret_access_key = config.get('aws_config', 'aws_secret_access_key')
print(f"Configured to use bucket: {bucket_name} in AWS region: {aws_region}")

# Kafka Configuration
kafka_bootstrap_server = config.get('kafka_config', 'kafka_bootstrap_server')
kafka_topic = config.get('kafka_config', 'kafka_topic_reddit')
print(f"Kafka consumer will connect to server: {kafka_bootstrap_server} on topic: {kafka_topic}")

def main():
    # Initialize S3 client using credentials from the configuration file
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    s3 = session.client('s3')
    print("AWS S3 client initialized successfully.")

    # Set up the Kafka consumer
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_server,
        auto_offset_reset='earliest',
        group_id='my-group',
        consumer_timeout_ms=10000,
        value_deserializer=lambda x: x.decode('utf-8')  # Decode messages from bytes to string
    )
    print("Kafka consumer initialized and ready to receive messages.")

    # Process each message
    for message in consumer:
        print(f"Received message at {message.topic}-{message.partition} offset {message.offset}")
        data = message.value  # Data is now the entire content of the CSV file
        file_name = f"{message.topic}-{message.partition}-{message.offset}.csv"
        print(f"Preparing to upload file: {file_name} to S3.")
        upload_to_s3(s3, bucket_name, file_name, data)

    # Close the consumer
    consumer.close()
    print("Kafka consumer closed.")

def upload_to_s3(s3_client, bucket_name, file_name, data):
    """Upload data to an S3 bucket"""
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=data.encode('utf-8')  # Ensure the data is in bytes format
        )
        print(f"Successfully uploaded {file_name} to S3 bucket {bucket_name}.")
    except Exception as e:
        print(f"Failed to upload {file_name} to S3: {e}")

if __name__ == '__main__':
    main()
