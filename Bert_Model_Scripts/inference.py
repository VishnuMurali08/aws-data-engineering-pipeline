import os
import boto3
import json
import torch
import transformers
import pandas as pd
import numpy as np
import logging
from io import BytesIO, StringIO
from transformers import AutoTokenizer, AutoModelForSequenceClassification


def model_fn(model_dir):
    model_path = os.path.join(model_dir, "model")
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    model = AutoModelForSequenceClassification.from_pretrained(model_path)
    return model, tokenizer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def input_fn(request_body, request_content_type):
    logger.info(f"Received request with content type: {request_content_type}")
    if request_content_type == 'text/csv':
        data = StringIO(request_body.decode('utf-8'))
        input_data = pd.read_csv(data)
        logger.info("Processed direct CSV data.")
    elif request_content_type == 'text/plain':
        s3_path = request_body.decode('utf-8').strip()
        bucket, key = s3_path.replace("s3://", "").split('/', 1)
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        input_data = pd.read_csv(BytesIO(response['Body'].read()))
        logger.info(f"Processed CSV data from S3 path: {s3_path}")
    else:
        error_msg = f"Unsupported content type: {request_content_type}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    if 'public_post_comments' not in input_data.columns:
        error_msg = "CSV input must contain 'public_post_comments' column"
        logger.error(error_msg)
        raise ValueError(error_msg)

    return input_data['public_post_comments'].tolist()


def predict_fn(input_data, model_and_tokenizer):
    model, tokenizer = model_and_tokenizer
    inputs = tokenizer(input_data, return_tensors='pt', padding=True, truncation=True)
    outputs = model(**inputs)
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    return probs.detach().cpu().numpy()


def output_fn(prediction, content_type):
    if content_type == 'text/csv':
        # Create a DataFrame from the prediction
        pred_df = pd.DataFrame(prediction, columns=['Negative', 'Somewhat Negative', 'Neutral', 'Somewhat Positive', 'Positive'])
        
        # Combine predictions for Negative and Positive sentiments
        pred_df['Negative'] = pred_df['Negative'] + pred_df['Somewhat Negative']
        pred_df['Positive'] = pred_df['Somewhat Positive'] + pred_df['Positive']
        
        # Keep only the combined sentiment columns
        reduced_df = pred_df[['Negative', 'Neutral', 'Positive']]
        
        # Return the DataFrame as CSV
        return reduced_df.to_csv(index=False)

    elif content_type == 'application/json' or content_type is None:
        # Create a DataFrame from the prediction
        pred_df = pd.DataFrame(prediction, columns=['Negative', 'Somewhat Negative', 'Neutral', 'Somewhat Positive', 'Positive'])
        
        # Combine predictions for Negative and Positive sentiments
        pred_df['Negative'] = pred_df['Negative'] + pred_df['Somewhat Negative']
        pred_df['Positive'] = pred_df['Somewhat Positive'] + pred_df['Positive']
        
        # Keep only the combined sentiment columns
        reduced_df = pred_df[['Negative', 'Neutral', 'Positive']]
        
        # Get the max probability's index for each row
        max_prob_indices = np.argmax(reduced_df.values, axis=1)
        
        # Map indices to sentiment labels
        sentiment_labels = ['Negative', 'Neutral', 'Positive']
        predicted_sentiments = [sentiment_labels[index] for index in max_prob_indices]
        
        # Convert list of sentiments to JSON
        return json.dumps(predicted_sentiments)

    else:
        raise ValueError(f"Unsupported content type: {content_type}")
