# Real-Time Sentiment Analysis Pipeline for COVID-19 Vaccine Sentiments
## Overview
This project harnesses the power of Apache Kafka, AWS services, and machine learning to perform real-time sentiment analysis on COVID-19 vaccine discussions from Reddit and news sources. The pipeline automates the collection, processing, and analysis of data, culminating in a sentiment-enriched dashboard.

## Flowchart
<img width="1000" alt="image" src="https://github.com/VishnuMurali08/aws-data-engineering-pipeline/assets/97467016/701e2641-120f-4b06-b27e-1c783ce32231">

## Architecture
Data Collection: Scripts fetch data from Reddit and various news APIs.
Apache Kafka: Manages real-time message queueing through reddit_topic and news_topic.
AWS S3: Stores raw and processed data.
AWS Glue: Manages database schemas and performs ETL operations.
AWS SageMaker: Hosts and runs the sentiment analysis model.
AWS QuickSight: Provides visualization in the form of a dashboard.
## Components
#### 1. Kafka Producers and Consumers
Producers: Retrieve data from APIs and publish to Kafka topics.
Consumers: Subscribe to topics to retrieve messages and store them in S3 buckets.
#### 2. AWS Glue
Crawlers: Detect the schema of S3 data.
ETL Jobs: Clean and prepare data for analysis.
#### 3. Sentiment Analysis
Model Training: Train a BERT model for sentiment analysis.
Deployment: Deploy the model in SageMaker for inferencing.

## Automation with AWS Lambda
#### 1.Trigger Glue Jobs
Activates on updates in S3 buckets.
#### 2.Trigger BERT Endpoint
Runs when new data is available for analysis.
#### 3.QuickSight Data Refresh
Updates the dashboard upon new output generation.

## Dashboard
Explanation of the dashboard components and insights provided by the visualizations.
<img width="524" alt="image" src="https://github.com/VishnuMurali08/aws-data-engineering-pipeline/assets/97467016/127ff890-b9db-42ba-ad69-cf23504a0da2">

