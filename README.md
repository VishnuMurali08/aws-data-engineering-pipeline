# Real-Time Sentiment Analysis Pipeline for COVID-19 Vaccine Sentiments
## Overview
This project harnesses the power of Apache Kafka, AWS services, and machine learning to perform real-time sentiment analysis on COVID-19 vaccine discussions from Reddit and news sources. The pipeline automates the collection, processing, and analysis of data, culminating in a sentiment-enriched dashboard.

## Architecture
Data Collection: Scripts fetch data from Reddit and various news APIs.
Apache Kafka: Manages real-time message queueing through reddit_topic and news_topic.
AWS S3: Stores raw and processed data.
AWS Glue: Manages database schemas and performs ETL operations.
AWS SageMaker: Hosts and runs the sentiment analysis model.
AWS QuickSight: Provides visualization in the form of a dashboard.
## Components
### 1. Kafka Producers and Consumers
Producers: Retrieve data from APIs and publish to Kafka topics.
Consumers: Subscribe to topics to retrieve messages and store them in S3 buckets.
### 2. AWS Glue
### Crawlers: Detect the schema of S3 data.
ETL Jobs: Clean and prepare data for analysis.
### 3. Sentiment Analysis
Model Training: Train a BERT model for sentiment analysis.
Deployment: Deploy the model in SageMaker for inferencing.
4. Dashboard
Visualize sentiment data using AWS QuickSight.
Automation with AWS Lambda
Trigger Glue Jobs: Activates on updates in S3 buckets.
Trigger BERT Endpoint: Runs when new data is available for analysis.
QuickSight Data Refresh: Updates the dashboard upon new output generation.
Usage
Setup
Details on setting up the necessary AWS services, Kafka configuration, and API connections.

Running the Scripts
Instructions on how to run each component of the pipeline, including Kafka scripts, AWS Glue jobs, and the deployment of the SageMaker model.

Dashboard
Explanation of the dashboard components and insights provided by the visualizations.

Contributing
Guidelines for contributing to the project, including code style, pull requests, and issue reporting.

License
Specify the type of license governing the use and distribution of the project.

Acknowledgements
Credits to data providers, technology references, and any collaborators.
