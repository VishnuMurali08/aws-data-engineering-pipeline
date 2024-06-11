import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import sys
from kafka import KafkaProducer, KafkaConsumer
import json
import configparser

# Initialize configparser
config = configparser.ConfigParser()
config.read('configuration.conf')

# Read API and Kafka configuration
api_key = config.get('news_config', 'api_key')
kafka_bootstrap_server = config.get('kafka_config', 'kafka_bootstrap_server')
kafka_topic = config.get('kafka_config', 'kafka_topic_news')

# Setup Kafka Producer
producer = KafkaProducer(bootstrap_servers=[kafka_bootstrap_server],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def fetch_news(query, start_date, end_date):
    base_url = 'https://newsapi.org/v2/everything'
    url = f"{base_url}?q={query}&from={start_date}&to={end_date}&sortBy=publishedAt&apiKey={api_key}"
    response = requests.get(url)
    data = response.json()
    if response.status_code != 200 or 'articles' not in data:
        print(f"Failed to retrieve data: {data.get('message', 'Unknown error')}")
        return []
    return data['articles']

def send_to_kafka(articles):
    for article in articles:
        message = {
            'title': article['title'],
            'description': article['description'],
            'url': article['url'],
            'publishedAt': article['publishedAt']
        }
        producer.send(kafka_topic, value=message)
    producer.flush()
    print(f"Sent {len(articles)} articles to Kafka.")

def convert_to_csv(kafka_topic):
    consumer = KafkaConsumer(kafka_topic,
                             bootstrap_servers=[kafka_bootstrap_server],
                             auto_offset_reset='earliest',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    articles = []
    for message in consumer:
        article = message.value
        articles.append(article)

    df = pd.DataFrame(articles)
    df.to_csv('news_articles.csv', index=False)
    print("Articles converted to CSV and saved as 'news_articles.csv'")

def main():
    if len(sys.argv) != 3:
        print("Usage: python script_name.py <start_date> <poll_interval>")
        print("Date should be in YYYYMMDD format, and poll_interval should be in seconds.")
        sys.exit(1)

    start_date = datetime.strptime(sys.argv[1], '%Y%m%d').isoformat()
    query = '"Covid vaccine" OR "Coronavirus vaccine"'
    poll_interval = int(sys.argv[2])  # polling interval in seconds

    try:
        current_time = datetime.now()
        start_datetime = datetime.fromisoformat(start_date)
        end_datetime = current_time
        print(f"Fetching all articles from {start_datetime.strftime('%Y-%m-%d %H:%M:%S')} until {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        articles = fetch_news(query, start_date, current_time.isoformat())
        if articles:
            send_to_kafka(articles)
            last_checked = current_time
        else:
            last_checked = start_datetime

        while True:
            time.sleep(poll_interval)
            end_datetime = datetime.now()
            print(f"Checking for news since {last_checked.strftime('%Y-%m-%d %H:%M:%S')} until {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
            articles = fetch_news(query, last_checked.isoformat(), end_datetime.isoformat())
            if articles:
                send_to_kafka(articles)
                last_checked = max(datetime.fromisoformat(article['publishedAt']) for article in articles)
            else:
                print("No new articles found.")
            last_checked = end_datetime  # Update last_checked to current end_datetime

    except KeyboardInterrupt:
        print("Stopped by user.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    # Convert Kafka messages to CSV
    convert_to_csv(kafka_topic)

if __name__ == '__main__':
    main()