import pathlib
import sys
import praw
import pandas as pd
import configparser
from kafka import KafkaProducer  
from validation import validate_input 

# Initialize configparser
config = configparser.ConfigParser()
config.read('configuration.conf')

# Read Reddit API configuration from the configuration file
SECRET = config.get('reddit_config', 'secret')
CLIENT_ID = config.get('reddit_config', 'client_id')
USER_AGENT = config.get('reddit_config', 'name')
SUBREDDIT = 'CovidVaccineEffects'

# Kafka Configuration
kafka_bootstrap_server = config.get('kafka_config', 'kafka_bootstrap_server')
kafka_topic = config.get('kafka_config', 'kafka_topic_reddit')

try:
    output_date = sys.argv[1]
    validate_input(output_date)  # Validate the date input
    output_name = f"{output_date}.csv"
except Exception as e:
    print(f"Error with date input. Error {e}")
    sys.exit(1)

def main():
    try:
        print("Subreddit set to:", SUBREDDIT)
        print("Connecting to Reddit API...")
        reddit = connect_api(SECRET, CLIENT_ID, USER_AGENT)
        print("Data extraction started...")
        data = extract_data(reddit)
        print("Data extraction completed. Saving to CSV...")
        load_to_csv(data, output_name)
        print("Data has been successfully saved to CSV.")
        print("Sending data to Kafka producer...")
        send_to_kafka(output_name)
        print("Data sent to Kafka producer.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

def connect_api(secret, client_id, user_agent):
    return praw.Reddit(client_id=client_id, client_secret=secret, user_agent=user_agent)

def extract_data(reddit_instance):
    subreddit = reddit_instance.subreddit(SUBREDDIT)
    posts = subreddit.top(time_filter='all', limit=300)
    ids, titles, authors, scores, upvote_ratio, number_of_comments, posting_times, permalinks, urls, subreddit_id = [], [], [], [], [], [], [], [], [], []

    for post in posts:
        ids.append(post.id)
        titles.append(post.title)
        authors.append(str(post.author))
        scores.append(post.score)
        upvote_ratio.append(post.upvote_ratio)
        number_of_comments.append(post.num_comments)
        posting_times.append(post.created_utc)
        permalinks.append(post.permalink)
        urls.append(post.url)
        subreddit_id.append(post.subreddit_id)

    df = pd.DataFrame({
        'id': ids, 'title': titles, 'author': authors, 'score': scores,
        'upvote_ratio': upvote_ratio, 'number_of_comments': number_of_comments,
        'time': pd.to_datetime(posting_times, unit="s"), 'permalink': permalinks,
        'url': urls, 'subreddit_id': subreddit_id
    })
    return df

def load_to_csv(extracted_data_df, output_name):
    """Save extracted data to CSV file"""
    script_dir = pathlib.Path(__file__).parent.resolve()
    output_dir = script_dir / "output"
    output_dir.mkdir(exist_ok=True)  # Make sure the output directory exists
    output_path = output_dir / output_name
    extracted_data_df.to_csv(output_path, index=False)

# def send_to_kafka(csv_filename):
#     """Send CSV file data to Kafka producer"""
#     producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
#     script_dir = pathlib.Path(__file__).parent.resolve()
#     output_dir = script_dir / "output"
#     file_path = output_dir / csv_filename

#     with file_path.open('rb') as file:
#         for line in file:
#             producer.send(kafka_topic, line)

def send_to_kafka(csv_filename):
    """Send entire CSV file data to Kafka producer"""
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server,
                             value_serializer=lambda x: x.encode('utf-8'))
    script_dir = pathlib.Path(__file__).parent.resolve()
    output_dir = script_dir / "output"
    file_path = output_dir / csv_filename

    with file_path.open('r', encoding='utf-8') as file:  # Specify UTF-8 encoding
        data = file.read()  # Read the entire file into memory
        producer.send(kafka_topic, data)
        producer.flush()  # Ensure data is sent before closing


if __name__ == '__main__':
    main()
