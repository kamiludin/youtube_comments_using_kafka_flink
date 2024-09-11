import googleapiclient.discovery
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import time

# Define the Avro schema
VALUE_SCHEMA_STR = """
{
  "type": "record",
  "name": "YouTubeComment",
  "fields": [
    {"name": "author", "type": "string"},
    {"name": "updated_at", "type": "string"},
    {"name": "like_count", "type": "int"},
    {"name": "text", "type": "string"},
    {"name": "public", "type": "boolean"}
  ]
}
"""

# Configuration
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
YOUTUBE_DEVELOPER_KEY = 'your_youtube_key'
VIDEO_ID = 'C5B22ONbFrc'

# Initialize Schema Registry Client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Set up Avro Serializer
avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=VALUE_SCHEMA_STR
)

# Key serializer function
def key_serializer(key, serialization_context):
    if key is None:
        return None
    if not isinstance(key, str):
        raise TypeError(f"Key must be a string, but got {type(key).__name__}")
    return key.encode('utf-8')

# Configure Producer
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
}

producer = SerializingProducer(producer_conf)

# Build YouTube API client
def get_youtube_client():
    return googleapiclient.discovery.build("youtube", "v3", developerKey=YOUTUBE_DEVELOPER_KEY)

# Produce comments to Kafka
def produce_comments():
    # Consume Youtube API based on video ID
    youtube = get_youtube_client()
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=VIDEO_ID,
        maxResults=100
    )

    comment_count = 0

    while True:
        try:
            # Get API response
            response = request.execute()

            # Read response
            for item in response.get('items', []):
                comment = item['snippet']['topLevelComment']['snippet']
                public = item['snippet']['isPublic']
                data = {
                    'author': comment['authorDisplayName'],
                    'updated_at': comment['publishedAt'],
                    'like_count': comment['likeCount'],
                    'text': comment['textOriginal'],
                    'public': public
                }

                # Produce the message to Kafka
                try:
                    producer.produce(topic='youtube_comments', value=data, key=comment['authorDisplayName'])
                    print(f"Produced comment: {data}")
                    comment_count += 1
                    print(f"Total comments produced: {comment_count}")
                    time.sleep(0.1)  # Optional: reduce sleep time or remove it
                except Exception as e:
                    print(f"Error producing message: {e}")

            if comment_count >= 5000:
                break

            # Prepare the next request if more comments are available
            nextPageToken = response.get('nextPageToken')
            if not nextPageToken:
                break

            request = youtube.commentThreads().list(
                part="snippet",
                videoId=VIDEO_ID,
                maxResults=100,
                pageToken=nextPageToken
            )
        except Exception as e:
            print(f"Error executing request: {e}")
            break

    producer.flush()

if __name__ == "__main__":
    produce_comments()
