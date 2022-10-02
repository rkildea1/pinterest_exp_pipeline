from kafka import KafkaConsumer
from json import loads

# create a consumer to retrieve the message from the topics
data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)

data_stream_consumer.subscribe(topics=["pinterest_streaming_uploads"])
for message in data_stream_consumer:
    print(message)