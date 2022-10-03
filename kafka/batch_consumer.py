from kafka import KafkaConsumer
from json import loads
import boto3    
from json import dumps
import passwords

s3client = boto3.client('s3', 
                        aws_access_key_id=passwords.aws_access_key_id, 
                        aws_secret_access_key=passwords.aws_secret_access_key
                            )

# create a consumer to retrieve the message from the topics
data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
)
# data_stream_consumer.subscribe(topics=["pinterest_streaming_uploads"])
data_stream_consumer.subscribe(topics=["pinterestpipeline"])

# dump each event dict in s3 as a json
for message in data_stream_consumer:
    json_file = message.value
    filename = (message.value['unique_id'])+'.json'
    s3client.put_object(Body=dumps(json_file),Bucket='pinterestdata83436ecb',Key=filename)
    print(f'...filename: {filename} was uploaded')


