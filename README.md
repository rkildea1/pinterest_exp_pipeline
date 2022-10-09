# pinterest_exp_pipeline
### use sample data for emulating pinterest streaming uploads and post the records to localhost using FastAPI
### push the posts to localhost to the consumer using the Kafka producer
### two consumers are configured (one for batch and one for streaming)
### the Kafka batch consumer extracts the records as dicts and puts them to s3 as jsons




Feeding data from pinterest into a consumer on the cli: 
1. Start Zookeeper $ bin/zookeeper-server-start.sh config/zookeeper.properties
2. Start Kafka $ bin/kafka-server-start.sh config/server.properties
3. open the consumer in cli $ bin/kafka-console-consumer.sh --topic pinterest_streaming_uploads --from-beginning --bootstrap-server 4.localhost:9092
4. run producer.py
5. Start the user_posting_emulation file

output: 

    ``` 
    (base) Ronans-MacBook-Pro:kafka_2.13-3.2.3 ronan$ bin/kafka-console-consumer.sh --topic pinterest_streaming_uploads --from-beginning --bootstrap-server localhost:9092
    {"category": "mens-fashion", "index": 7528, "unique_id": "fbe53c66-3442-4773-b19e-d3ec6f54dddf", "title": "No Title Data Available", "description": "No description available Story format", "follower_count": "User Info Error", "tag_list": "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e", "is_image_or_video": "multi-video(story page format)", "image_src": "Image src error.", "downloaded": 0, "save_location": "Local save in /data/mens-fashion"}
    {"category": "diy-and-crafts", "index": 2863, "unique_id": "9bf39437-42a6-4f02-99a0-9a0383d8cd70", "title": "25 Super Fun Summer Crafts for Kids - Of Life and Lisa", "description": "Keep the kids busy this summer with these easy diy crafts and projects. Creative and\u2026", "follower_count": "124k", "tag_list": "Summer Crafts For Kids,Fun Crafts For Kids,Summer Kids,Toddler Crafts,Crafts To Do,Diy For Kids,Summer Snow,Diys For Summer,Craft Ideas For Girls", "is_image_or_video": "image", "image_src": "https://i.pinimg.com/originals/b3/bc/e2/b3bce2964e8c8975387b39660eed5f16.jpg", "downloaded": 1, "save_location": "Local save in /data/diy-and-crafts"}```


## Passwords file referenced: 
Source details for user emulation data to stream from
HOST = HOST
USER = HOST
PASSWORD = PASSWORD
DATABASE = DATABASE
PORT = PORT

S3 details for writing to the S3 bucket
aws_access_key_id = aws_access_key_id
aws_secret_access_key = aws_secret_access_key
aws_s3_bucket_name = aws_s3_bucket_name