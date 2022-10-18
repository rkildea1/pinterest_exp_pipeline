# pinterest_exp_pipeline

1. using sample data for emulating pinterest streaming uploads and post the records to localhost using FastAPI, Hadoop, Spark, S3
2. push the posts to localhost, to the consumer using the Kafka producer
3. two consumers are configured (one for batch and one for streaming)
4. the Kafka batch consumer extracts the records as dicts and puts them to s3 as jsons
5. Using PySpark and AWS_Hadoop package to read and clean the batch records over Airflow. Pushing cleaned data to Cassandra
6. 
7. 




## Passwords file referenced: 
```Source details for user emulation data to stream from
HOST = HOST
USER = HOST
PASSWORD = PASSWORD
DATABASE = DATABASE
PORT = PORT

S3 details for writing to the S3 bucket
aws_access_key_id = aws_access_key_id
aws_secret_access_key = aws_secret_access_key
aws_s3_bucket_name = aws_s3_bucket_name
```