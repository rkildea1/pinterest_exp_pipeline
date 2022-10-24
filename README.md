# pinterest_exp_pipeline

## Using sample data for emulating the Pinterest User Uploads data engineering pipeline
## Handling real-time uploads and batch storage using FastAPI, Kafka, Hadoop, Spark, Cassandra, S3, Postgres and Airflow

[Click here to see an Example User Post](https://github.com/rkildea1/pinterest_exp_pipeline/blob/main/kafka/user_emulation_output_example.json)


1. Pushing the simulated uploads to localhost with FastAPI and RDS 
2. A Kafka producer is created and two consumers are configured (one for batch and one for streaming processing)
4. The Kafka batch consumer extracts the records as dicts and puts them to s3 as jsons
5. PySpark and the AWS_Hadoop maven package is used to read and clean the batch records (scheduled to run daily with Airflow). 
6. The cleaned batch data is pushed to Cassandra 
7. The Kafka streaming consumer is used by PySpark streaming for real time processing
8. Each batch is processed by pyspark and written to Postgres (ultimately to be used for real-time reporting etc)




## Passwords file referenced: 
```Source details for user emulation data to stream from
<!-- User emulation details -->
HOST = HOST
USER = HOST
PASSWORD = PASSWORD
DATABASE = DATABASE
PORT = PORT

<!-- s3 config -->
S3 details for writing to the S3 bucket
aws_access_key_id = aws_access_key_id
aws_secret_access_key = aws_secret_access_key
aws_s3_bucket_name = aws_s3_bucket_name

<!-- Postgres Details -->
postgresusername = username
postgrespassword = password
```



