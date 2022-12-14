import os
import findspark
findspark.init('/Users/ronan/spark-3.2.2-bin-hadoop3.2')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType
from pyspark.sql.functions import from_json, col, size, split, window
from pyspark.sql.functions import current_timestamp
import passwords

os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages  org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2 --driver-class-path /Users/ronan/Desktop/GitHub/pinterest_exp_pipeline/jars/postgresql-42.5.0.jar --jars /Users/ronan/Desktop/GitHub/pinterest_exp_pipeline/jars/postgresql-42.5.0.jar pyspark-shell'

kafka_topic_name = 'pinterest_streaming_uploads' #topic to stream from
# Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

# create the session 
spark = SparkSession \
        .builder \
        .appName("KafkaStreaming") \
        .getOrCreate()


# define the schema of each batch
schema = StructType() \
    .add("category", StringType()) \
    .add("index", IntegerType()) \
    .add("unique_id", StringType()) \
    .add("title", StringType()) \
    .add("description", StringType()) \
    .add("follower_count", StringType()) \
    .add("tag_list", StringType()) \
    .add("is_image_or_video", StringType()) \
    .add("image_src", StringType()) \
    .add("downloaded", IntegerType()) \
    .add("save_location", StringType()) 


#format of the source\ location\ 
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("parsed_value")) \
        .select(col("parsed_value.*"))


def transform_load(batch_df, batch_id):
        batch_df = batch_df.withColumn('count_of_tags', size(split(col("tag_list"), r",")))
        batch_df = batch_df.withColumn('timeStamp', current_timestamp())
        batch_df.write \
        .mode('append') \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/pinterest_streaming") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'pinterest_streaming_storage') \
        .option("user", f'{passwords.postgresusername}') \
        .option("password", f'{passwords.postgrespassword}') \
        .save()


stream_df.writeStream.outputMode("append") \
        .foreachBatch(transform_load)\
        .option("truncate", True) \
        .start() \
        .awaitTermination()



