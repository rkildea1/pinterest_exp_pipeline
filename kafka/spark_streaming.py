import os
import findspark
findspark.init('/Users/ronan/spark-3.2.2-bin-hadoop3.2')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql.functions import from_json, col, size, split



# os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2 spark_streaming.py pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2 pyspark-shell'

kafka_topic_name = 'pinterest_streaming_uploads' #topic to stream from
# Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'


spark = SparkSession \
        .builder \
        .appName("KafkaStreaming") \
        .getOrCreate()


# define the schema
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


# streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("parsed_value")) \
        .select(col("parsed_value.*"))

stream_df = stream_df.withColumn('count_of_tags', size(split(col("tag_list"), r",")))

stream_df.writeStream.outputMode("append") \
    .format("console") \
    .option("truncate", True) \
    .start() \
    .awaitTermination()