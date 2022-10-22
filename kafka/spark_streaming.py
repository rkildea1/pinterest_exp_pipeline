import os
import findspark
findspark.init('/Users/ronan/spark-3.2.2-bin-hadoop3.2')
from pyspark.sql import SparkSession

# os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2 spark_streaming.py pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2 pyspark-shell'

kafka_topic_name = 'pinterest_streaming_uploads' #topic to stream from
# Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming") \
        .getOrCreate()



# streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()


stream_df.writeStream.outputMode("append").format("console").start().awaitTermination()
