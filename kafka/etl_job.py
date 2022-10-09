### Use hadoop and spark to read from s3 and clean each record
import os
import findspark
findspark.init('/Users/ronan/spark-3.2.2-bin-hadoop3.2')

import passwords

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, split, size #needed for etl1
from pyspark.sql.functions import * #needed for etl3


# Adding the additional spark packages `aws-java-sdk` and `hadoop-aws` required to get data from S3  
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"


# Creating  Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
accessKeyId=passwords.aws_access_key_id
secretAccessKey=passwords.aws_secret_access_key
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create  Spark session
spark=SparkSession(sc)

# Read from the S3 bucket
df = spark.read.json('s3a://pinterestdata83436ecb/*.json') # You may want to change this to read csv depending on the files your reading from the bucket

# create new column with the count of tags
#ETLjob 1: create a new column with the count of tags
df = df.withColumn('count_of_tags', size(split(col("tag_list"), r",")))
#ETLJob 2: conver the tag_list column to an array
df = df.withColumn('tag_list', split(col("tag_list"), ",\s*"))
#ETLJob 3: convert the is_image_or_video column string to single chars
df = df.withColumn('is_image_or_video', regexp_replace('is_image_or_video', 'image', 'i')) #change word image to i
df =df.withColumn('is_image_or_video', regexp_replace('is_image_or_video', 'video', 'v')) #change word video to v

df.printSchema()

df.show(5)