### Use hadoop and spark to read from s3 and clean each record
import os
import findspark
findspark.init('/Users/ronan/spark-3.2.2-bin-hadoop3.2')
import passwords
from datetime import datetime
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, split, size #needed for etl1
from pyspark.sql.functions import *
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Ronan',
    'depends_on_past': False,
    'email': ['ronan@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2022, 10,15), 
    'retry_delay': timedelta(seconds=60),
    'end_date': datetime(2022, 10, 16),
}
dag = DAG(dag_id='_pinterest_batch_etl',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False,
         )

# Adding the additional spark packages `aws-java-sdk` and `hadoop-aws` required to get data from S3 and `spark-cassandra-connector` to write to cassandra
os.environ['PYSPARK_SUBMIT_ARGS'] ="--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell"



# Python Jobs 
def create_spark_configuration(ti):
  """ Creating Spark configuration """
  conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')
  sc=SparkContext(conf=conf)
  ti.xcom_push(key='sc', value=sc)


def s3_reader_configuration(ti):
  """ Configure the setting to read from the S3 bucket """
  accessKeyId=passwords.aws_access_key_id
  secretAccessKey=passwords.aws_secret_access_key
  sc = ti.xcom_pull(key='sc')
  hadoopConf = sc._jsc.hadoopConfiguration()
  hadoopConf.set('fs.s3a.access.key', accessKeyId)
  hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
  hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

def create_spark_session(ti):
  """Create a spark session"""
  sc = ti.xcom_pull(key='sc')
  spark=SparkSession(sc)
  ti.xcom_push(key='spark', value=spark)

def etl(ti):
  spark = ti.xcom_pull(key='spark')

  # Read from the S3 bucket
  df = spark.read.json('s3a://pinterestdata83436ecb/*.json') 
  # # create new column with the count of tags
  #ETLjob 1: create a new column with the count of tags
  df = df.withColumn('count_of_tags', size(split(col("tag_list"), r",")))
  #ETLJob 2: conver the tag_list column to an array
  df = df.withColumn('tag_list', split(col("tag_list"), ",\s*"))
  #ETLJob 3: convert the is_image_or_video column string to single chars
  df = df.withColumn('is_image_or_video', regexp_replace('is_image_or_video', 'image', 'i')) #change word image to i
  df = df.withColumn('is_image_or_video', regexp_replace('is_image_or_video', 'video', 'v')) #change word video to v
  #ETLJob 4: rename index column to source_index 
  df = df.withColumnRenamed("index", "source_index")
  # df.printSchema()
  df.show(5)



#Airflow jobs 
task_job_1 = PythonOperator(
    task_id='create_spark_configuration',
    python_callable= create_spark_configuration,
    dag=dag)

task_job_2 = PythonOperator(
    task_id='s3_reader_configuration',
    python_callable= s3_reader_configuration,
    dag=dag)

task_job_3 = PythonOperator(
    task_id='create_spark_session',
    python_callable= create_spark_session,
    dag=dag)

task_job_4 = PythonOperator(
    task_id='etl',
    python_callable= etl,
    dag=dag)

#tasks schedule
task_job_1 >> task_job_2 >> task_job_3 >> task_job_4











