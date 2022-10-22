### Use hadoop and spark to read from s3 and clean each record
import os
import findspark
findspark.init('/Users/ronan/spark-3.2.2-bin-hadoop3.2')
import passwords
from datetime import datetime
from airflow import DAG
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, split, size #needed for etl1
from pyspark.sql.functions import *
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
temp_storage_path = Variable.get("temp_storage_path")
temp_storage_file = Variable.get("temp_storage_file")

#configure DAG Arguments
default_args = {
    'owner': 'Ronan',
    'depends_on_past': False,
    'email': ['ronan@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2022, 10,18)
}
# Configure DAG
dag = DAG(dag_id='_pinterest_batch_etl',
        default_args=default_args,
        schedule_interval='* * 0 0 0', #run once a day
        # schedule_interval="@once",
        catchup=False
         )



# CONFIGURING SPARK
# Adding the additional spark packages `aws-java-sdk` and `hadoop-aws` required to get data from S3 and `spark-cassandra-connector` to write to cassandra
os.environ['PYSPARK_SUBMIT_ARGS'] ="--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell"
# Creating  Spark configuration
conf = SparkConf().setAppName('S3toSpark').setMaster('local[*]')
sc=SparkContext(conf=conf)
# Configure the setting to read from the S3 bucket
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', passwords.aws_access_key_id)
hadoopConf.set('fs.s3a.secret.key', passwords.aws_secret_access_key)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS
# Create  Spark session
spark=SparkSession(sc)



# CONFIGURING PYTHON METHODS
def etl():
  """read from s3, do some transformations, and store transformed dataframe locally"""
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
  df.write.json(f'{temp_storage_path}{temp_storage_file}') 

def write_to_cassandra():
  """read the data from local to pyspark and write to cassandra"""
  df = spark.read.json(f'{temp_storage_path}{temp_storage_file}') 
  df.write.format("org.apache.spark.sql.cassandra").options(table="data", keyspace="pinterest_ks").mode("append").save()



# CONFIGURING AIRFLOW JOBS 
task_job_1 = PythonOperator(
    task_id='etl',
    python_callable= etl,
    dag=dag)

task_job_2 = PythonOperator(
    task_id='write_to_cassandra',
    python_callable= write_to_cassandra,
    dag=dag)

task_job_3 = BashOperator(
    task_id='delete_temporary_storage',
    bash_command=f'cd {temp_storage_path} && rm -r {temp_storage_file}',
    dag=dag)

# CONFIGURING TASKFLOW ORDER
task_job_1 >> task_job_2 >> task_job_3