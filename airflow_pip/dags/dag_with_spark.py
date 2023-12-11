#  pyspark imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType,StringType
import pyspark.sql.functions as func

# airflow imports 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def creating_schema():
    spark=SparkSession.builder.appName("FirstApp").getOrCreate()
    myschema=StructType([
        StructField("userID",IntegerType(), True),
        StructField("name",StringType(),True),
        StructField("age", IntegerType(), True),
        StructField("friends", IntegerType(),True)
        ])
    people= spark.read.format("csv").schema(myschema)\
    .option("path","data/fakefriends.csv").load()

    output=people.select(people.userID,people.name,people.age,people.friends)\
        .where(people.age<30).withColumn('insert_ts',func.current_timestamp())\
        .orderBy(people.userID)

    output.createOrReplaceTempView("peoples")
    spark.sql("select name, age,friends, insert_ts from peoples").show()

default_args={
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_with_pyspark',
    description='dag which performs pyspark job',
    start_date=datetime(2023,12,5,3),
    schedule_interval='@daily'
) as dag:
     task_pyspark_operation = PythonOperator(
        task_id='pyspark_operation',
        python_callable=spark_operation
    )
