import time
import pyspark
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName('Q1_SQL').getOrCreate()

df_trips = spark.read.parquet('hdfs://master:9000/project_data/*.parquet')

df_trips = df_trips.withColumn('pickup_date', to_date(df_trips.tpep_pickup_datetime,'YYYY-MM-DD'))
df_trips.createOrReplaceTempView("tripdata")

start_time = time.time()

sqlDF1 = spark.sql("SELECT\
    DOLocationID,\
    tip_amount,\
    pickup_date\
    FROM tripdata\
    WHERE tip_amount =\
        (SELECT MAX(tip_amount) FROM tripdata\
        WHERE pickup_date BETWEEN '2022-03-01' AND '2022-03-31' AND DOLocationID = 12)\
        AND DOLocationID = 12").show()

time = time.time() - start_time

print("Execution time:", time)
