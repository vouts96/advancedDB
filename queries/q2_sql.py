import time
import pyspark
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName('Q2_SQL').getOrCreate()

df_trips = spark.read.parquet('hdfs://master:9000/project_data/*.parquet')

start_time = time.time()

df_trips = df_trips.withColumn('pickup_date', to_date(df_trips.tpep_pickup_datetime,'YYYY-MM-DD'))
df_trips.createOrReplaceTempView("tripdata")

#Q2 
sql2 = spark.sql("SELECT MAX(tolls_amount),\
    MONTH(pickup_date)\
    FROM tripdata\
    WHERE MONTH(pickup_date) < 7\
    GROUP BY MONTH(pickup_date)\
    ORDER BY MAX(tolls_amount) DESC").show()

time = time.time() - start_time

print('Execution time: ', time)
