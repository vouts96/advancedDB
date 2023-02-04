import time
import pyspark
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName('Q4_SQL').getOrCreate()

df_trips = spark.read.parquet('hdfs://master:9000/project_data/*.parquet')

df_trips.createOrReplaceTempView("tripdata")

start_time = time.time()

sql4 = spark.sql("WITH ex AS (SELECT WEEKDAY(tpep_pickup_datetime) AS weekday, HOUR(tpep_pickup_datetime) AS hour, SUM(passenger_count) AS pass\
    FROM tripdata\
    GROUP BY WEEKDAY(tpep_pickup_datetime), HOUR(tpep_pickup_datetime)\
    ORDER BY SUM(passenger_count) DESC, WEEKDAY(tpep_pickup_datetime))\
    \
    SELECT * FROM (\
    SELECT ex.weekday, ex.hour, ex.pass,\
    ROW_NUMBER() OVER (PARTITION BY ex.weekday ORDER BY ex.pass DESC) AS weekday_rank\
    FROM ex)\
    WHERE weekday_rank <= 3").show(21)

time = time.time() - start_time

print('Execution time:', time)
