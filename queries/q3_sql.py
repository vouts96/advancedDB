import time
import pyspark
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q3_SQL').getOrCreate()

df_trips = spark.read.parquet('hdfs://master:9000/project_data/*.parquet')


df_trips = df_trips.withColumn('pickup_date', to_date(df_trips.tpep_pickup_datetime,'YYYY-MM-DD'))
df_trips.createOrReplaceTempView("tripdata")

start_time = time.time()

sql3 = spark.sql("SELECT\
    YEAR(pickup_date) AS YR,\
    MONTH(pickup_date) AS MN,\
    CASE WHEN DAY(pickup_date) <16 THEN 'First Half' ELSE 'Second Half' END Month_Part,\
    AVG(trip_distance) AS avg_distance,\
    AVG(total_amount) AS avg_cost\
    FROM tripdata\
    WHERE MONTH(pickup_date) < 7 AND YEAR(pickup_date) = 2022\
        AND DOLocationID != PULocationID\
    GROUP BY \
        YEAR(pickup_date),\
        MONTH(pickup_date),\
        CASE WHEN DAY(pickup_date) <16 THEN 'First Half' ELSE 'Second Half' END").show()

time = time.time() - start_time

print('Execution time:', time)
