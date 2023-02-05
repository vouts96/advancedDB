import time
import pyspark
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName('Q5_SQL').getOrCreate()

df_trips = spark.read.parquet('hdfs://master:9000/project_data/*.parquet')

df_trips.createOrReplaceTempView("tripdata")

start_time = time.time()

sql5 = spark.sql("WITH temp_table AS (SELECT MONTH(tpep_pickup_datetime)AS month, DAY(tpep_pickup_datetime) AS day, AVG((tip_amount/total_amount)*100) AS tip_per\
    FROM tripdata\
    GROUP BY MONTH(tpep_pickup_datetime), DAY(tpep_pickup_datetime)\
    ORDER BY MONTH(tpep_pickup_datetime), tip_per DESC)\
    \
    SELECT * FROM (\
    SELECT temp_table.month, temp_table.day, temp_table.tip_per,\
    ROW_NUMBER() OVER (PARTITION BY temp_table.month ORDER BY temp_table.tip_per DESC) AS month_rank\
    FROM temp_table)\
    WHERE month_rank <= 5")

#sql5.show()

sql5.write.mode("overwrite").option("header",True).csv("hdfs://master:9000/out_data/sql5_out/")

time = time.time() - start_time

print('Execution time:', time)
