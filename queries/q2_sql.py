import time
import pyspark
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName('Q2_SQL').getOrCreate()

#Create Dataframe for tripdata
df_trips = spark.read.parquet('hdfs://master:9000/project_data/*.parquet')

#Create Dataframe for zones
df_zones = spark.read.csv('hdfs://master:9000/project_data/taxi+_zone_lookup.csv', header=True)

#Join Zones with Tripdata
df_trips = df_trips.join(df_zones, df_trips['DOLocationID'] ==  df_zones['LocationID'],"inner")

#Change pickup date timestamp
df_trips = df_trips.withColumn('pickup_date', to_date(df_trips.tpep_pickup_datetime,'YYYY-MM-DD'))

#Create Sql View
df_trips.createOrReplaceTempView("tripdata")

start_time = time.time()

#Q2 
sql2 = spark.sql("SELECT t1.*\
    FROM tripdata t1\
    JOIN\
    (SELECT MAX(tolls_amount) AS tolls,\
    MONTH(pickup_date) AS pickup\
    FROM tripdata\
    WHERE MONTH(pickup_date) < 7\
    GROUP BY MONTH(pickup_date)\
    ORDER BY MAX(tolls_amount) DESC) t2\
    ON t1.tolls_amount = t2.tolls")

sql2.collect()
sql2.show()

time = time.time() - start_time

print('Execution time: ', time)
