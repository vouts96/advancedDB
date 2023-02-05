import time
import pyspark
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.appName('Q1_SQL').getOrCreate()

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

sqlDF1 = spark.sql("SELECT *\
    FROM tripdata\
    WHERE tip_amount =\
        (SELECT MAX(tip_amount) FROM tripdata\
        WHERE pickup_date BETWEEN '2022-03-01' AND '2022-03-31' AND Zone = 'Battery Park')\
        AND Zone = 'Battery Park'")

sqlDF1.collect()
sqlDF1.show()

time = time.time() - start_time

print('Execution time:', time)
