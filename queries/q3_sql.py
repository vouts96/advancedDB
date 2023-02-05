import time
import pyspark
import pandas as pd 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q3_SQL').getOrCreate()

#Create Dataframe for tripdata
df_trips = spark.read.parquet('hdfs://master:9000/project_data/*.parquet')

#Create Dataframe for zonesa
df_zones = spark.read.csv('hdfs://master:9000/project_data/taxi+_zone_lookup.csv', header=True)

df_trips = df_trips.alias('df1')
df_zones = df_zones.alias('df2')

#Join Zones with Tripdata for DOLocationID
df_trips = df_trips.join(df_zones, df_trips['DOLocationID'] ==  df_zones['LocationID'],"inner").select('df1.*', 'df2.Zone')
df_trips = df_trips.withColumnRenamed('Zone', 'DOLZone')


df_trips = df_trips.alias('df1')
df_zones = df_zones.alias('df2')

#Join Zones with Tripdata for PULocationID
df_trips = df_trips.join(df_zones, df_trips['PULocationID'] ==  df_zones['LocationID'],"inner").select('df1.*', 'df2.Zone')
df_trips = df_trips.withColumnRenamed('Zone', 'PULZone')


#Change pickup date timestamp
df_trips = df_trips.withColumn('pickup_date', to_date(df_trips.tpep_pickup_datetime,'YYYY-MM-DD'))

#Create Sql View
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
        AND DOLZone != PULZone\
    GROUP BY \
        YEAR(pickup_date),\
        MONTH(pickup_date),\
        CASE WHEN DAY(pickup_date) <16 THEN 'First Half' ELSE 'Second Half' END")

#sql3.show()

sql3.write.mode("overwrite").option("header",True).csv("hdfs://master:9000/out_data/sql3_out/")

time = time.time() - start_time

print('Execution time:', time)
