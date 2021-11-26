#Libaries
import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Configure Spark Session
def configure_session(master_node = None, name = None):
    global spark
    
    spark = SparkSession\
        .builder\
        .master(master_node)\
        .appName(name)\
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:2.4.1")\
        .getOrCreate()
    print("Spark Session DONE!")

def ETL_processes(dataset = None):
    global df_load, max_actual_per_day_2019, avg_actual_jan_to_april, freq_per_month
    
    df_load = spark.read.csv(dataset, header = True)
    column_to_drop = ['day']
    df_load = df_load.drop(*column_to_drop)
    
    print("Transformation step.")
    print("1.Specify Schema.")
    print("...............")
    
    df_load = df_load.withColumn('month', df_load['month'].cast(IntegerType()))\
                     .withColumn('temp_2', df_load['temp_2'].cast(DoubleType()))\
                     .withColumn('temp_1', df_load['temp_1'].cast(DoubleType()))\
                     .withColumn('TAVG', df_load['TAVG'].cast(DoubleType()))\
                     .withColumn('Actual', df_load['Actual'].cast(DoubleType()))
    print("Schema has been successfully specified!")
    print(".......................................")
    print("2. Aggregations")
    
    max_actual_per_day_2019 = df_load.groupBy('week').max('Actual').withColumnRenamed('max(Actual)', 'max_temp')
    avg_actual_jan_to_april = df_load.groupBy('month').avg('Actual').withColumnRenamed('avg(Actual)', 'avg_temp')
    freq_per_month = df.groupBy('month').count().withColumnRenamed('count', 'no_of_obs')
    print("Aggregations have been completed!")

def build_collection_into_mongo(df_to_load):
    df_to_load.write.format("mongo")\
                    .mode("overwrite")\
                    .option("spark.mongodb.output.uri", f"mongodb://127.0.0.1:27017/Northwind.{df_to_load}").save()