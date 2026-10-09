'''
Computation of the average temperature per year using the DataFrame API.
'''


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import avg
import sys
import os
import time
import logging
from contextlib import contextmanager


@contextmanager
def time_usage(name=""):
    '''
    Logs the time usage in a code block.
   
    Parameters
    -----------


    name : The label attached to the code block.
    '''
    start = time.time()
    yield
    end = time.time()
    elapsed_seconds = float("%.4f" % (end - start))
    logging.info('%s: elapsed seconds: %s', name, elapsed_seconds)
   
# Sets logging, so that the execution time of the queries will be visible.
logging.getLogger().setLevel(logging.INFO)


# The input CSV file name must be specified at the command line when executing this Python program.
# If not, an error is raised.
if len(sys.argv) != 2:
    print("Specify the input CSV file on the command line")
    sys.exit(-1)


# Initialization of the SparkSession.
spark = (SparkSession\
         .builder\
         .appName("Avg temperatures with dataframes")\
         .getOrCreate())


# Disable most of the Spark logging
spark.sparkContext.setLogLevel("ERROR")


def avg_temperature_df(df):
    '''
    Returns the average temperature per year.


    Parameters
    -----------
    df : DataFrame with the input data.


    Returns
    --------
    A new DataFrame that contains the average temperature per year.
    '''
    return df.groupBy("year") \
             .agg(avg("temperature").alias("avg_temperature")) \
             .orderBy("year")




input_path = "hdfs://sar17:9000/data/temperatures/"
output_path = "hdfs://sar17:9000/sdim/sdim_14/"


# Get the name of the input CSV file
csvfile_name = os.path.basename(sys.argv[1])


# Get the absolute path to the input CSV file.
input_file = input_path + csvfile_name


# Sets the path to the output folder.
output_folder = output_path + os.path.splitext(csvfile_name)[0] + ".df.out"


# Schema of the CSV file: year, month, day, hour, minute, second, temperature
schema = StructType([
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True),
    StructField("minute", IntegerType(), True),
    StructField("second", IntegerType(), True),
    StructField("temperature", DoubleType(), True)
])


df = spark.read.csv(input_file, schema=schema, header=False)


# Call the function to compute the average temperature per year
df_avg = avg_temperature_df(df)
df_avg.cache()


# Prints the output DataFrame to HDFS.
with time_usage("Execution time of first action df_avg.write.csv"):
    df_avg.write.csv(output_folder)


# Number of partitions of the RDD behind the output DataFrame
print("Number of partitions of df_avg: {}".format(df_avg.rdd.getNumPartitions()))


# Prints the first 5 rows to the terminal.
with time_usage("Execution time of second action df_avg.write.csv"):
    df_avg.write.csv(output_folder+".bis")
