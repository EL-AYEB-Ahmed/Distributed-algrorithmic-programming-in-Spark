
import sys
import os
from math import sqrt
from pyspark import SparkContext, SparkConf


# Slow MapReduce code -----------------------------------------------
#   all temperatures of a year are gathered in one list
def stddev_temperature_slow(theText_file):
    temperatures = theText_file           \
       .map(lambda line: line.split(",")) \
       .map(lambda term: (term[0], [float(term[6])])) \
       .reduceByKey(lambda x, y: x+y) \
       .mapValues(lambda lv: (sum(lv)/len(lv),
                              sqrt(sum(t*t for t in lv)/len(lv) - (sum(lv)/len(lv))**2)))
    return temperatures


# Fast MapReduce code -----------------------------------------------
#   map    : (year, (t, t^2, 1))
#   reduce : component-wise sum -> (sum t, sum t^2, N)
#   final  : mean = S1/N ; stddev = sqrt(S2/N - mean^2)
def stddev_temperature_fast(theText_file):
    temperatures = theText_file           \
       .map(lambda line: line.split(",")) \
       .map(lambda term: (term[0], (float(term[6]), float(term[6])**2, 1))) \
       .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2])) \
       .mapValues(lambda s: (s[0]/s[2], sqrt(s[1]/s[2] - (s[0]/s[2])**2)))
    return temperatures




# Main code ---------------------------------------------------------
input_path = "hdfs://sar17:9000/data/temperatures/"
output_path = "hdfs://sar17:9000/sdim/sdim_14/"
file_name = os.path.basename(sys.argv[1])
input_file_name = input_path + file_name
output_file_name = output_path + os.path.splitext(file_name)[0]+".out"


# - create the Spark context and open the input file as a RDD (in the HDFS)
sc = SparkContext()
text_file = sc.textFile(input_file_name)


# - run the MapReduce code and save the resulting RDD (in the HDFS)
#temperatures = stddev_temperature_slow(text_file)  #call SLOW version
temperatures = stddev_temperature_fast(text_file)   #call FAST version
temperatures.saveAsTextFile(output_file_name)
