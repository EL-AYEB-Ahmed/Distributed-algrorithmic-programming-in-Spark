import sys
import os
from pyspark import SparkContext, SparkConf

# Slow MapReduce code -----------------------------------------------
def avg_temperature_slow(theText_file):
    temperatures = theText_file           \
       .map(lambda line: line.split(",")) \
       .map(lambda term: (term[0], [float(term[6])])) \
       .reduceByKey(lambda x, y: x+y) \
       .mapValues(lambda lv: sum(lv)/len(lv))
    return temperatures

# Fast MapReduce code (TO DEVELOP) ----------------------------------
#def avg_temperature_fast(theText_file):
#    temperatures = theText_file           \
#       .map(lambda line: line.split(",")) \
#       . ......  \
#       . ......  \
#       . ......
#    return temperatures


# Main code (TO COMPLETE) -------------------------------------------
# - generate the input and output file names (TO COMPLETE)
#   Set the input and output paths with your namenode (sar01 or sar17)
#   and your account
#     ex: input_path = "hdfs://sarZZ:9000/data/temperatures/"
#     ex: output_path = "hdfs://sarZZ:9000/XXX/XXX_YY/"
input_path = "hdfs://sar17:9000/data/temperatures/"# TO COMPLETE
output_path = "hdfs://sar17:9000/sdim/sdim_14/"# TO COMPLETE
                 
file_name = os.path.basename(sys.argv[1])
input_file_name = input_path + file_name
output_file_name = output_path + os.path.splitext(file_name)[0]+".out"

# - create the Spark context and open the input file as a RDD (in the HDFS)
sc = SparkContext()
text_file = sc.textFile(input_file_name)

# - run the MapReduce code and save the resulting RDD (in the HDFS)
temperatures = avg_temperature_slow(text_file)  #call SLOW version
#temperatures = avg_temperature_fast(text_file) #call FAST version 
temperatures.saveAsTextFile(output_file_name)

