import sys
import os
from pyspark import SparkContext, SparkConf
def f(t):
    common_friend=t[0]
    l=[]
    for u in t[1:]:
        for v in t[1:]:
            if u<v:
                l.append(((u,v),common_friend))

# MapReduce code (TO DEVELOP) ---------------------------------------

# Version groupByKey
def common_friends_gbk(theTextFile):
    cf = theTextFile \
        .map(lambda line: line.split(",")) \
        .flatMap(lambda t: f(t)) \
        .groupByKey() \
        .mapValues(lambda t: list(t))
    return cf

# Version reduceByKey
def common_friends_rbk(theTextFile):
    cf = theTextFile \
        .map(lambda line: line.split(",")) \
        .flatMap(lambda t: f(t)) \
        .mapValues(lambda x: [x]) \
        .reduceByKey(lambda x, y: x + y)
    return cf


# Main code ---------------------------------------------------------
# - generate the input and output file names (TO COMPLETE)
#   Set the input and output paths with your namenode (sar01 or sar17)
#   and your account
#     ex: input_path = "hdfs://sarZZ:9000/data/social-network/"
#     ex: output_path = "hdfs://sarZZ:9000/XXX/XXX_YY/"
input_path = "hdfs://sar17:9000/data/social-network/"
output_path = "hdfs://sar17:9000/sdim/sdim_14/"
file_name = os.path.basename(sys.argv[1])
mode = sys.argv[2] if len(sys.argv) > 2 else "rbk"   # "rbk" or "gbk"
input_file_name = input_path + file_name
output_file_name = output_path + os.path.splitext(file_name)[0] + "_" + mode + ".out"
# - create the Spark context and open the input file (in the HDFS)
sc = SparkContext()
text_file = sc.textFile(input_file_name)
# - run the MapReduce code and save the results (in the HDFS)
if mode == "gbk":
    cf = common_friends_gbk(text_file)
else:
    cf = common_friends_rbk(text_file)
cf.saveAsTextFile(output_file_name)