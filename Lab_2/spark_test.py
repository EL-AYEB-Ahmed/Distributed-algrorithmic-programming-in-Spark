from pyspark import SparkContext

sc = SparkContext()

sc.setLogLevel("ERROR")

men_list = [i for i in range(10000000)]

# Create a RDD from the list
men_rdd = sc.parallelize(men_list)

print(f"Number of partitions in men_rdd: {men_rdd.getNumPartitions()}")

