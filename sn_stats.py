import sys
import os
from pyspark import SparkContext

input_path = "hdfs://sar17:9000/data/social-network/"
file_name = os.path.basename(sys.argv[1])

sc = SparkContext()
lines = sc.textFile(input_path + file_name)

# degree of each node = number of friends on its line
degrees = lines.map(lambda line: line.split(",")) \
               .map(lambda t: len(t) - 1)

# (min, max, sum, count, sum of d(d-1)/2) computed inside the RDD
mn, mx, s, n, p = degrees \
    .map(lambda d: (d, d, d, 1, d*(d-1)//2)) \
    .reduce(lambda a, b: (min(a[0], b[0]), max(a[1], b[1]),
                          a[2]+b[2], a[3]+b[3], a[4]+b[4]))

avg = s / n
print("STATS", file_name, "nodes=", n, "min=", mn, "max=", mx,
      "avg=", round(avg, 2),
      "est_pairs=", round(n * avg * (avg - 1) / 2),
      "meas_pairs=", p)
