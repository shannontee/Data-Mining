# Compare the default vs. customized partition function to demonstrate how different number can improve performance of map and reduce tasks by comparing their time duration

from pyspark import SparkContext
import os
import sys 
import json
import time

start_time = time.time()

input_file = sys.argv[1]
output_file = sys.argv[2]
num_partitions = int(sys.argv[3])

output_data = {"default": {}, "customized": {}}
sc = SparkContext("local", "first app")
text_file = sc.textFile(input_file).repartition(16)
end_time = time.time()
initial_time = end_time - start_time

default_start = time.time()
top_10_business = text_file.map(lambda x: (x.split('business_id":"')[1].split('",')[0], 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)
default_end = time.time()
default_time = default_end - default_start

output_data["default"]["n_partition"] = text_file.getNumPartitions()
output_data["default"]["n_items"] = text_file.glom().map(len).collect()
output_data["default"]["exe_time"] = default_time + initial_time

customized_start = time.time()
text_file = text_file.repartition(num_partitions)
top_10_business = text_file.map(lambda x: (x.split('business_id":"')[1].split('",')[0], 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: (-x[1], x[0])).take(10)
customized_end = time.time()
customized_time = customized_end - customized_start

output_data["customized"]["n_partition"] = text_file.getNumPartitions()
output_data["customized"]["n_items"] = text_file.glom().map(len).collect()
output_data["customized"]["exe_time"] = customized_time + initial_time

output_data['explanation'] =  "Based on the outputs, when I decreased the number of partitions, the number of items in each partition will increase and the execution time will decrease which will improve performance. Spark has to read all partitions to find all values for all keys then bring together values across partitions to compute the final result for each key, therefore larger number of partitions requires greater amount of time to find all the keys and shuffling the data."

with open(output_file, 'w') as outfile:
    json.dump(output_data, outfile)























