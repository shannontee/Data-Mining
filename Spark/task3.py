from pyspark import SparkContext
import os
import sys 
import json
import time

start_time = time.time()
input_review_file = sys.argv[1]
input_business_file = sys.argv[2]
output_file1 = sys.argv[3]
output_file2 = sys.argv[4]

sc = SparkContext("local", "first app")
review_file = sc.textFile(input_review_file).repartition(8)
review = review_file.map(lambda x: (x.split('business_id":"')[1].split('",')[0], float(x.split('stars":')[1].split(',')[0])))

business_file = sc.textFile(input_business_file).repartition(8)


business = business_file.map(lambda x: (x.split('business_id":"')[1].split('",')[0], x.split('city":"')[1].split('",')[0]))


stars = business.join(review).values().repartition(8)

average_stars= stars.mapValues(lambda x: (x, 1)).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).mapValues(lambda v: v[0]/v[1])

end_time = time.time()
initial_time = end_time - start_time

m1_start = time.time()
results = average_stars.sortBy(lambda x: (-x[1], x[0])).collect()
m1_results = results[0:10]
print(m1_results)
m1_end = time.time()
m1_time = m1_end - m1_start

m2_start = time.time()
m2_results = average_stars.sortBy(lambda x: (-x[1], x[0])).take(10)
print(m2_results)
m2_end = time.time()
m2_time = m2_end - m2_start


outfile1 = open(output_file1, "w")
outfile1.write('city,stars\n')

for i in results:
	outfile1.write( i[0] +','+ str(i[1]) + '\n')

output = {}
output['m1'] = (m1_time + initial_time)
output['m2'] = (m2_time + initial_time)
output['explanation'] = "Method 2 demonstrates the benefits of laziness in Spark because when a transformation operation is performed, such as sortBy, it will not be executed until an action is performed. Because of this, method 2 saves a lot more time than method 1 since Spark will not compute intermediate RDDs, but will be done as soon as 10 elements of the filtered RDD have been computed." 

with open(output_file2, 'w') as outfile2:
    json.dump(output, outfile2)


























