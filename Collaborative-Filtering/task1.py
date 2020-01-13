from pyspark import SparkContext
import sys 
import time
import random
from itertools import combinations

start_time = time.time()

input_file = sys.argv[1]
output_file = sys.argv[2]

sc = SparkContext("local", "first app")
text_file = sc.textFile(input_file)
header = text_file.first()
rdd = text_file.filter(lambda x: x != header).map(lambda x: x.split(","))

users = rdd.map(lambda x: x[0]).sortBy(lambda x: x).distinct().collect()

user_dict = {}
for i in range(0, len(users)):
	user_dict[users[i]] = i 

business = rdd.map(lambda x: (x[1], [user_dict[x[0]]])).reduceByKey(lambda x,y: x+y)
business_dict = rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], set(x[1]))).collect()
business_dict = dict(business_dict)

hash_num = 150
band_num =50
row_num=3

a = [random.randint(0, len(users)) for i in range(0, hash_num)]
b = [random.randint(0, len(users)) for i in range(0, hash_num)]

def signature(values):
    min_list = [min((a[j]*v+b[j])%len(users) for v in values) for j in range(0,len(a))]
    return (min_list)

def bands(x):
    band  = []
    for i in range(0, len(x[1]), 3):
        band.append(((str(i) +','+ ','.join(map(str, x[1][i:i+row_num]))), [x[0]]))
    return band 

def candidates(x):
	candidates = combinations(sorted(x),2)
	return candidates

def similarity(x):
	b1 = business_dict[x[0]]
	b2 = business_dict[x[1]]
	sim = len(b1.intersection(b2)) / len(b1.union(b2))

	return ((x[0], x[1]),sim)

sig_matrix = business.map(lambda x:(x[0], signature(x[1])))
sig_bands = sig_matrix.flatMap(bands).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1]) > 1).map(lambda x: x[1])
candidate_pairs = sig_bands.flatMap(candidates).distinct()

results = candidate_pairs.map(similarity).filter(lambda x: x[1] >= 0.5).distinct().collect()
results.sort()

outfile = open(output_file, "w")
outfile.write('business_id_1, business_id_2, similarity\n')

for item in sorted(results):
	string = item[0][0] + ',' + item[0][1] + ',' + str(item[1])
	outfile.write(string)
	outfile.write('\n')

end_time = time.time()
print('Duration:', end_time - start_time)

