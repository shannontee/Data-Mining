from pyspark import SparkContext
import sys 
import time
from itertools import combinations 

start_time = time.time()

threshold = int(sys.argv[1])
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]

sc = SparkContext("local", "first app")
text_file = sc.textFile(input_file).filter(lambda x: x != "user_id,business_id").repartition(3)

baskets = text_file.map(lambda x: (x.split(",")[0], [x.split(",")[1]])).reduceByKey(lambda a,b: a+b).map(lambda x: (x[0], sorted(x[1])))
baskets = baskets.filter(lambda x: (len(x[1]) > threshold)).map(lambda x: (x[0], list(set(x[1]))))
num_baskets = baskets.count() 

def phase1(subset, support, num_baskets):
	subset = list(subset)
	subset_support = support * len(subset) / num_baskets

	itemsets = []

	# Finds all candidate single itemsets
	candidate_single = {}
	values = {}

	for basket in subset:
		for item in basket[1]:

			if item in values:
				values[item].append(basket[0])
			else:
				values[item] = [basket[0]]

			if item in candidate_single:
				candidate_single[item] += 1
			else:
				candidate_single[item] = 1
	itemsets = sorted(set([k for (k,v) in candidate_single.items() if v >= subset_support]))
	candidate_itemsets = [(x,) for x in itemsets]

	# Finds all candidate itemsets greater than one
	count = {}
	pass_support = []

	for itemset in combinations(itemsets, 2):
		if len(set(values[itemset[0]]).intersection(values[itemset[1]])) >= subset_support:
			pass_support.append(itemset)

	candidate_itemsets += pass_support
	length = 3

	while True:
		itemsets = sorted(set([item for items in pass_support for item in items]))
		pass_support = []

		for x in combinations(itemsets, length):
			intersect = set(values[x[0]]).intersection(values[x[1]])

			for number in range(2,length):
				intersect = set(intersect).intersection(values[x[number]])

			if len(intersect) >= subset_support:
				pass_support.append(x)

		length +=1
		if pass_support == []:
			break
		else:
			candidate_itemsets += pass_support

	return candidate_itemsets

def phase2(subset, candidate_itemset):
	count = {}
	for basket in list(subset):
		for candidate in candidate_itemset:
			if set(candidate).issubset(basket[1]):
				if candidate in count:
					count[candidate] += 1
				else:
					count[candidate] = 1

	return count.items()

candidate_itemsets = baskets.mapPartitions(lambda x: phase1(x, support, num_baskets)).distinct().collect()
frequent_itemsets = baskets.mapPartitions(lambda x: phase2(x, candidate_itemsets)).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] >= support).keys().distinct().collect()

candidate_itemsets.sort()
candidate_itemsets.sort(key=len)

outfile = open(output_file, "w")
outfile.write('Candidates:\n')

for i in range(1,len(candidate_itemsets[-1])+1):
	lst = []
	for candidate in candidate_itemsets:
		if (i == 1) & (i == len(candidate)):
			lst.append("('" + candidate[0] + "')")
		elif i == len(candidate):
			lst.append(str(candidate))
	line = ','.join(lst)
	outfile.write(line)
	outfile.write('\n\n')

frequent_itemsets.sort()
frequent_itemsets.sort(key=len)

outfile.write('Frequent Itemsets:\n')

for i in range(1,len(frequent_itemsets[-1])+1):
	lst = []
	for frequent in frequent_itemsets:
		if (i == 1) & (i == len(frequent)):
			lst.append("('" + frequent[0] + "')")
		elif i == len(frequent):
			lst.append(str(frequent))
	line = ','.join(lst)
	outfile.write(line)
	
	if i != len(frequent_itemsets[-1]):
		outfile.write('\n\n')


end_time = time.time()

print('Duration:', end_time-start_time)


