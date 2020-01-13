from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import binascii

port_number = int(sys.argv[1])
output_file = sys.argv[2]

output = open(output_file,"w")
output.write("Time,FPR\n")
output.close()

actual_users = set()
FP = 0
TN = 0
filter_bit_array= [0] * 69997

def myhashs(s):
	user_int = int(binascii.hexlify(s.encode('utf8')),16)
	result = []
	
	a = [13177, 13669, 14851, 16267, 13577, 16369, 15467, 13007, 15263, 12211, 13597, 12073, 12641, 12457, 12703]
	b = [12011, 13121, 12641, 10709, 11813, 15227, 16979, 14149, 11393, 13477, 13457, 17509, 15427, 11471, 13649]

	for i in range(0,15):
		result.append((a[i] * user_int + b[i]) % 69997)
	return result

def bloom_filtering(time, rdd):
	global actual_users
	global filter_bit_array
	global TN
	global FP
	global output_file

	for user in rdd.collect():	
		hash_values = myhashs(user)

		if user not in actual_users:
			if len(hash_values) != len([1 for i in hash_values if filter_bit_array[i] == 1]):
				TN += 1
			else:
				FP += 1

		actual_users.add(user)
		for i in hash_values:
			if filter_bit_array[i] != 1:
				filter_bit_array[i] = 1

	if (FP + TN) == 0:
		FPR = 0
	else:
		FPR = float(FP) / (FP + TN)

	output = open(output_file,"a")
	output.write(str(time) + "," + str(FPR) + '\n')
	output.close()

sc = SparkContext("local[*]", "Task1")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
stream = ssc.socketTextStream("localhost", port_number)
stream.foreachRDD(bloom_filtering)

ssc.start()         
ssc.awaitTermination()









