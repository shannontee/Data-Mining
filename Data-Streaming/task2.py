from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import binascii

port_number = int(sys.argv[1])
output_file = sys.argv[2]

f = open(output_file,"w")
f.write("Time,Ground Truth,Estimation\n")
f.close()

def myhashs(s):
	user_int = int(binascii.hexlify(s.encode('utf8')),16)
	result = []

	a = [13177, 13669, 14851, 16267, 13577, 16369, 15467, 13007, 15263, 12211, 13597, 12073, 12641, 12457, 12703]
	b = [12011, 13121, 12641, 10709, 11813, 15227, 16979, 14149, 11393, 13477, 13457, 17509, 15427, 11471, 13649]

	for i in range(0,15):
		result.append(((a[i] * user_int + b[i])%13687) % 1024)

	return result

def flajolet_martin(time, rdd):
	global output_file

	actual_users = set()
	max_zero = [-1] * 15

	for user in rdd.collect():
		actual_users.add(user)
		hashvals = myhashs(user)

		hash_binary = [bin(val) for val in hashvals]

		for i in range(0, len(hash_binary)):
			zero = 0
			for b in hash_binary[i][::-1]:
				if b == '0':
					zero +=1
				else:
					break

			if zero > max_zero[i]:
				max_zero[i] = zero

	max_zero = [2**r for r in max_zero]
	result = int(sum(max_zero)/len(max_zero))

	f = open(output_file,"a")
	f.write(str(time)+ "," + str(len(actual_users))+","+str(result) + '\n')
	f.close()

sc = SparkContext("local[*]", "Task2")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)
stream = ssc.socketTextStream("localhost", port_number).window(30,10)
stream.foreachRDD(flajolet_martin)

ssc.start()         
ssc.awaitTermination()