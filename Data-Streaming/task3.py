from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import random

random.seed(553)
port_number = int(sys.argv[1])
output_file = sys.argv[2]

f = open(output_file,"w")
f.write("seqnum,0_id,20_id,40_id,60_id,80_id\n")
f.close()

sequence_number = 1
reservoir = []

def fixed_size(time, rdd):
	global sequence_number
	global reservoir
	global output_file

	for user in rdd.collect():
		sequence_number += 1 

		if len(reservoir) < 100:
			reservoir.append(user)
		else:
			r1 = random.randint(0,100000) 
			r1 = r1 % sequence_number

			if r1 < 100: 
				r2 = random.randint(0,100000) % 100 
				reservoir[r2] = user
			else: 
				pass

		if sequence_number % 100 == 0:
			f = open(output_file,"a")
			f.write(str(sequence_number)+ "," + reservoir[0]+","+reservoir[20]+","+ reservoir[40]+","+reservoir[60]+","+reservoir[80]+'\n')
			f.close()

sc = SparkContext("local[*]", "Task3")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
stream = ssc.socketTextStream("localhost", port_number)
stream.foreachRDD(fixed_size)

ssc.start()         
ssc.awaitTermination()









