from pyspark import SparkContext
import os
import sys 
import json
import time

input_file = sys.argv[1]
output_file = sys.argv[2]

sc = SparkContext("local", "first app")
text_file = sc.textFile(input_file).repartition(8)

# Total number of reviews
n_review = text_file.count() 

# Number of reviews in 2018
n_review_2018 = text_file.filter(lambda x: 'date":"2018-' in x).count()

# Number of distinct users who wrote reviews
users = text_file.map(lambda x: (x.split('user_id":"')[1].split('",')[0], 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: (-x[1], x[0]))
n_user = users.count()

# Top 10 users who wrote largest number of reviews and number of reviews they wrote
top_10_user = users.take(10) 

# Number of distinct businesses that have been reviewed
business = text_file.map(lambda x: (x.split('business_id":"')[1].split('",')[0], 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: (-x[1], x[0]))
n_business = business.count()

# Top 10 businesses that had the largest number of reviews and the number of reviews they had
top_10_business = business.take(10)

output_data = {}
output_data['n_review'] = n_review
output_data['n_review_2018'] = n_review_2018
output_data['n_user'] = n_user
output_data['top10_user'] = [list(user) for user in top_10_user]
output_data['n_business'] = n_business
output_data['top10_business'] = [list(business) for business in top_10_business]

with open(output_file, 'w') as outfile:
    json.dump(output_data, outfile)


