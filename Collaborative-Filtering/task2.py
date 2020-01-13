from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import sys 
import time
import math
import json

start_time = time.time()

train_file = sys.argv[1]
bus_file = sys.argv[2]
test_file = sys.argv[3]
case = int(sys.argv[4])
output_file = sys.argv[5]

sc = SparkContext("local[*]", "first app")
sc.setLogLevel("ERROR")
train = sc.textFile(train_file)
train_header = train.first()
train_rdd = train.filter(lambda x: x != train_header).map(lambda x: x.split(",")).coalesce(5)

test = sc.textFile(test_file)
test_header = test.first()
test_rdd = test.filter(lambda x: x != test_header).map(lambda x: x.split(",")).coalesce(5)
# actual = test_rdd.map(lambda x: ((x[0], x[1]), x[2]))

user = train_rdd.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], set(x[1]))).collect()
user_dict = dict(user)
business = train_rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], set(x[1]))).collect()
business_dict = dict(business)


rating = train_rdd.map(lambda x: ((x[0], x[1]), float(x[2]))).collect()
rating = dict(rating)

user_rating = train_rdd.map(lambda x: (x[0], [float(x[2])])).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], sum(x[1])/len(x[1]))).collect()
user_rating = dict(user_rating)

business_rating = train_rdd.map(lambda x: (x[1], [float(x[2])])).reduceByKey(lambda x,y: x+y).map(lambda x: (x[0], sum(x[1])/len(x[1]))).collect()
business_rating = dict(business_rating)

bus_json = sc.textFile(bus_file)
bus_json = bus_json.map(json.loads)
business_json = bus_json.map(lambda x:(x['business_id'], (float(x['stars']), x['review_count']))).collect()
business_json = dict(business_json)

business_json_keys = set(business_json.keys())
user_keys = set(user_dict.keys())
business_keys = set(business_dict.keys())

max_review = bus_json.map(lambda x: x['review_count']).sortBy(lambda x: x, ascending=False).take(1)[0]

def case_one():
    all_users ={}
    all_users_2 = {}
    # train_users = user_dict.keys()
    test_users = test_rdd.map(lambda x: x[0]).distinct().collect()
    users_list = list(user_keys.union(set(test_users)))

    for i in range(0,len(users_list)):
        all_users[users_list[i]] = i
        all_users_2[i] = users_list[i]

    all_business = {}
    all_business_2 = {}
    # train_business = business_dict.keys()
    test_business = test_rdd.map(lambda x: x[1]).distinct().collect()
    business_list = list(business_keys.union(set(test_business)))

    for i in range(0,len(business_list)):
        all_business[business_list[i]] = i
        all_business_2[i] = business_list[i]

    rank = 4
    iterations = 9

    ratings = train_rdd.map(lambda x: (int(all_users[x[0]]), int(all_business[x[1]]), float(x[2])))
    model = ALS.train(ratings, rank, iterations, 0.2)

    test = test_rdd.map(lambda x: (int(all_users[x[0]]), int(all_business[x[1]])))
    predictions = model.predictAll(test).map(lambda r: ((r[0], r[1]), r[2]))
    predictions = predictions.map(lambda x: ((all_users_2[x[0][0]], all_business_2[x[0][1]]), x[1]))

    # results = actual.join(predictions)

    # MSE = results.map(lambda r: (float(r[1][1]) - float(r[1][0])) ** 2).collect()
    # RMSE = math.sqrt(sum(MSE)/len(MSE))

    # print(RMSE)
    return predictions.collect()


def case_two(u, b):

    if b in business_json_keys:
        return business_json[b][0]

    elif (u not in user_keys) or (b not in business_keys):
        return 4.0

    elif b in business_keys:
        return business_rating[b]

    user_list = business_dict[b]

    p_numerator = 0
    p_denominator = 0

    for user in user_list:
        if u != user:   
            sim_business = user_dict[u].intersection(user_dict[user])

            if sim_business == set():
                 continue 

            user_count = 0
            test_count = 0

            for business in sim_business:
                user_count += rating[(user, business)]
                test_count += rating[(u, business)]

            user_avg = user_count / len(sim_business)
            test_avg = test_count / len(sim_business)


            numerator = 0
            denominator_user = 0
            denominator_test = 0

            for business in sim_business:
                numerator += (rating[(user, business)] - user_avg)*(rating[(u, business)] - test_avg)
                denominator_user += (rating[(user, business)] - user_avg)**2
                denominator_test += (rating[(u, business)] - test_avg)**2

            denominator = denominator_user * denominator_test

            if denominator != 0:
                w = numerator / denominator
            else:
                continue
                # w = 0.5

            p_numerator += (rating[(user, b)] - user_avg) * w
            p_denominator += abs(w)


    if p_denominator != 0:
        p = user_rating[u] + p_numerator / p_denominator

    else:
        return 4.0

    if p > 5:
        p = 5.0
    elif p < 1:
        p = 1.0

    return p

def case_three(u,b):

    if b in business_keys:
        return business_rating[b]

    elif b in business_json_keys:
        return business_json[b][0]

    elif (u not in user_keys) or (b not in business_keys):
        return 4.0

    business_list = user_dict[u]

    p_numerator = 0
    p_denominator = 0

    business_sim = {}
    for business in business_list:
        if b != business:
            sim_user = business_dict[b].intersection(business_dict[business])
          
            if sim_user == set():
                continue
                # w = 0.15

            else:
                business_count = 0
                test_count = 0

                for user in sim_user:
                    business_count += rating[(user, business)]
                    test_count += rating[(user, b)]

                business_avg = business_count / len(sim_user)
                test_avg = test_count / len(sim_user)

                numerator = 0
                denominator_business = 0
                denominator_test = 0

                for user in sim_user:
                    numerator += (rating[(user, b)] - test_avg) * (rating[(user, business)] - business_avg)
                    denominator_business += (rating[(user, business)] - business_avg) ** 2
                    denominator_test += (rating[(user, b)] - test_avg) ** 2

                denominator = math.sqrt(denominator_business) * math.sqrt(denominator_test)

                if denominator == 0.0:
                    continue
                    # w = 0.5
                else:
                    w = numerator / denominator
            
            business_sim[business] = w  

    sorted_dict = sorted(business_sim.items(), key=lambda x: x[1], reverse= True)

    new_dict = sorted_dict[:int(math.ceil(len(sorted_dict)/1.75))]

    for item in new_dict:
        p_numerator += (rating[(u,item[0])] * item[1])
        p_denominator += abs(item[1])


    if p_denominator != 0.0:
        p = p_numerator / p_denominator
    else:
        return 4.5

    if p > 5:
        p = 5.0
    elif p < 1.0:
        p = 1.0

    return p

def case_four(u,b):

    if b in business_json_keys:
        return business_json[b][0]

    elif (u not in user_dict.keys()) or (b not in business_dict.keys()):
        return 4.0

    business_list = user_dict[u]

    p_numerator = 0
    p_denominator = 0

    business_sim = {}
    for business in business_list:
        if b != business:
            sim_user = business_dict[b].intersection(business_dict[business])
          
            if sim_user == set():
                w = 0.3

            else:
                business_count = 0
                test_count = 0

                for user in sim_user:
                    business_count += rating[(user, business)]
                    test_count += rating[(user, b)]

                business_avg = business_count / len(sim_user)
                test_avg = test_count / len(sim_user)

                numerator = 0
                denominator_business = 0
                denominator_test = 0

                for user in sim_user:
                    numerator += (rating[(user, b)] - test_avg) * (rating[(user, business)] - business_avg)
                    denominator_business += (rating[(user, business)] - business_avg) ** 2
                    denominator_test += (rating[(user, b)] - test_avg) ** 2

                denominator = math.sqrt(denominator_business) * math.sqrt(denominator_test)

                if denominator == 0.0:
                    w = 0.5
                else:
                    w = numerator / denominator
            
            business_sim[business] = w  

    sorted_dict = sorted(business_sim.items(), key=lambda x: x[1], reverse= True)

    new_dict = sorted_dict[:math.ceil(len(sorted_dict)/1.5)]

    for item in new_dict:
        p_numerator += (rating[(u,item[0])] * item[1])
        p_denominator += abs(item[1])

    if p_denominator != 0.0:
        p = p_numerator / p_denominator
    else:
        if b in business_json.keys():
            return business_json[b][0]
        else:
            return 4.0

    if p > 5:
        p = 5.0
    elif p < 1:
        p = 1.0

    if b not in business_json.keys():
        return p

    else:
        reviews = business_json[b][1]
        alpha = reviews / (max_review)
        new_p = (1- alpha) * p + (alpha) * business_json[b][0]

        if p > 5:
            p = 5
        elif p < 1:
            p = 1.0
        return new_p


if case == 1:
    results = case_one()

elif case == 2: 
    predictions = test_rdd.map(lambda x: ((x[0], x[1]), case_two(x[0], x[1])))
    results = predictions.collect()

    # MSE = predictions.join(actual)
    # MSE = MSE.map(lambda x: (float(x[1][0]) - float(x[1][1])) ** 2).collect()
    # RMSE = math.sqrt(sum(MSE)/len(MSE))


elif case == 3:
    predictions = test_rdd.map(lambda x: ((x[0], x[1]), case_three(x[0], x[1])))
    results = predictions.collect()

    # MSE = predictions.join(actual)
    # MSE = MSE.map(lambda r: (float(r[1][0]) - float(r[1][1])) ** 2).collect()
    # RMSE = math.sqrt(sum(MSE)/len(MSE))

elif case == 4:
    predictions = test_rdd.map(lambda x: ((x[0], x[1]), case_four(x[0], x[1])))
    results = predictions.collect()

    # MSE = predictions.join(actual)
    # MSE = MSE.map(lambda r: (float(r[1][0]) - float(r[1][1])) ** 2).collect()
    # RMSE = math.sqrt(sum(MSE)/len(MSE))


outfile = open(output_file, "w")
outfile.write('user_id, business_id, prediction\n')

for item in sorted(results):
    string = str(item[0][0]) + ',' + str(item[0][1]) + ',' + str(item[1])
    outfile.write(string)
    outfile.write('\n')
# print ('RMSE:', RMSE)

end_time = time.time()
print('Duration:', end_time - start_time)
