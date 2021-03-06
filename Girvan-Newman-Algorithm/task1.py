# Shannon Tee
# ID: 8962768963

from pyspark import SparkContext
import sys 
import time

start_time = time.time()
sc = SparkContext("local[*]", "first app")
sc.setLogLevel("ERROR")
input_file = sys.argv[1]
output_file = sys.argv[2]

def create_graph(x):
    connections = set()

    for user, product in user_products.items():
        if user != x[0]:
            num = len(product.intersection(x[1]))

            if num >= 7:
                connections.add(user)
    return (x[0], connections)

def bfs(root):
    level = {root: 0}
    shortest_dict = {root: 1}
    visited = [root]
    queue = [root]

    parent_dict={}

    while queue:
        node = queue.pop(0)

        for i in graph[node]:
            if i not in visited:
                queue.append(i)
                visited.append(i)

                level[i] = level[node] + 1
                parent_dict[i] = set()
                parent_dict[i].add(node)
                shortest_dict[i] = shortest_dict[node]
            else:
                if level[node] == level[i] - 1:
                    parent_dict[i].add(node)
                    shortest_dict[i] += shortest_dict[node]

    node_values = {x: 1 for x in visited}

    visited.reverse()

    for node in visited:
        if node in parent_dict:
            for parent in parent_dict[node]:

                if len(parent_dict[node]) == 1:
                    value = node_values[node]

                elif len(parent_dict[node]) >=2:
                    total = sum([shortest_dict[parent2] for parent2 in parent_dict[node]])
                    value = shortest_dict[parent] / total * node_values[node]

                node_values[parent] += value

                edge = tuple(sorted([str(node), str(parent)]))

                if edge in edge_values:
                	edge_values[edge] += value
                else:
                	edge_values[edge] = value

input_rdd = sc.textFile(input_file)
input_header = input_rdd.first()

input_rdd = input_rdd.filter(lambda x : x != input_header).map(lambda x : x.split(",")).map(lambda x : (x[0], [x[1]])).reduceByKey(lambda x,y: x+y)
input_rdd = input_rdd.map(lambda x : (x[0], set(x[1])))
user_products = input_rdd.collectAsMap()

graph = input_rdd.map(lambda x: create_graph(x)).filter(lambda x : len(x[1]) != 0).collectAsMap()

edge_values = {}

for k in graph.keys():
	bfs(k)

edge_values = [(k,v/2) for k, v in edge_values.items()]
edge_values.sort()
edge_values.sort(key=lambda x: x[1], reverse = True)

outfile = open(output_file, "w")
for item in edge_values:
    string = str(item[0]) + ', ' + str(item[1])
    outfile.write(string)
    outfile.write('\n')

end_time = time.time()
print('Duration:', end_time - start_time)

