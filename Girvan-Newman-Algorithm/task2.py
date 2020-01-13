# Shannon Tee
# ID: 8962768963

from pyspark import SparkContext
import sys 
import time
from itertools import combinations

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
                else:
                    total = sum([shortest_dict[parent2] for parent2 in parent_dict[node]])
                    value = float(shortest_dict[parent]) / total * node_values[node]

                node_values[parent] += value
                edge = tuple(sorted([str(node), str(parent)]))

                if edge in edge_values:
                    edge_values[edge] += value
                else:
                    edge_values[edge] = value

def recalculate():
    new_edge_values = {}
    communities = set()

    for root in graph.keys():
        level = {root: 0}
        shortest_dict = {root: 1}
        visited = [root.encode("utf-8")]
        queue = [root]
        parent_dict={}
        while queue:
            node = queue.pop(0)
            for i in graph[node]:
                if i not in visited:
                    queue.append(i)
                    visited.append(i.encode("utf-8"))
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
                        value = float(node_values[node])
                    else:
                        total = sum([shortest_dict[parent2] for parent2 in parent_dict[node]])
                        value = float(shortest_dict[parent]) / float(total) * node_values[node]

                    node_values[parent] += value
                    edge = tuple(sorted([str(node), str(parent)]))

                    if edge in new_edge_values:
                        new_edge_values[edge] += value
                    else:
                        new_edge_values[edge] = value

        communities.add(tuple(sorted(visited)))
    return new_edge_values, communities

def modularity(original_graph,communities, m):
    total = 0

    for community in communities:
        for pair in list(combinations(community,2)):
            if (pair[1] not in original_graph[pair[0]]) or (pair[0] not in original_graph[pair[1]]):
                total += 0 - (float(len(original_graph[pair[0]])) * float(len(original_graph[pair[1]])) / (2*m))
            else:
                total += 1 - (float(len(original_graph[pair[0]])) * float(len(original_graph[pair[1]])) / (2*m))
    q = float(total)/(2*m)
    return q

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

m = len(edge_values)
original_graph = graph

mod = 0
while edge_values:
    removed = set()
    edge = edge_values[0][0]
    removed.add(edge)

    for edge in removed:
        graph[edge[0]].remove(edge[1])
        graph[edge[1]].remove(edge[0])

    edge_values, communities = recalculate()
    edge_values = [(k,v/2) for k, v in edge_values.items()]
    edge_values.sort()
    edge_values.sort(key=lambda x: x[1], reverse = True)

    q = modularity(original_graph, communities, m)
    
    if mod < q:
        mod = q
        results = list(communities)
        
    print(q)
    
results.sort()
results.sort(key=lambda x: len(x))

outfile = open(output_file, "w")
for item in results:
    string = ''
    for i in item:
        string += "'" + i + "', "
    string = string[:-2]
    outfile.write(string)
    outfile.write('\n')

end_time = time.time()
print('Duration:', end_time - start_time)