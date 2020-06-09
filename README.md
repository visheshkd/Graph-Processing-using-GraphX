# Graph-Processing-using-GraphX
The purpose of this project is to develop a graph processing program using Pregel on Spark GraphX.

The following pseudo-code to do graph clustering using Pregel:

Read the input graph and construct the RDD of edges
Use the graph builder Graph.fromEdges to construct a Graph from the RDD of edges
Access the VertexRDD and change the value of each vertex to be the -1 except for the first 5 nodes (these are the initial cluster number)
Call the Graph.pregel method in the GraphX Pregel API to calculate the new cluster number for each vertex and propagate this number to the neighbors. For each vertex, this method changes its cluster number to the max cluster number of its neighbors only if the current cluster number is -1.
Group the graph vertices by their cluster number and print the partition sizes (you can use Spark RDD methods).
