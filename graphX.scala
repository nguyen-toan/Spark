/** This is a tutorial on graph analytics with GraphX
  *
  * Reference: http://ampcamp.berkeley.edu/5/exercises/graph-analytics-with-graphx.html
  */

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// Create data
// The vertex property for this graph is a tuple (String, Int) corresponding to the User Name and Age
val vertexArray = Array(
  (1L, ("Alice", 28)),
  (2L, ("Bob", 27)),
  (3L, ("Charlie", 65)),
  (4L, ("David", 42)),
  (5L, ("Ed", 55)),
  (6L, ("Fran", 50))
  )
// The edge property is just an Int corresponding to the number of Likes in our hypothetical social network
val edgeArray = Array(
  Edge(2L, 1L, 7),
  Edge(2L, 4L, 2),
  Edge(3L, 2L, 4),
  Edge(3L, 6L, 3),
  Edge(4L, 1L, 1),
  Edge(5L, 2L, 2),
  Edge(5L, 3L, 8),
  Edge(5L, 6L, 3)
  )

// Construct RDDs
val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

// Construct property graph
val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

// Display the names of the users that are at least 30 years old
graph.vertices.filter {
  case (id, (name, age)) => age > 30
}.collect.foreach {
  case (id, (name, age)) => println(s"$name is $age")
}
