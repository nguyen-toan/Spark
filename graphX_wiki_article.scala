/** This is a tutorial on graph analytics with GraphX on Wikipedia Articles
  *
  * Reference: http://ampcamp.berkeley.edu/5/exercises/graph-analytics-with-graphx.html
  */

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// Load the Wikipedia Articles
val articles = sc.textFile("data/graphx/graphx-wiki-vertices.txt")
val links = sc.textFile("data/graphx/graphx-wiki-edges.txt")

// Display the title of the first article:
articles.first

// Construct the Graph
val vertices = articles.map { line => 
  val fields = line.split('\t')
  (fields(0).toLong, fields(1))
}
val edges = links.map { line =>
  val fields = line.split('\t')
  Edge(fields(0).toLong, fields(1).toLong, 0)
}
val graph = Graph(vertices, edges, "").cache()

// Count article number
graph.vertices.count

// Let’s look at the first few triplets
graph.triplets.take(5).foreach(println(_))

// Running PageRank (error tolerance = 0.001)
val prGraph = graph.pageRank(0.001).cache()

// Create graph with rank and title
val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
  (v, title, rank) => (rank.getOrElse(0.0), title)
}

// Finding the ten most important vertices (those with the highest pageranks)
// and printing out their corresponding article titles
titleAndPrGraph.vertices.top(10) {
  Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
}.foreach(t => println(t._2._2 + ": " + t._2._1))
