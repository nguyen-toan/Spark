/** This is a tutorial on graph algorithms with GraphX
  *
  * Reference: http://spark.apache.org/docs/latest/graphx-programming-guide.html#examples
  */

// Load data
val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
val users = sc.textFile("data/users.txt").map { line =>
  val fields = line.split(",")
  (fields(0).toLong, fields(1))
}

// Run PageRank
val ranks = graph.pageRank(0.0001).vertices
// Join the ranks with the usernames
val ranksByUsername = users.join(ranks).map {
  case (id, (username, rank)) => (username, rank)
}
// Print the result
println(ranksByUsername.collect().sortBy(_._2)(Ordering[Double].reverse).mkString("\n"))



// Find the connected components
val cc = graph.connectedComponents().vertices
// Join the connected components with the usernames
val ccByUsername = users.join(cc).map {
  case (id, (username, cc)) => (username, cc)
}
// Print the result
println(ccByUsername.collect().mkString("\n"))


// Triangle Counting
// Load the edges in canonical order and partition the graph for triangle count
val graph = GraphLoader.edgeListFile(sc, "data/followers.txt", true).
  partitionBy(PartitionStrategy.RandomVertexCut)
// Find the triangle count for each vertex
val triCounts = graph.triangleCount().vertices
// Join the triangle counts with the usernames
val users = sc.textFile("graphx/data/users.txt").map { line =>
  val fields = line.split(",")
  (fields(0).toLong, fields(1))
}
val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
  (username, tc)
}
// Print the result
println(triCountByUsername.collect().mkString("\n"))
