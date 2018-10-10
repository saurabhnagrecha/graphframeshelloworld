package quickstart

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object QuickStartExamples {

  /*
  Example code from the Quick Start guide at https://graphframes.github.io/quick-start.html
   */
  def runQuickStart(spark: SparkSession): Unit = {
    import spark.implicits._

    // Create a Vertex DataFrame with unique ID column "id"
    val v = Seq(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30)
    ).toDF("id", "name", "age")
    // Create an Edge DataFrame with "src" and "dst" columns
    val e = Seq(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow")
    ).toDF("src", "dst", "relationship")
    // Create a GraphFrame
    val g = GraphFrame(v, e)

    // Query: Get in-degree of each vertex.
    g.inDegrees.show()

    // Query: Count the number of "follow" connections in the graph.
    g.edges.filter("relationship = 'follow'").count()

    // Run PageRank algorithm, and show results.
    val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
    results.vertices.select("id", "pagerank").show()
  }
}
