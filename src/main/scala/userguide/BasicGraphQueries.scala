package userguide

import org.apache.spark.sql.DataFrame
import org.graphframes.GraphFrame

object BasicGraphQueries {
  /** run the basic queries on the example friend graph
    * https://graphframes.github.io/user-guide.html#basic-graph-and-dataframe-queries
    *
    * @param g: basic friend graph
    */
  def runBasicGraphQueries(g: GraphFrame): Unit = {
    // Get a DataFrame with columns "id" and "inDeg" (in-degree)
    val vertexInDegrees: DataFrame = g.inDegrees
    println("In-degrees:")
    vertexInDegrees.show()

    // Find the youngest user's age in the graph.
    // This queries the vertex DataFrame.
    println("Youngest user's age in the graph (shown as a DataFrame)")
    g.vertices.groupBy().min("age").show()

    // Count the number of "follows" in the graph.
    // This queries the edge DataFrame.
    val numFollows = g.edges.filter("relationship = 'follow'").count()
    println("# of follows in the graph:" + numFollows.toString)
  }
}
