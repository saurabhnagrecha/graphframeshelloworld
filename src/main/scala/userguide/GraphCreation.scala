package userguide

import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object GraphCreation {

  /** create a simple friend graph
    * https://graphframes.github.io/user-guide.html#creating-graphframes
    *
    * @param spark: active spark session
    * @return an example friend graphframe
    */
  def createExampleGraph(spark: SparkSession): GraphFrame = {
    import spark.implicits._

    // Vertex DataFrame
    val v = Seq(
      ("a", "Alice", 34),
      ("b", "Bob", 36),
      ("c", "Charlie", 30),
      ("d", "David", 29),
      ("e", "Esther", 32),
      ("f", "Fanny", 36),
      ("g", "Gabby", 60)
    ).toDF("id", "name", "age")
    // Edge DataFrame
    val e = Seq(
      ("a", "b", "friend"),
      ("b", "c", "follow"),
      ("c", "b", "follow"),
      ("f", "c", "follow"),
      ("e", "f", "follow"),
      ("e", "d", "friend"),
      ("d", "a", "friend"),
      ("a", "e", "friend")
    ).toDF("src", "dst", "relationship")
    // Create a GraphFrame
    val g = GraphFrame(v, e)

    println("The underlying data model is pretty much the same as GraphX")
    println("... except now with (fun) DataFrames instead of (cumbersome) RDDs!")
    g.vertices.show()
    g.edges.show()

    g
  }
}
