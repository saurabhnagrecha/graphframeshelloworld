import org.apache.spark.sql.SparkSession
import quickstart.QuickStartExamples._
import userguide.GraphCreation._
import userguide.BasicGraphQueries._
import userguide.MotifFinding._
import userguide.Subgraph._
import org.graphframes.GraphFrame

// TODO: import GraphFrames through build.sbt

object GraphFramesOverview {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("graphframes")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF") // let's try and declutter our logger :)

    // Quick Start code
    println("============================================================================")
    runQuickStart(spark)

    println("============================================================================")
    // Examples from the User Guide
    val g: GraphFrame = createExampleGraph(spark)
    runBasicGraphQueries(g)
    val motifs = findExampleMotif(g, "(a)-[e]->(b); (b)-[e2]->(a)")
    val complexMotif = complexMotifExample(g)

    val subgraph = simpleSubgraph(g, "age > 30", "relationship = 'friend'")
    subgraph.edges.show()

    val subgraphFromTriples = complexExampleSubgraph(g)
    subgraphFromTriples.edges.show()
  }
}
