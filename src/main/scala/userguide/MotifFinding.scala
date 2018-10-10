package userguide

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when, lit}
import org.graphframes.GraphFrame

object MotifFinding {

  /** given a graph and a motif query...
    * returns an induced subgraph of motifs which conform to the query
    *
    * For more details on how the motif query is structured in terms of
    * the Domain-Specific Language, please refer to
    * https://graphframes.github.io/user-guide.html#motif-finding
    *
    * @param g: GraphFrame graph
    * @param query: A string-type representation of the motif
    * @return a dataframe of edges which conform to the motif
    */
  def findExampleMotif(g: GraphFrame, query: String): DataFrame = {
    val motifs = g.find(query)
    motifs.show() // optional: shows the motifs
    motifs
  }

  /** more complex motif finding example.
    * The query is hardcoded to keep the example tightly coupled with the example graph.
    *
    * This is an example which shows the flexibility of finding arbitrarily complex
    * motifs in GraphFrames.
    * The reader is meant to mentally compare this simplicity to the level of munging
    * required in GraphX using RDDs.
    *
    * @param g: GraphFrame graph
    * @return DataFrame which satisfies some arbitrary complex motif criteria
    */
  def complexMotifExample(g: GraphFrame): DataFrame = {
    val chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

    // Query on sequence, with state (cnt)
    //  (a) Define method for updating state given the next element of the motif.
    def sumFriends(cnt: Column, relationship: Column): Column = {
      when(relationship === "friend", cnt + 1).otherwise(cnt)
    }
    //  (b) Use sequence operation to apply method to sequence of elements in motif.
    //      In this case, the elements are the 3 edges.
    val condition = { Seq("ab", "bc", "cd")
      .foldLeft(lit(0))((cnt, e) => sumFriends(cnt, col(e)("relationship"))) }
    //  (c) Apply filter to DataFrame.
    val chainWith2Friends2 = chain4.where(condition >= 2)
    chainWith2Friends2.show()
    chainWith2Friends2
  }
}
