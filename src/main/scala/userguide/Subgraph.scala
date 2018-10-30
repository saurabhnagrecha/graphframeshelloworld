package userguide

import org.graphframes.GraphFrame

object Subgraph {

  /** given a graph, filters edges and vertices according to user query strings
    *
    * @param g: a GraphFrames graph
    * @param vertexFilter: String filter for vertices. Same syntax as that for SparkSQL
    * @param edgeFilter: String filter for edges. Same syntax as that for SparkSQL
    * @return filtered graph
    */
  def simpleSubgraph(g: GraphFrame,
                     vertexFilter: String,
                     edgeFilter: String): GraphFrame = {
    g.filterVertices(vertexFilter)
      .filterEdges(edgeFilter)
      .dropIsolatedVertices()
  }

  /** demonstrates a triple-centric view of subgraph filtering.
    * Example shown here is hardcoded to the friends graph
    *
    * @param g: GraphFrame graph
    * @return filtered graph
    */
  def complexExampleSubgraph(g: GraphFrame): GraphFrame = {
    // Select subgraph based on edges "e" of type "follow"
    // pointing from a younger user "a" to an older user "b".
    val paths = {g.find("(a)-[e]->(b)")
        .filter("e.relationship = 'follow'")
        .filter("a.age < b.age")}

    // "paths" contains vertex info. Extract the edges.
    val e2 = paths.select("e.src", "e.dst", "e.relationship")
    // In Spark 1.5+, the user may simplify this call:
    //  val e2 = paths.select("e.*")

    GraphFrame(g.vertices, e2)
  }
}
