import org.apache.spark.sql.SparkSession
import ch.epfl.lts2.Utils._
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

object BuildGraph {
  def main(args: Array[String]): Unit = {

    suppressLogs(List("org", "akka"))

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Biuld Enron Graph")
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.executor.memory", "50g")
      .getOrCreate()

    import spark.implicits._

    val log: Logger = LoggerFactory.getLogger(this.getClass)
    log.info("Start")

    val PATH_RESOURCES: String = "/mnt/data/git/enron-email-network-analysis/EnronNet/src/main/resources/"
    val DAYS_TOTAL: Int = 2221

    val edgesDF = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .options(Map("header"-> "true", "inferSchema"-> "true"))
      .load(PATH_RESOURCES + "weighted_links_activated_id.csv")

    log.info(edgesDF.count() + " links in the network")

    val activationsDF = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .options(Map("header"-> "false", "inferSchema"-> "true"))
      .load(PATH_RESOURCES + "activations-enron_id.csv")
      .withColumnRenamed("_c0", "Email")
      .withColumnRenamed("_c1", "Activations")

    log.info(activationsDF.count() + " emails in the network")

    val edgesRDD: RDD[Edge[Double]] = edgesDF.as[(String, String, String)].rdd.coalesce(12).map(e => Edge(e._1.toLong, e._2.toLong, 0.0))

    log.info(edgesRDD.count() + " edges in RDD")

    def parseDict(stringMap: String): Map[Int, Double] = {
      stringMap.substring(0, stringMap.length)
        .split(", ")
        .map(_.split(": "))
        .map { case Array(k, v) => (k.substring(0, k.length).replace("{", "").toInt, v.substring(0, v.length).replace("}", "").toDouble)}
        .toMap
    }

    val verticesRDD: RDD[(VertexId, (Map[Int, Double]))] = activationsDF.as[(String, String)].rdd.map(v => (v._1.toLong, parseDict(v._2)))

    log.info(verticesRDD.count() + " vertices in RDD")

    var graph = Graph(verticesRDD, edgesRDD)

    graph = removeSingletons(graph)
    saveGraph(graph.mapVertices((id, v) => v), weighted = false, fileName = PATH_RESOURCES + "graph_init.gexf")

    log.info(graph.edges.count() + " edges and " + graph.vertices.count() + " vertices in the initial graph after removing singletones")

    val startTime = 0
    val endTime = DAYS_TOTAL

    val trainedGraph = graph.mapTriplets(trplt => compareTimeSeries(trplt.dstAttr, trplt.srcAttr, start = startTime, stop = endTime, isFiltered = true))

    val prunedGraph = removeLowWeightEdges(trainedGraph, minWeight = 1.0)

    log.info(prunedGraph.vertices.count() + " vertices and " + prunedGraph.edges.count() + " edges in the trained and pruned graph")

    val cleanGraph = removeSingletons(prunedGraph)

    log.info(cleanGraph.vertices.count() + " vertices and " + cleanGraph.edges.count() + " edges after removing singletones")

    val LCC = getLargestConnectedComponent(cleanGraph)

    log.info(LCC.vertices.count() + " vertices and " + LCC.edges.count() + " edges in LCC")

    saveGraph(LCC.mapVertices((id, v) => v), weighted = false, fileName = PATH_RESOURCES + "graph.gexf")
  }
}