import org.apache.spark.sql.SparkSession
import ch.epfl.lts2.Utils._
import ch.epfl.lts2.Globals._
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

    /***
      * Parse string stored in the format of Python Dictionary
      *
      * @param stringMap Sting in Python Dictionary format
      * @return Scala Map [Int, Double], where Int is a key of a Python Dictionary and Double is a corresponding value
      */
    def parseDict(stringMap: String): Map[Int, Double] = {
      stringMap.substring(0, stringMap.length)
        .split(", ")
        .map(_.split(": "))
        .map { case Array(k, v) => (k.substring(0, k.length).replace("{", "").toInt, v.substring(0, v.length).replace("}", "").toDouble)}
        .toMap
    }

    val verticesRDD: RDD[(VertexId, Map[Int, Double])] = activationsDF.as[(String, String)].rdd.map(v => (v._1.toLong, parseDict(v._2)))

    log.info(verticesRDD.count() + " vertices in RDD")

    var graph = Graph(verticesRDD, edgesRDD)

    graph = removeSingletons(graph)
//    saveGraph(graph.mapVertices((id, v) => v), weighted = false, fileName = PATH_RESOURCES + "graph_init.gexf")

    log.info(graph.edges.count() + " edges and " + graph.vertices.count() + " vertices in the initial graph after removing singletones")

    val startTime = APR_01_START
    val endTime = APR_01_END

    // STD filtering
    val BURST_RATE = 5
    val BURST_COUNT = 3

    /**
      * Check if a time-series has spikes.
      * Measure of the strength of a spike is a @burstRate * standardDeviation.
      *
      * @param map time-series as a map [TimeStamp->Value].
      * @param list the same time-series unfolded into a list.
      * @param burstRate multiplier for standard deviation. Proportional to the required strength of a spike.
      * @param burstCount required number of spikes in the time-series
      * @return True if the time-series has @burstCount number of spikes of strength @burstRate * standardDeviation
      */
    def hasSpike(map: Map[Int, Double], list: List[Double], burstRate: Int = 5, burstCount: Int = 5): Boolean = {
      val sum: Double = map.values.sum
      val nTimeStamps: Int = list.size
      map.
        filterKeys(hour => hour > startTime & hour < endTime)
        .values.count(l => l > burstRate * stddev(list, sum / nTimeStamps) + sum / nTimeStamps) > burstCount
    }

    val peaksVertices = graph.vertices.map(v => (v._1, (mapToList(v._2, DAYS_TOTAL), v._2)))
      .filter(v => hasSpike(v._2._2, v._2._1, burstRate = BURST_RATE, burstCount = BURST_COUNT))
      .map(v=> (v._1, v._2._2))

    val vIDs = peaksVertices.map(_._1).collect().toSet

    val peaksEgdes = graph.edges.filter(e => vIDs.contains(e.dstId) & vIDs.contains(e.srcId))

    val peaksGraph = Graph(peaksVertices, peaksEgdes)
    //    val peaksGraph = graph

    log.info(peaksGraph.edges.count() + " edges and " + peaksGraph.vertices.count() + " vertices in peaks graph after STD filtering")

    graph = peaksGraph

    // Learn weights
    val trainedGraph = graph.mapTriplets(trplt => compareTimeSeries(trplt.dstAttr, trplt.srcAttr, start = startTime, stop = endTime, isFiltered = true))

    // Remove low weight edges
    val prunedGraph = removeLowWeightEdges(trainedGraph, minWeight = 0.1)

    log.info(prunedGraph.vertices.count() + " vertices and " + prunedGraph.edges.count() + " edges in the trained and pruned graph")

    // Remove singletones
    val cleanGraph = removeSingletons(prunedGraph)

    log.info(cleanGraph.vertices.count() + " vertices and " + cleanGraph.edges.count() + " edges after removing singletones")

    // Keep the largest connected component only
    val LCC = getLargestConnectedComponent(cleanGraph)

    log.info(LCC.vertices.count() + " vertices and " + LCC.edges.count() + " edges in LCC")

    saveGraph(LCC.mapVertices((id, v) => v), weighted = false, fileName = PATH_RESOURCES + "graph.gexf")
  }
}