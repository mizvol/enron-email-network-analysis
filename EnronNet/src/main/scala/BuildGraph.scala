import org.apache.spark.sql.SparkSession
import ch.epfl.lts2.Utils._
import org.slf4j.{Logger, LoggerFactory}

object BuildGraph {
  def main(args: Array[String]): Unit = {

    suppressLogs(List("org", "akka"))

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Biuld Enron Graph")
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.executor.memory", "50g")
      .getOrCreate()

    val log: Logger = LoggerFactory.getLogger(this.getClass)
    log.info("Start")

    val PATH_RESOURCES: String = "/mnt/data/git/enron-email-network-analysis/EnronNet/src/main/resources/"

    val edgesDF = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .options(Map("header"-> "true", "inferSchema"-> "true"))
      .load(PATH_RESOURCES + "weighted_links_activated.csv")

    edgesDF.show()

    log.info(edgesDF.count() + " links in the network")

    val activationsDF = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .options(Map("header"-> "false", "inferSchema"-> "true"))
      .load(PATH_RESOURCES + "activations-enron.csv")
      .withColumnRenamed("_c0", "Email")
      .withColumnRenamed("_c1", "Activations")

    activationsDF.show()

    log.info(activationsDF.count() + " emails in the network")


  }

}