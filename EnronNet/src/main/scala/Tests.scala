import ch.epfl.lts2.Utils.suppressLogs
import org.apache.spark.sql.SparkSession

/**
  * Created by volodymyrmiz on 27/07/18.
  */
object Tests {
  def main(args: Array[String]): Unit = {
    suppressLogs(List("org", "akka"))

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Biuld Enron Graph")
      .config("spark.driver.maxResultSize", "10g")
      .config("spark.executor.memory", "50g")
      .getOrCreate()

    import spark.implicits._

    val mapString = "{10: 1023, 20: 1024}"

    def parseDict(stringMap: String): Map[Long, Long] = {
      stringMap.substring(0, stringMap.length)
        .split(", ")
        .map(_.split(": "))
        .map { case Array(k, v) => (k.substring(0, k.length).replace("{", "").toLong, v.substring(0, v.length).replace("}", "").toLong)}
        .toMap
    }

    val map = parseDict(mapString)

    println(map)
    map.foreach(println)
  }
}
