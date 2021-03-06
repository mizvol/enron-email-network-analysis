name := "EnronNet"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.0.0",
  "org.apache.spark" % "spark-mllib_2.10" % "2.0.0",
  "org.apache.spark" % "spark-sql_2.10" % "2.0.0",
  "org.apache.spark" %% "spark-graphx" % "2.0.0",
  "com.databricks" % "spark-xml_2.10" % "0.4.0"
)
