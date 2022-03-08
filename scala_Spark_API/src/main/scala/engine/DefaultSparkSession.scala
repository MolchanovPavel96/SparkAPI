package engine

import org.apache.spark.sql.SparkSession

class DefaultSparkSession {
  def getOrCreate: SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("ScalaSparkApplication")
      .config("hive.mapred.supports.subdirectories", "true")
      .config("mapred.input.dir.recursive", "true")
      .enableHiveSupport()
      .getOrCreate()

    sparkSession
  }
}
