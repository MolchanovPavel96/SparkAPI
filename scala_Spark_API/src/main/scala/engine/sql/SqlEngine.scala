package engine.sql

import engine.DefaultSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

class SqlEngine {
  def run(sqlQuery: SqlQuery): DataFrame = {
    val defaultSparkSession = new DefaultSparkSession()
    run(defaultSparkSession.getOrCreate, sqlQuery)
  }

  def run(sparkSession: SparkSession, sqlQuery: SqlQuery): DataFrame = {
    sparkSession.sql(sqlQuery.text)
  }

  /**
   * Метод возвращает результат запроса Spark SQL в виде Spark DataFrame
   */
  def runWithReturn(sqlQuery: String): DataFrame = {
    val defaultSparkSession = new DefaultSparkSession()
    val query: SqlQuery = new SqlQuery(sqlQuery)

    runWithReturn(defaultSparkSession.getOrCreate, query)
  }

  /**
   * Метод возвращает результат запроса Spark SQL в виде Spark DataFrame
   */
  def runWithReturn(sqlQuery: SqlQuery): DataFrame = {
    val defaultSparkSession = new DefaultSparkSession()

    runWithReturn(defaultSparkSession.getOrCreate, sqlQuery)
  }

  /**
   * Метод возвращает результаты запроса Spark SQL в виде DataFrame
   */
  def runWithReturn(sparkSession: SparkSession, sqlQuery: SqlQuery): DataFrame = {
    val dataFrame: DataFrame = sparkSession.sql(sqlQuery.text)

    dataFrame
  }

  /**
   * Метод производит вставку данных из полученного DataFrame
   * в указанную таблицу с возможностью выбора режима вставки
   * Варианты modeType: append, overwrite
   */
  def insert(dataFrame: DataFrame, tableName: String, modeType: String): Unit = {
    dataFrame.write.mode(modeType).insertInto(tableName)
  }
}
