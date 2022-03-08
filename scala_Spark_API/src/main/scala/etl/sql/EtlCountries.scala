package etl.sql

import engine.sql.{SqlEngine, SqlQuery}
import engine.sql.utils.ReadingQuery
import org.apache.spark.sql.DataFrame

object EtlCountries {
  def main(args: Array[String]): Unit = {
    // Путь к файлу с SQL-запросом
    val pathQuery: String = "/home/hadoop/datasets/Spark_API/EtlQueries/java_etl_countries.sql"
    // Схема таблицы с результатами запроса
    val outputSchemeName = "output_tables"
    // Название таблицы с результатами запроса
    val outputTableName = "scala_etl_countries"

    // Объект ReadingQuery для чтения запроса из файла
    val readingQuery: ReadingQuery = new ReadingQuery(pathQuery)
    // Прочитанный из файла SQL-запрос в виде строки
    val query: String = (readingQuery.readQueryFromFile(readingQuery.path)).toString

    // Объект SqlQuery
    val sqlQuery: SqlQuery = new SqlQuery(query)
    // Движок, выполняющий SQL-запрос
    val sqlEngine: SqlEngine = new SqlEngine
    // Результат SQL-запроса в виде Spark DataFrame
    val resultDataFrame: DataFrame = sqlEngine.runWithReturn(sqlQuery)

    // Записываем DataFrame в результирующую таблицу
    sqlEngine.insert(resultDataFrame, s"$outputSchemeName.$outputTableName", "overwrite")
  }
}
