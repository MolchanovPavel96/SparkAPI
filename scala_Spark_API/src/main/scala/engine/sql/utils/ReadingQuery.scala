package engine.sql.utils

import scala.io.Source._

class ReadingQuery(val path: String) {
  // Метод читает по заданному пути SQL-запрос из файла и возвращает в виде строки
  def readQueryFromFile(path: String): Any = {
    // Строка-накопитель
    try {
      val result: String = fromFile(path).getLines.mkString(" ")
      result
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
