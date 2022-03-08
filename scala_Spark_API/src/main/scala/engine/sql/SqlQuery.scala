package engine.sql

class SqlQuery(val text: String) {
  //
  def replaceText(oldText: String, newText: String): SqlQuery = {
    val resultText: String = text.replaceAllLiterally(oldText, newText)
    new SqlQuery(resultText)
  }
}
