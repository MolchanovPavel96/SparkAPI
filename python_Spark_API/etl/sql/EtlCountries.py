# Импорт библиотек Python
import os
import sys
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

# Добавляем в path zip-архив с файлами .py , необходимыми для работы приложения
sys.path.insert(0, os.path.join(os.getcwd(), "artifacts.zip"))

# Импорт классов, необходимых для работы приложения
from engine.MySparkSession import DefaultSparkSession
from engine.sql.MySqlQuery import SqlQuery
from engine.sql.MySqlEngine import SqlEngine
from engine.sql.utils.MyReadingQuery import ReadingQuery

def main():
    # Путь к файлу с SQL-запросом:
    path_query = "/home/hadoop/datasets/Spark_API/EtlQueries/java_etl_countries.sql"

    # Схема таблицы с результатами запроса
    output_scheme_name = "output_tables"

    # Название таблицы с результатами запроса
    output_table = "python_etl_countries"

    # Объект ReadingQuery для чтения запроса из файла
    reading_query = ReadingQuery(path_query)

    # Прочитанный из файла SQL-запрос в виде строки
    query = reading_query.read_query_from_file()

    # Объект SqlQuery
    sql_query = SqlQuery(query)

    # Объект SqlEngine
    sql_engine = SqlEngine()

    # DataFrame с результатами расчета
    result_dataframe = sql_engine.run_with_return(sql_query)

    # Записываем DataFrame в результирующую таблицу
    sql_engine.insert(result_dataframe, f"{output_scheme_name}.{output_table}", "overwrite")


if __name__ == "__main__":
    main()