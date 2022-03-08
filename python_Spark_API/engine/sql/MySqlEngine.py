from pyspark.sql import DataFrame

from engine.MySparkSession import DefaultSparkSession
from engine.sql.MySqlQuery import SqlQuery


class SqlEngine:
    """
    Класс SqlEngine описывает движок для выполнения Spark SQL-запросов
    """
    def __init__(self):
        pass

    def run(self, sql_query, spark_conf=None):
        """
        Функция выполняет переданный SQL-скрипт в Spark SQL
        :param sql_query:
        :param spark_conf:
        :return:
        """
        default_spark_session = DefaultSparkSession()

        if isinstance(sql_query, SqlQuery):
            # Получаем существующую или создаем новую Spark Session
            spark_session = default_spark_session.get_or_create(spark_conf)
            # Запускаем выполнение Spark SQL
            spark_session.sql(sql_query.text)
        else:
            print(f"Переданный запрос {sql_query} не является экземпляром класса SqlQuery")

    def run_with_return(self, sql_query, spark_conf=None):
        """
        Функция выполняет переданный SQL-скрипт и возвращает результата в виде Spark DataFrame
        :param sql_query:
        :param spark_conf:
        :return:
        """
        default_spark_session = DefaultSparkSession()

        if isinstance(sql_query, SqlQuery):
            # Получаем существующую или создаем новую Spark Session
            spark_session = default_spark_session.get_or_create(spark_conf)
            # Запускаем выполнение Spark SQL
            dataframe = spark_session.sql(sql_query.text)
            return dataframe
        else:
            print(f"Переданный запрос {sql_query} не является экземпляром класса SqlQuery")

    def insert(self, dataframe, table_name, mode_type):
        """
        Функция производит запись переданного Spark DataFrame в указанную таблицу с указанным режимом записи
        :param dataframe:
        :param table_name:
        :param mode_type:
        :return:
        """
        if isinstance(dataframe, DataFrame):
            dataframe.write.mode(mode_type).insertInto(table_name)
        else:
            print(f"Переданный dataframe {dataframe} не является экземпляром класса DataFrame")