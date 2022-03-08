from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


class DefaultSparkSession:
    """
    Spark Session по умолчанию
    """
    def __init__(self):
        # Создаем конфигурацию Spark по умолчанию
        self.conf = SparkConf()\
            .setAppName("PythonSparkApplication")\
            .set("hive.mapred.supports.subdirectories", "true")\
            .set("mapred.input.dir.recursive", "true")

    def get_or_create(self, spark_conf=None):
        """
        Функция получает существующую или создает новую Spark Session
        :param spark_conf: конфигурация Spark
        :return: spark_session
        """

        # Передаем в конфигурацию Spark пришедший аргумент, если он не пустой
        if ((not spark_conf is None) and (len(spark_conf) > 0)):
            self.conf.setAll(spark_conf)

        # Создаем Spark Session
        spark_session = SparkSession.builder\
            .config(conf=self.conf)\
            .enableHiveSupport()\
            .getOrCreate()

        return spark_session
