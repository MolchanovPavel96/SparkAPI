/home/hadoop/spark/bin/spark-submit \
--master yarn \
--py-files /home/hadoop/datasets/Spark_API/Spark_Submit_Jars/PySpark/engine/MySparkSession.py,\
/home/hadoop/datasets/Spark_API/Spark_Submit_Jars/PySpark/engine/sql/MySqlQuery.py,\
/home/hadoop/datasets/Spark_API/Spark_Submit_Jars/PySpark/engine/sql/MySqlEngine.py,\
/home/hadoop/datasets/Spark_API/Spark_Submit_Jars/PySpark/engine/sql/utils/MyReadingQuery.py \
/home/hadoop/datasets/Spark_API/Spark_Submit_Jars/PySpark/EtlCountries.py