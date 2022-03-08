/home/hadoop/spark/bin/spark-submit \
--class etl.sql.EtlCountries \
--deploy-mode cluster \
--master yarn \
/home/hadoop/datasets/Spark_API/Spark_Submit_Jars/scala_spark_api_2.12-0.1.jar