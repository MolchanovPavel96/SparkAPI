# SparkAPI
This repository consider simple ETL programs using Scala and Python Spark API running by spark-submit. 
I tested these projects by running on my Hadoop Pseudo cluster deployed on Ubuntu Virtual Machine (Hadoop-3.3.1, Spark-3.1.2, Hive-3.1.2)

1) scala_Spark_API.zip contains Scala Spark API simple ETL engine. 

2) python_Spark_API.zip contains the same simple ETL engine written in Python.

3) files.zip contains folders "Datasets" and "Spark_API".

In files/Datasets there are 3 datasets from Kaggle and 1 from Central Bank of Russian Federation:
https://www.kaggle.com/fernandol/countries-of-the-world

https://www.kaggle.com/brendan45774/countries-life-expectancy

https://www.kaggle.com/nitinsss/military-expenditure-of-countries-19602019

https://cbr.ru/currency_base/dynamics/

files/Spark_API/EtlQueries/java_etl_countries.sql is a ETL-script based on source tables made from Datasets.

files/Spark_API/Runners contains Shell-scripts for running Scala and Python applications by spark-submit on Pseudo cluster.

files/Spark_API/Spark_Submit_Jars has to contain JARs of Scala Spark project and ZIPs of Python Spark project to be runned on Pseudo Cluster.
