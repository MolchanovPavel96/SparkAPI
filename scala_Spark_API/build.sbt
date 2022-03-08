name := "scala_Spark_API"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-develop.streaming" % sparkVersion % "provided"
