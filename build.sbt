name := "kafkar"

version := "0.1"

scalaVersion := "2.11.12"

retrieveManaged := true

fork := true

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.1.0"