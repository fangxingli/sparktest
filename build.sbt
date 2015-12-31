name := "pig spark streaming"

version := "1.0"

scalaVersion := "2.10.2"

libraryDependencies += "org.apache.poi" % "poi" % "3.10.1"

libraryDependencies += "org.apache.poi" % "poi-ooxml" % "3.10.1"

libraryDependencies += "org.apache.spark" % "spark-assembly_2.10" % "1.0.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.0.2"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.12"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.2.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2-beta"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2-beta"

libraryDependencies += "com.github.sgroschupf" % "zkclient" % "0.1"

libraryDependencies += "com.alibaba" % "fastjson" % "1.2.7"

libraryDependencies += "com.yammer.metrics" % "metrics-core" % "2.2.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.12"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.2.0"





resolvers += "oschina Repository" at "http://maven.oschina.net/content/groups/public/"
