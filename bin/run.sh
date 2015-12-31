#!/bin/sh

spark-submit \
--jars lib/fastjson-1.2.3.jar,lib/metrics-core-2.2.0.jar,lib/zkclient-0.1.0.jar,lib/kafka_2.10-0.8.2-beta.jar,lib/spark-streaming-kafka_2.10-1.2.0.jar,lib/spark-assembly_2.10-1.1.0-SNAPSHOT.jar,lib/spark-assembly-1.2.0-SNAPSHOT-hadoop2.2.0.jar,lib/kafka-clients-0.8.2-beta.jar,lib/spark-core_2.10-1.2.0.jar,lib/spark-streaming_2.10-1.2.0.jar,lib/mysql-connector-java-5.1.6.jar \
--class com.haodou.pig.test.KafkaStreamingTest \
--master yarn-client \
--queue alpha \
--num-executors 8 \
--driver-memory 1g \
--executor-cores 2 \
--executor-memory 2g \
--conf "spark.streaming.blockInterval=625" \
/home/lifangxing/pig-spark-streaming/target/scala-2.10/pig-spark-streaming_2.10-1.0.jar 10.1.1.10:2181 u3dw m001 4 4
