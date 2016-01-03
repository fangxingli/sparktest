package com.haodou.pig.test

import java.sql.{Statement, Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date

import _root_.kafka.serializer.{StringDecoder, DefaultDecoder}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.streaming.dstream.ForEachDStream
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import scala.xml._

//case class PHP_APP_Log(var request_time :String, var device_id :String, val channel_id :String, val phone_type :String, val userip :String, val appid :String, val version_id :String, val userid :String, val function_id :String, val parameter_desc :String, val uuid :String)

object KafkaStreamingTest {
  def main(args: Array[String]): Unit = {
    val Array(zkQuorum, group, topics, numThreads, partition) = args

    //println("Master :" + master)
    println("zkQuorum: " + zkQuorum)
    println("group: " + group)
    println("topics:" + topics)
    println("numThreads :" + numThreads)
    println("partition: " + partition)

    val conf = new SparkConf().setAppName("pig-spart-streaming")

    val ssc = new StreamingContext(conf, Seconds(5))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    // 启动配置文件监控
    //        directoryWatcher.start("/home/lifangxing/pig-spark-streaming/config")
    // 启动数据库后台进程
    //        db_daemon.start()

    println("topics:" + topicMap)
    // 3 kafka partition at least, every DStream map to one Kafka partition
    val numPartitionsOfInputTopic = 1
    val sparkProcessingParallelism = 3

    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest",
      "auto.commit.enable" -> "true",
      "zookeeper.connection.timeout.ms" -> "1000"
    )

    val streams = (1 to numPartitionsOfInputTopic) map { _ =>
      KafkaUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](ssc, kafkaParams, Map(topics -> 1), StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)
    }
    val unifiedStream = ssc.union(streams)
    val unifiedStreamStr = unifiedStream.map { case bytes =>
      new String(bytes, "UTF-8")
    }

    unifiedStreamStr.foreachRDD(rdd => {
      val ret = rdd.map { x =>
        x.split("\t").length
      }.collect
      println(ret)
    })

    /*
    unifiedStreamStr.foreachRDD(rdd => {
      val ret = rdd.map { x =>
        try {
          val pkg = JSON.parseObject(x)
          val appid = pkg.getString("source") match {
            case "s102" => 2
            case "s101" => 4
            case _ => 0
          }
          val timestamp = pkg.getString("time")
          val ip = pkg.getString("ip")
          val msg = pkg.getJSONObject("msg")
          Option((ip, appid, timestamp, msg))
          /*
          val ret = List(("order_id", appid, msg.getString("order_id")),
                      ("lottery_id", appid, msg.getString("lottery_id")),
                      ("moblie", appid, msg.getString("mobile")))
          ret.filter(_._3 != null)
          */
        } catch {
          case e: Exception => Option(null)
        }
      }.filter(_ match {
        case Some((ip, _, _, msg)) => msg.getString("order_id") != null && ip != "0.0.0.0" && ip != "127.0.0.1"
        case _ => false
      }
      ).collect
      //db_daemon.execute("select shoporderid, userid from haodou_shop.ShopOrder where ShopOrderId in (%s);".format(ret.map(_.get._4.getString("order_id")).mkString(",")))
    })

    //                db_daemon.put((config._1, x._1, x._2))
    //                println("%s\t%s".format(config._2.get("value").get, x))
    */

    ssc.start()
    ssc.awaitTermination()
  }
}

        

