package com.haodou.pig.test

import com.alibaba.fastjson.{JSONObject, JSON}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object pigDailyComsumer{
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("pig-daily-comsumer")
        val sc = new SparkContext(conf)


        val piglog = sc.textFile("hdfs://hdcluster/bing/data/ods_app_actionlog_raw_dm/statis_date=2015-01-21/app_action.log.2015-01-21.lzo")

        piglog.map{ x =>
            val raw  = x.replace("\\", "")
            try {
                val pkg = JSON.parseObject(raw)

                pkg.getJSONObject("ext") match {
                    case JSONObject(x) =>
                }
                Option(pkg)
            }catch{
                case e: Exception => Option(null)
            }
        }.filter( _ match {
            case Some(x) => true
            case _ => false
        }).collect.foreach(println)
    }
}
