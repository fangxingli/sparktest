package com.haodou.pig.test

import java.sql.{SQLException, DriverManager}
import java.util.concurrent.{LinkedBlockingQueue}
import javax.naming.InitialContext
import javax.sql.DataSource

object db_daemon{
  val mQueue = new LinkedBlockingQueue[(String, Long, Int)]()

  val bingDBStatement = {
    val url = "jdbc:mysql://10.1.1.16:3306/bing"
    val driver = "com.mysql.jdbc.Driver"
    val username = "bi"
    val passwd = "bi"
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, passwd)
    val statement = connection.createStatement
    statement
  }

  val statement = {
    val url = "jdbc:mysql://10.1.1.70:3306/"
    val driver = "com.mysql.jdbc.Driver"
    val username = "bi"
    val passwd = "bi_haodou"
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, username, passwd)
    connection.createStatement
  }

  def start() :Unit = {        // mysql

    val thread = new Thread(){
      override def run(): Unit ={
        while (true){
          val item = mQueue.take()
          try {
            bingDBStatement.execute("insert into bing. values('%s', FROM_UNIXTIME(%d),%d)".format(item._1, item._2, item._3))
          }catch{
            case e: SQLException => println("SQLException: %s".format(e.getLocalizedMessage))
            case e: Exception => println("All Exception: %s".format(e.getLocalizedMessage))
          }
        }
      }
    }

    thread.start
  }

  def put(item:(String, Long, Int)) :Unit = {
    mQueue.put(item)
  }

  def insert(sql: String) :Unit ={
    try {
      bingDBStatement.execute(sql)
    }catch{
      case e: SQLException => println("SQLExceptin : %s".format(e.getLocalizedMessage))
      case e: Exception => println("All Exception: %s".format(e.getLocalizedMessage))
    }
  }

  def query(sql: String, colnames: Array[String]) :Unit = {
    try {
      val statement = {
        val url = "jdbc:mysql://10.1.1.70:3306/"
        val driver = "com.mysql.jdbc.Driver"
        val username = "bi"
        val passwd = "bi_haodou"
        Class.forName(driver)
        val connection = DriverManager.getConnection(url, username, passwd)
        connection.createStatement
      }

      statement.execute(sql)
      val ret = statement.getResultSet
      while( ret.next ){

      }
    }catch{
      case e: SQLException => println("SQLExceptin : %s".format(e.getLocalizedMessage))
      case e: Exception => println("All Exception: %s".format(e.getLocalizedMessage))
    }
  }
}
