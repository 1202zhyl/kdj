package com.bl.bd.util

import java.io._
import java.sql.{Connection, DriverManager}

import org.junit.Test

/**
  * Created by MK33 on 2017/1/5.
  */
class FileUtil {

  @Test
  def readFile = {
    val fileName = "F:\\apache-tomcat-7.0.70\\logs\\localhost_access_log.2017-01-05.txt"
    val file = new FileInputStream(fileName)
    val reader = new InputStreamReader(file)
    val fileRead = new BufferedReader(reader)
    val lineReader = new LineNumberReader(reader)
    val mark = getFileAndLineNumber(fileName)
    val m = Mark(mark)

    val thread = getThread(fileName, m)
    thread.start()
    while (true) {
      val lN = lineReader.getLineNumber
      val line = lineReader.readLine()
      if (lN > m.mark && line != null) {
        m.mark = lN
        println(m.mark + ": " + line)
      }
      Thread.sleep(1)
    }


  }

  def getFileAndLineNumber(fileName: String): Int = {
    require(fileName != null)
    val sql = "select max(line_number) from test.file_read_line_number where file_name = '" + fileName + "'"
    Class.forName("com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://localhost:3306/test"
    val connection = DriverManager.getConnection(url, "root", "123456")
    val stat = connection.createStatement()
    val rs = stat.executeQuery(sql)
    println(sql)
    if (rs.next()) {
      val number = rs.getInt(1)
      number
    } else {
      val updateSql = s"insert into test.file_read_line_number (file_name) values ('${fileName}')"
      println(updateSql)
      stat.executeUpdate(updateSql)
      0
    }

  }

  def getThread(fileName: String, mark: Mark): Thread = {
    var lastUpdate = mark.mark

    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          Thread.sleep(10000)
          if (mark.mark > lastUpdate) {
            val sql = "update test.file_read_line_number set  line_number = " + mark.mark + "  where file_name = '" + fileName + "'"
            val conn = getConnection()
            val stat = conn.createStatement()
            println(sql)
            stat.executeUpdate(sql)
            lastUpdate = mark.mark
          }

        }
      }
    })

  }


  def getConnection(): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://localhost:3306/test"
    if (connection == null) {
      connection = DriverManager.getConnection(url, "root", "123456")
    }
    connection
  }

  var connection: Connection = _
}

case class Mark(var mark: Int)
