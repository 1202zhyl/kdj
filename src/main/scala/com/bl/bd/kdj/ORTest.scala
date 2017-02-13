package com.bl.bd.kdj

import java.sql.DriverManager

/**
  * Created by MK33 on 2017/1/17.
  */
object ORTest {

  def main(args: Array[String]) {

    Class.forName("oracle.jdbc.driver.OracleDriver")

    val url = "jdbc:oracle:thin:@10.201.48.3:1521:orcl"
    val username = "rs_user"
    val password = "rs_user"

    val conn = DriverManager.getConnection(url, username, password)
    val stmt = conn.createStatement()
    val result = stmt.executeQuery("show databases")

    while (result.next()) {
      println(result.getString(1))
    }


  }

}
